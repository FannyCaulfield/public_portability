import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { Client } from 'pg';
import Redis from 'ioredis';
import { once } from 'events';
import {
  startPgNotifyListener,
  stopPgNotifyListener,
  isPgNotifyListenerRunning,
} from '@/lib/pg-notify-listener';

const pgDirectConfig = {
  host: process.env.POSTGRES_HOST || 'localhost',
  port: parseInt(process.env.POSTGRES_DIRECT_PORT || '5432'),
  database: process.env.POSTGRES_DB || 'nexus',
  user: process.env.POSTGRES_USER || 'postgres',
  password: process.env.POSTGRES_PASSWORD || 'mysecretpassword',
};

const redisConfig = {
  host: process.env.REDIS_HOST || 'localhost',
  port: parseInt(process.env.REDIS_PORT || '6379'),
  password: process.env.REDIS_PASSWORD,
  db: parseInt(process.env.REDIS_DB || '0'),
};

async function waitForRedisReady(client: Redis) {
  if ((client as any).status === 'ready') return;
  await once(client, 'ready');
}

async function sleep(ms: number) {
  await new Promise((r) => setTimeout(r, ms));
}

async function waitForCondition(fn: () => Promise<boolean>, timeoutMs = 5000, intervalMs = 100) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (await fn()) return;
    await sleep(intervalMs);
  }
  throw new Error(`Condition not met within ${timeoutMs}ms`);
}

async function getIdentityState(pg: Client, testUserId: string, twitterId: string) {
  const result = await pg.query(
    `SELECT
       i.id AS identity_id,
       i.app_user_id,
       pa.platform,
       pa.platform_account_id,
       pa.platform_username,
       pa.platform_instance,
       pa.auth_social_account_id
     FROM identity.identities i
     LEFT JOIN identity.platform_accounts pa
       ON pa.identity_id = i.id
     WHERE i.app_user_id = $1
     ORDER BY pa.platform, pa.platform_account_id`,
    [testUserId]
  );

  const nodeResult = await pg.query(
    `SELECT
       twitter_id::text AS twitter_id
     FROM network.nodes
     WHERE twitter_id = $1`,
    [twitterId]
  );

  return {
    identityRows: result.rows,
    nodeRow: nodeResult.rows[0] ?? null,
  };
}

describe('Sync users → Redis mappings (pg_notify → Node listener → Redis)', () => {
  let pg: Client;
  let redis: Redis;

  const testUserId = `00000000-0000-0000-0000-${String(Date.now()).slice(-12).padStart(12, '0')}`;
  const twitterId = String(900000000000 + (Date.now() % 100000000000));
  let blueskySocialAccountId: string | null = null;
  let mastodonSocialAccountId: string | null = null;

  beforeAll(async () => {
    if (!process.env.REDIS_PASSWORD) {
      throw new Error('REDIS_PASSWORD must be set to run this integration test');
    }

    pg = new Client(pgDirectConfig);
    await pg.connect();

    redis = new Redis(redisConfig);
    redis.on('error', (err: Error) => {
      console.error('[Redis error]', err.message);
    });
    await waitForRedisReady(redis);

    await startPgNotifyListener();

    if (!isPgNotifyListenerRunning()) {
      throw new Error('PgNotify listener did not start');
    }

    // Ensure clean keys
    await redis.del(`twitter_to_bluesky:${twitterId}`);
    await redis.del(`twitter_to_mastodon:${twitterId}`);

    // Create user with required fields
    await pg.query(
      `INSERT INTO "next-auth".users (id, name, email, created_at, updated_at, has_onboarded, hqx_newsletter, oep_accepted, research_accepted, have_seen_newsletter, automatic_reconnect)
       VALUES ($1, $2, $3, now(), now(), false, false, false, false, false, false)
       ON CONFLICT (id) DO NOTHING`,
      [testUserId, 'test-sync-redis-mapping', `test-${twitterId}@example.com`]
    );

    await pg.query(
      `INSERT INTO "next-auth".social_accounts (
         user_id,
         provider,
         provider_account_id,
         username,
         instance,
         email,
         is_primary,
         last_seen_at
       )
       VALUES ($1, 'twitter', $2, $3, '', $4, true, now())`,
      [testUserId, twitterId, `test_${twitterId}`, `test-${twitterId}@example.com`]
    );
  });

  afterAll(async () => {
    await stopPgNotifyListener();

    try {
      await pg.query('DELETE FROM "next-auth".users WHERE id = $1', [testUserId]);
    } catch {
      // ignore
    }

    await pg.end();
    await redis.quit();
  });

  it('should upsert/delete bluesky mapping in Redis when next-auth.users changes', async () => {
    const key = `twitter_to_bluesky:${twitterId}`;

    // Insert bluesky social account (should upsert)
    const blueskyInsert = await pg.query(
      `INSERT INTO "next-auth".social_accounts (
         user_id,
         provider,
         provider_account_id,
         username,
         instance,
         email,
         is_primary,
         last_seen_at
       )
       VALUES ($1, 'bluesky', $2, $3, '', $4, true, now())
       RETURNING id`,
      [testUserId, `did:plc:${twitterId}`, `bsky_${twitterId}`, `test-${twitterId}@example.com`]
    );
    blueskySocialAccountId = blueskyInsert.rows[0]?.id ?? null;

    await waitForCondition(async () => {
      const val = await redis.get(key);
      return val === `bsky_${twitterId}`;
    }, 8000, 200);

    await waitForCondition(async () => {
      const { identityRows, nodeRow } = await getIdentityState(pg, testUserId, twitterId);
      const twitterAccount = identityRows.find((row) => row.platform === 'twitter');
      const blueskyAccount = identityRows.find((row) => row.platform === 'bluesky');

      return Boolean(
        identityRows.length >= 2 &&
        twitterAccount?.platform_account_id === twitterId &&
        twitterAccount?.platform_instance === '' &&
        blueskyAccount?.platform_account_id === `did:plc:${twitterId}` &&
        blueskyAccount?.platform_username === `bsky_${twitterId}` &&
        blueskyAccount?.platform_instance === '' &&
        blueskyAccount?.auth_social_account_id === blueskySocialAccountId &&
        nodeRow?.twitter_id === twitterId
      );
    }, 8000, 200);

    // Delete bluesky social account (should delete)
    await pg.query(
      `DELETE FROM "next-auth".social_accounts
       WHERE id = $1`,
      [blueskySocialAccountId]
    );
    blueskySocialAccountId = null;

    await waitForCondition(async () => {
      const val = await redis.get(key);
      return val === null;
    }, 8000, 200);

    await waitForCondition(async () => {
      const { identityRows, nodeRow } = await getIdentityState(pg, testUserId, twitterId);
      const twitterAccounts = identityRows.filter((row) => row.platform === 'twitter');
      const blueskyAccounts = identityRows.filter((row) => row.platform === 'bluesky');

      return Boolean(
        twitterAccounts.length === 1 &&
        blueskyAccounts.length === 0 &&
        nodeRow?.twitter_id === twitterId
      );
    }, 8000, 200);
  }, 20000);

  it('should upsert/delete mastodon mapping in Redis when next-auth.users changes', async () => {
    const key = `twitter_to_mastodon:${twitterId}`;

    // Insert mastodon social account (should upsert)
    const mastodonInsert = await pg.query(
      `INSERT INTO "next-auth".social_accounts (
         user_id,
         provider,
         provider_account_id,
         username,
         instance,
         email,
         is_primary,
         last_seen_at
       )
       VALUES ($1, 'mastodon', $2, $3, $4, $5, true, now())
       RETURNING id`,
      [testUserId, `mastodon_${twitterId}`, `m_${twitterId}`, 'example.social', `test-${twitterId}@example.com`]
    );
    mastodonSocialAccountId = mastodonInsert.rows[0]?.id ?? null;

    await waitForCondition(async () => {
      const val = await redis.get(key);
      if (!val) return false;
      try {
        const parsed = JSON.parse(val);
        return parsed?.id === `mastodon_${twitterId}` && parsed?.username === `m_${twitterId}` && parsed?.instance === 'example.social';
      } catch {
        return false;
      }
    }, 8000, 200);

    await waitForCondition(async () => {
      const { identityRows, nodeRow } = await getIdentityState(pg, testUserId, twitterId);
      const twitterAccount = identityRows.find((row) => row.platform === 'twitter');
      const mastodonAccount = identityRows.find((row) => row.platform === 'mastodon');

      return Boolean(
        identityRows.length >= 2 &&
        twitterAccount?.platform_account_id === twitterId &&
        twitterAccount?.platform_instance === '' &&
        mastodonAccount?.platform_account_id === `mastodon_${twitterId}` &&
        mastodonAccount?.platform_username === `m_${twitterId}` &&
        mastodonAccount?.platform_instance === 'example.social' &&
        mastodonAccount?.auth_social_account_id === mastodonSocialAccountId &&
        nodeRow?.twitter_id === twitterId
      );
    }, 8000, 200);

    // Delete mastodon social account (should delete)
    await pg.query(
      `DELETE FROM "next-auth".social_accounts
       WHERE id = $1`,
      [mastodonSocialAccountId]
    );
    mastodonSocialAccountId = null;

    await waitForCondition(async () => {
      const val = await redis.get(key);
      return val === null;
    }, 8000, 200);

    await waitForCondition(async () => {
      const { identityRows, nodeRow } = await getIdentityState(pg, testUserId, twitterId);
      const twitterAccounts = identityRows.filter((row) => row.platform === 'twitter');
      const mastodonAccounts = identityRows.filter((row) => row.platform === 'mastodon');

      return Boolean(
        twitterAccounts.length === 1 &&
        mastodonAccounts.length === 0 &&
        nodeRow?.twitter_id === twitterId
      );
    }, 8000, 200);
  }, 20000);

  it('should verify triggers exist on next-auth.users', async () => {
    const result = await pg.query(`
      SELECT tgname
      FROM pg_trigger
      WHERE tgrelid = '"next-auth".social_accounts'::regclass
        AND tgname IN ('sync_twitter_bluesky_users_trigger', 'sync_twitter_mastodon_users_trigger')
    `);

    const names = result.rows.map((r) => r.tgname);
    expect(names).toContain('sync_twitter_bluesky_users_trigger');
    expect(names).toContain('sync_twitter_mastodon_users_trigger');
  });
});
