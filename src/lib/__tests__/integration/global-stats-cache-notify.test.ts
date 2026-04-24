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
  host: process.env.POSTGRES_DIRECT_HOST || process.env.POSTGRES_HOST || 'localhost',
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

describe('Global stats cache invalidation (pg_notify → Node listener → Redis)', () => {
  let pg: Client;
  let redis: Redis;

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
  });

  afterAll(async () => {
    await stopPgNotifyListener();
    await pg.end();
    await redis.quit();
  });

  it('should write stats:global in Redis after UPDATE on global_stats_cache (migration applied)', async () => {
    const nowIso = new Date().toISOString();

    await pg.query(
      `INSERT INTO cache.global_stats_cache (
         id,
         users_total,
         users_onboarded,
         users_updated_at,
         with_handle,
         with_handle_bluesky,
         with_handle_mastodon,
         followed_on_bluesky,
         followed_on_mastodon,
         connections_updated_at,
         followers,
         following,
         heavy_updated_at,
         updated_at
       )
       VALUES (
         true,
         $1,
         $2,
         $3::timestamptz,
         $4,
         $5,
         $6,
         $7,
         $8,
         $9::timestamptz,
         $10,
         $11,
         $12::timestamptz,
         $13::timestamptz
       )
       ON CONFLICT (id) DO UPDATE SET
         users_total = EXCLUDED.users_total,
         users_onboarded = EXCLUDED.users_onboarded,
         users_updated_at = EXCLUDED.users_updated_at,
         with_handle = EXCLUDED.with_handle,
         with_handle_bluesky = EXCLUDED.with_handle_bluesky,
         with_handle_mastodon = EXCLUDED.with_handle_mastodon,
         followed_on_bluesky = EXCLUDED.followed_on_bluesky,
         followed_on_mastodon = EXCLUDED.followed_on_mastodon,
         connections_updated_at = EXCLUDED.connections_updated_at,
         followers = EXCLUDED.followers,
         following = EXCLUDED.following,
         heavy_updated_at = EXCLUDED.heavy_updated_at,
         updated_at = EXCLUDED.updated_at`,
      [123, 45, nowIso, 456, 78, 90, 12, 34, nowIso, 567, 678, nowIso, nowIso]
    );

    await waitForCondition(async () => {
      const raw = await redis.get('stats:global');
      if (!raw) return false;
      try {
        const parsed = JSON.parse(raw);
        return parsed?.users?.total === 123
          && parsed?.users?.onboarded === 45
          && parsed?.connections?.followers === 567
          && parsed?.connections?.following === 678
          && parsed?.connections?.withHandle === 456
          && parsed?.connections?.withHandleBluesky === 78
          && parsed?.connections?.withHandleMastodon === 90
          && parsed?.connections?.followedOnBluesky === 12
          && parsed?.connections?.followedOnMastodon === 34;
      } catch {
        return false;
      }
    }, 8000, 200);
  }, 20000);

  it('should verify trigger function uses pg_notify (migration applied)', async () => {
    const result = await pg.query(`
      SELECT proname, prosrc
      FROM pg_proc
      WHERE proname = 'notify_global_stats_cache_change_v2'
    `);

    expect(result.rows.length).toBe(1);
    expect(result.rows[0].prosrc).toContain('pg_notify');
    expect(result.rows[0].prosrc).toContain('global_stats_cache_invalidation');
  });

  it('should verify triggers exist on cache.global_stats_cache', async () => {
    const result = await pg.query(`
      SELECT tgname
      FROM pg_trigger
      WHERE tgrelid = 'cache.global_stats_cache'::regclass
        AND tgname IN ('trg_notify_global_stats_cache_v2_ins', 'trg_notify_global_stats_cache_v2_upd')
    `);

    const names = result.rows.map((r) => r.tgname);
    expect(names).toContain('trg_notify_global_stats_cache_v2_ins');
    expect(names).toContain('trg_notify_global_stats_cache_v2_upd');
  });
});
