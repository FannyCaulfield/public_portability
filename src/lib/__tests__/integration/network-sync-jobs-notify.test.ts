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

const NETWORK_WORKER_PENDING_KEY = 'network_worker:jobs:pending';

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

describe('Network sync jobs notify (pg_notify -> Node listener -> Redis)', () => {
  let pg: Client;
  let redis: Redis;

  const testUserId = `00000000-0000-0000-0000-${String(Date.now()).slice(-12).padStart(12, '0')}`;
  let createdJobId: number | null = null;

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

    // Clean queue key used by this integration path.
    await redis.del(NETWORK_WORKER_PENDING_KEY);
  });

  afterAll(async () => {
    await stopPgNotifyListener();

    if (createdJobId !== null) {
      try {
        await pg.query('DELETE FROM jobs.network_sync_jobs WHERE id = $1', [createdJobId]);
      } catch {
        // ignore cleanup error
      }
    }

    await redis.del(NETWORK_WORKER_PENDING_KEY);
    await pg.end();
    await redis.quit();
  });

  it('should enqueue payload in Redis when network_sync_jobs emits NOTIFY', async () => {
    const result = await pg.query(
      `INSERT INTO jobs.network_sync_jobs (user_id, provider, scope, dedupe_key, status, triggered_by)
       VALUES ($1::uuid, 'bluesky', 'followings', $2, 'pending', 'integration_test')
       RETURNING id`,
      [testUserId, `it:${testUserId}:bluesky:followings`]
    );

    createdJobId = Number(result.rows[0].id);

    await waitForCondition(async () => {
      const jobs = await redis.lrange(NETWORK_WORKER_PENDING_KEY, 0, 0);
      if (jobs.length === 0) return false;

      try {
        const parsed = JSON.parse(jobs[0]);
        return (
          Number(parsed.job_id) === createdJobId &&
          parsed.user_id === testUserId &&
          parsed.provider === 'bluesky' &&
          parsed.scope === 'followings' &&
          parsed.status === 'pending'
        );
      } catch {
        return false;
      }
    }, 8000, 200);
  }, 20000);

  it('should verify trigger function uses pg_notify on network_sync_jobs channel', async () => {
    const result = await pg.query(`
      SELECT proname, prosrc
      FROM pg_proc
      WHERE proname = 'notify_network_sync_jobs'
    `);

    expect(result.rows.length).toBe(1);
    expect(result.rows[0].prosrc).toContain('pg_notify');
    expect(result.rows[0].prosrc).toContain('network_sync_jobs');
  });

  it('should verify trigger exists on jobs.network_sync_jobs', async () => {
    const result = await pg.query(`
      SELECT tgname
      FROM pg_trigger
      WHERE tgrelid = 'jobs.network_sync_jobs'::regclass
        AND tgname = 'network_sync_jobs_notify_trigger'
    `);

    const names = result.rows.map((r) => r.tgname);
    expect(names).toContain('network_sync_jobs_notify_trigger');
  });
});
