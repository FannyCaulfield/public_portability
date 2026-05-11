import { randomUUID } from 'crypto'
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest'
import { Client } from 'pg'
import { pgMatchingHashRepository } from '@/lib/repositories/network/pg-matching-hash-repository'

const pgDirectConfig = {
  host: process.env.POSTGRES_HOST || 'localhost',
  port: parseInt(process.env.POSTGRES_DIRECT_PORT || '5432'),
  database: process.env.POSTGRES_DB || 'nexus',
  user: process.env.POSTGRES_USER || 'postgres',
  password: process.env.POSTGRES_PASSWORD || 'mysecretpassword',
}

describe('PgMatchingHashRepository integration', () => {
  let pg: Client

  const sourceUserId = randomUUID()
  const sourceUserId2 = randomUUID()
  const sourceTwitterId = BigInt(`8${Date.now().toString().slice(-15)}`)
  const sourceTwitterId2 = sourceTwitterId + BigInt(1)
  const followerTwitterId = sourceTwitterId + BigInt(500)
  const sourceIdentityId = randomUUID()
  const sourceIdentityId2 = randomUUID()

  beforeAll(async () => {
    pg = new Client(pgDirectConfig)
    await pg.connect()

    await pg.query(
      `INSERT INTO "next-auth".users (id, name, email, created_at, updated_at)
       VALUES ($1, 'hash-source-1', $2, now(), now()), ($3, 'hash-source-2', $4, now(), now())`,
      [sourceUserId, `hash-${sourceUserId}@example.com`, sourceUserId2, `hash-${sourceUserId2}@example.com`]
    )

    await pg.query(
      `INSERT INTO network.sources (id)
       VALUES ($1), ($2)
       ON CONFLICT (id) DO NOTHING`,
      [sourceUserId, sourceUserId2]
    )
  })

  beforeEach(async () => {
    await pg.query('DELETE FROM network.sources_followers WHERE source_id = ANY($1::uuid[])', [[sourceUserId, sourceUserId2]])
    await pg.query('DELETE FROM identity.platform_accounts WHERE identity_id = ANY($1::uuid[])', [[sourceIdentityId, sourceIdentityId2]])
    await pg.query('DELETE FROM identity.identities WHERE id = ANY($1::uuid[])', [[sourceIdentityId, sourceIdentityId2]])
    await pg.query('DELETE FROM network.nodes WHERE twitter_id = ANY($1::bigint[])', [[sourceTwitterId.toString(), sourceTwitterId2.toString(), followerTwitterId.toString()]])
    await pg.query('DELETE FROM graph.graph_nodes_03_11_25 WHERE id = ANY($1::bigint[])', [[sourceTwitterId.toString(), sourceTwitterId2.toString(), followerTwitterId.toString()]])

    await pg.query(
      `INSERT INTO identity.identities (id, app_user_id, created_at, updated_at, last_seen_at)
       VALUES ($1, $2, now(), now(), now()), ($3, $4, now(), now(), now())`,
      [sourceIdentityId, sourceUserId, sourceIdentityId2, sourceUserId2]
    )

    await pg.query(
      `INSERT INTO identity.platform_accounts (identity_id, platform, platform_account_id, platform_username, platform_instance, created_at, updated_at, last_seen_at)
       VALUES
         ($1, 'twitter', $2, 'hash-source-1', '', now(), now(), now()),
         ($3, 'twitter', $4, 'hash-source-2', '', now(), now(), now())`,
      [sourceIdentityId, sourceTwitterId.toString(), sourceIdentityId2, sourceTwitterId2.toString()]
    )

    await pg.query(
      `INSERT INTO graph.graph_nodes_03_11_25 (id, label, x, y, community, tier, node_type, created_at, updated_at)
       VALUES
         ($1, 'source-1', 1.1111111, 2.2222222, 4, 1, 'member', now(), now()),
         ($2, 'source-2', 3.3333333, 4.4444444, 7, 1, 'member', now(), now()),
         ($3, 'follower', 5.5555555, 6.6666666, 2, 1, 'member', now(), now())`,
      [sourceTwitterId.toString(), sourceTwitterId2.toString(), followerTwitterId.toString()]
    )

    await pg.query(
      `INSERT INTO network.nodes (twitter_id)
       VALUES ($1), ($2), ($3)
       ON CONFLICT (twitter_id) DO NOTHING`,
      [sourceTwitterId.toString(), sourceTwitterId2.toString(), followerTwitterId.toString()]
    )

    await pg.query(
      `INSERT INTO network.sources_followers (source_id, node_id, has_been_followed_on_bluesky, has_been_followed_on_mastodon)
       VALUES
         ($1, $2, false, false),
         ($3, $2, false, false)
       ON CONFLICT DO NOTHING`,
      [sourceUserId, followerTwitterId.toString(), sourceUserId2]
    )
  })

  afterAll(async () => {
    await pg.query('DELETE FROM network.sources_followers WHERE source_id = ANY($1::uuid[])', [[sourceUserId, sourceUserId2]])
    await pg.query('DELETE FROM identity.platform_accounts WHERE identity_id = ANY($1::uuid[])', [[sourceIdentityId, sourceIdentityId2]])
    await pg.query('DELETE FROM identity.identities WHERE id = ANY($1::uuid[])', [[sourceIdentityId, sourceIdentityId2]])
    await pg.query('DELETE FROM network.nodes WHERE twitter_id = ANY($1::bigint[])', [[sourceTwitterId.toString(), sourceTwitterId2.toString(), followerTwitterId.toString()]])
    await pg.query('DELETE FROM graph.graph_nodes_03_11_25 WHERE id = ANY($1::bigint[])', [[sourceTwitterId.toString(), sourceTwitterId2.toString(), followerTwitterId.toString()]])
    await pg.query('DELETE FROM network.sources WHERE id = ANY($1::uuid[])', [[sourceUserId, sourceUserId2]])
    await pg.query('DELETE FROM "next-auth".users WHERE id = ANY($1::uuid[])', [[sourceUserId, sourceUserId2]])
    await pg.end()
  })

  it('returns following hashes for a non-onboarded follower using identity platform accounts only', async () => {
    const result = await pgMatchingHashRepository.getFollowingHashesForFollower(followerTwitterId.toString())

    expect(result.error).toBeNull()
    expect(result.data).toEqual(
      expect.arrayContaining([
        '1.111111_2.222222',
        '3.333333_4.444444',
      ])
    )
  })

  it('returns community stats for a target using identity platform accounts only', async () => {
    const result = await pgMatchingHashRepository.getFollowerCommunityStatsForTarget(sourceTwitterId.toString())

    expect(result.error).toBeNull()
    expect(result.data).not.toBeNull()
    expect(result.data?.totalFollowersInGraph).toBe(1)
    expect(result.data?.communities).toEqual([
      expect.objectContaining({
        community: 2,
        count: 1,
        percentage: 100,
      }),
    ])
  })
})
