import { randomUUID } from 'crypto'
import { afterAll, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest'
import { Client } from 'pg'

const mockContext = {
  data: {} as any,
  session: null as any,
}

vi.mock('@/lib/validation/middleware', () => ({
  withValidation: (_schema: unknown, handler: (request: Request, data: any, session: any) => Promise<any>) => {
    return async (request: Request) => handler(request, mockContext.data, mockContext.session)
  },
}))

const pgDirectConfig = {
  host: process.env.POSTGRES_HOST || 'localhost',
  port: parseInt(process.env.POSTGRES_DIRECT_PORT || '5432'),
  database: process.env.POSTGRES_DB || 'nexus',
  user: process.env.POSTGRES_USER || 'postgres',
  password: process.env.POSTGRES_PASSWORD || 'mysecretpassword',
}

type MockRouteResponse<T = any> = {
  data: T
}

function asMockRouteResponse<T = any>(value: unknown): MockRouteResponse<T> {
  return value as MockRouteResponse<T>
}

async function insertTwitterSocialAccount(
  pg: Client,
  userId: string,
  twitterId: string,
  username: string,
  email: string
) {
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
    [userId, twitterId, username, email]
  )
}

describe('GET /api/migrate/matching_found', () => {
  let pg: Client

  const testUserId = randomUUID()
  const parallelUserId = randomUUID()
  const nonOnboardedUserId = randomUUID()
  const sourceBlueskyUserId = randomUUID()
  const sourceMastodonUserId = randomUUID()
  const sourceTwitterId = BigInt(`7${Date.now().toString().slice(-15)}`)
  const parallelSourceTwitterId = sourceTwitterId + BigInt(1)
  const nonOnboardedTwitterId = sourceTwitterId + BigInt(2)
  const sourceBlueskyTwitterId = sourceTwitterId + BigInt(401)
  const sourceMastodonTwitterId = sourceTwitterId + BigInt(402)
  const blueskyNodeId = sourceTwitterId + BigInt(101)
  const mastodonNodeId = sourceTwitterId + BigInt(202)
  const syncedNodeId = sourceTwitterId + BigInt(303)
  const mastodonId = `masto-${randomUUID().slice(0, 8)}`

  beforeAll(async () => {
    pg = new Client(pgDirectConfig)
    await pg.connect()

    await pg.query(
      `INSERT INTO "next-auth".users (
         id,
         name,
         email,
         created_at,
         updated_at,
         has_onboarded,
         hqx_newsletter,
         oep_accepted,
         research_accepted,
         have_seen_newsletter,
         automatic_reconnect
       )
       VALUES ($1, $2, $3, now(), now(), true, false, false, false, false, false)`,
      [testUserId, 'matching-found-test-user', `matching-found-${testUserId}@example.com`]
    )

    await insertTwitterSocialAccount(
      pg,
      testUserId,
      sourceTwitterId.toString(),
      'matching-found-test-user',
      `matching-found-${testUserId}@example.com`
    )

    await pg.query(
      `INSERT INTO "next-auth".users (
         id,
         name,
         email,
         created_at,
         updated_at,
         has_onboarded,
         hqx_newsletter,
         oep_accepted,
         research_accepted,
         have_seen_newsletter,
         automatic_reconnect
       )
       VALUES
         ($1, $2, $3, now(), now(), true, false, false, false, false, false),
         ($4, $5, $6, now(), now(), false, false, false, false, false, false),
         ($7, $8, $9, now(), now(), true, false, false, false, false, false),
         ($10, $11, $12, now(), now(), true, false, false, false, false, false)`,
      [
        parallelUserId, 'matching-found-parallel-user', `matching-found-${parallelUserId}@example.com`,
        nonOnboardedUserId, 'matching-found-non-onboarded-user', `matching-found-${nonOnboardedUserId}@example.com`,
        sourceBlueskyUserId, 'matching-found-source-bluesky', `matching-found-${sourceBlueskyUserId}@example.com`,
        sourceMastodonUserId, 'matching-found-source-mastodon', `matching-found-${sourceMastodonUserId}@example.com`,
      ]
    )

    await insertTwitterSocialAccount(
      pg,
      parallelUserId,
      parallelSourceTwitterId.toString(),
      'matching-found-parallel-user',
      `matching-found-${parallelUserId}@example.com`
    )
    await insertTwitterSocialAccount(
      pg,
      nonOnboardedUserId,
      nonOnboardedTwitterId.toString(),
      'matching-found-non-onboarded-user',
      `matching-found-${nonOnboardedUserId}@example.com`
    )
    await insertTwitterSocialAccount(
      pg,
      sourceBlueskyUserId,
      sourceBlueskyTwitterId.toString(),
      'matching-found-source-bluesky',
      `matching-found-${sourceBlueskyUserId}@example.com`
    )
    await insertTwitterSocialAccount(
      pg,
      sourceMastodonUserId,
      sourceMastodonTwitterId.toString(),
      'matching-found-source-mastodon',
      `matching-found-${sourceMastodonUserId}@example.com`
    )

    await pg.query(
      `INSERT INTO network.sources (id)
       VALUES ($1)
       ON CONFLICT (id) DO NOTHING`,
      [testUserId]
    )

    await pg.query(
      `INSERT INTO network.sources (id)
       VALUES ($1)
       ON CONFLICT (id) DO NOTHING`,
      [parallelUserId]
    )

    await pg.query(
      `INSERT INTO network.sources (id)
       VALUES ($1), ($2)
       ON CONFLICT (id) DO NOTHING`,
      [sourceBlueskyUserId, sourceMastodonUserId]
    )

    await pg.query(
      `INSERT INTO network.nodes (twitter_id, bluesky_handle, bluesky_unavailable, mastodon_unavailable)
       VALUES ($1, $2, false, false)
       ON CONFLICT (twitter_id) DO UPDATE SET
         bluesky_handle = EXCLUDED.bluesky_handle,
         bluesky_unavailable = false,
         mastodon_unavailable = false`,
      [blueskyNodeId.toString(), 'matching-found-target.bsky.social']
    )

    await pg.query(
      `INSERT INTO network.nodes (
         twitter_id,
         mastodon_id,
         mastodon_username,
         mastodon_instance,
         bluesky_unavailable,
         mastodon_unavailable
       )
       VALUES ($1, $2, $3, $4, false, false)
       ON CONFLICT (twitter_id) DO UPDATE SET
         mastodon_id = EXCLUDED.mastodon_id,
         mastodon_username = EXCLUDED.mastodon_username,
         mastodon_instance = EXCLUDED.mastodon_instance,
         bluesky_unavailable = false,
         mastodon_unavailable = false`,
      [mastodonNodeId.toString(), mastodonId, 'matchingfound', 'https://target.social']
    )

    await pg.query(
      `INSERT INTO network.nodes (twitter_id, bluesky_unavailable, mastodon_unavailable)
       VALUES ($1, false, false)
       ON CONFLICT (twitter_id) DO NOTHING`,
      [nonOnboardedTwitterId.toString()]
    )
  })

  beforeEach(async () => {
    mockContext.data = {}
    mockContext.session = {
      user: {
        id: testUserId,
        twitter_id: sourceTwitterId.toString(),
        has_onboarded: true,
      },
    }

    await pg.query('DELETE FROM network.sources_targets WHERE source_id = $1', [testUserId])
    await pg.query('DELETE FROM network.sources_targets WHERE source_id = $1', [parallelUserId])
    await pg.query('DELETE FROM network.sources_followers WHERE source_id = $1', [sourceBlueskyUserId])
    await pg.query('DELETE FROM network.sources_followers WHERE source_id = $1', [sourceMastodonUserId])
    await pg.query('DELETE FROM public.twitter_bluesky_users WHERE id = $1', [parallelUserId])
    await pg.query('DELETE FROM public.twitter_mastodon_users WHERE id = $1', [parallelUserId])
    await pg.query('DELETE FROM public.twitter_bluesky_users WHERE id = $1', [sourceBlueskyUserId])
    await pg.query('DELETE FROM public.twitter_mastodon_users WHERE id = $1', [sourceMastodonUserId])
    await pg.query(
      `UPDATE network.nodes
       SET bluesky_handle = $2,
           mastodon_id = $3,
           mastodon_username = $4,
           mastodon_instance = $5,
           bluesky_unavailable = false,
           mastodon_unavailable = false
       WHERE twitter_id = $1`,
      [mastodonNodeId.toString(), null, mastodonId, 'matchingfound', 'https://target.social']
    )
    await pg.query('DELETE FROM network.nodes WHERE twitter_id = $1', [syncedNodeId.toString()])
  })

  afterAll(async () => {
    try {
      await pg.query('DELETE FROM network.sources_targets WHERE source_id = $1', [testUserId])
      await pg.query('DELETE FROM network.sources_targets WHERE source_id = $1', [parallelUserId])
      await pg.query('DELETE FROM network.sources_followers WHERE source_id = $1', [sourceBlueskyUserId])
      await pg.query('DELETE FROM network.sources_followers WHERE source_id = $1', [sourceMastodonUserId])
      await pg.query('DELETE FROM public.twitter_bluesky_users WHERE id = $1', [parallelUserId])
      await pg.query('DELETE FROM public.twitter_mastodon_users WHERE id = $1', [parallelUserId])
      await pg.query('DELETE FROM public.twitter_bluesky_users WHERE id = $1', [sourceBlueskyUserId])
      await pg.query('DELETE FROM public.twitter_mastodon_users WHERE id = $1', [sourceMastodonUserId])
      await pg.query('DELETE FROM network.sources WHERE id = $1', [sourceBlueskyUserId])
      await pg.query('DELETE FROM network.sources WHERE id = $1', [sourceMastodonUserId])
      await pg.query('DELETE FROM network.sources WHERE id = $1', [parallelUserId])
      await pg.query('DELETE FROM network.sources WHERE id = $1', [testUserId])
      await pg.query('DELETE FROM "next-auth".users WHERE id = $1', [sourceBlueskyUserId])
      await pg.query('DELETE FROM "next-auth".users WHERE id = $1', [sourceMastodonUserId])
      await pg.query('DELETE FROM "next-auth".users WHERE id = $1', [nonOnboardedUserId])
      await pg.query('DELETE FROM "next-auth".users WHERE id = $1', [parallelUserId])
      await pg.query('DELETE FROM "next-auth".users WHERE id = $1', [testUserId])
      await pg.query('DELETE FROM network.nodes WHERE twitter_id = ANY($1::bigint[])', [[blueskyNodeId.toString(), mastodonNodeId.toString(), syncedNodeId.toString(), nonOnboardedTwitterId.toString()]])
    } catch {
      // ignore cleanup failures
    }

    await pg.end()
  })

  it('reflects add/remove of source_targets across Bluesky and Mastodon', async () => {
    const { GET } = await import('@/app/api/migrate/matching_found/route')

    let response = asMockRouteResponse(await GET({} as any))
    expect(response.data).toEqual({
      matches: {
        following: [],
        stats: {
          total_following: 0,
          matched_following: 0,
          bluesky_matches: 0,
          mastodon_matches: 0,
        },
      },
    })

    await pg.query(
      `INSERT INTO network.sources_targets (source_id, node_id)
       VALUES ($1, $2)
       ON CONFLICT (source_id, node_id) DO NOTHING`,
      [testUserId, blueskyNodeId.toString()]
    )

    response = asMockRouteResponse(await GET({} as any))
    let body = response.data

    expect(body.matches.stats).toEqual({
      total_following: 1,
      matched_following: 1,
      bluesky_matches: 1,
      mastodon_matches: 0,
    })
    expect(body.matches.following).toHaveLength(1)
    expect(body.matches.following[0]).toMatchObject({
      node_id: blueskyNodeId.toString(),
      bluesky_handle: 'matching-found-target.bsky.social',
      mastodon_id: null,
      mastodon_username: null,
      mastodon_instance: null,
      has_follow_bluesky: false,
      has_follow_mastodon: false,
      dismissed: false,
    })

    await pg.query(
      `INSERT INTO network.sources_targets (source_id, node_id)
       VALUES ($1, $2)
       ON CONFLICT (source_id, node_id) DO NOTHING`,
      [testUserId, mastodonNodeId.toString()]
    )

    response = asMockRouteResponse(await GET({} as any))
    body = response.data

    expect(body.matches.stats).toEqual({
      total_following: 2,
      matched_following: 2,
      bluesky_matches: 1,
      mastodon_matches: 1,
    })
    expect(body.matches.following).toHaveLength(2)
    expect(body.matches.following).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          node_id: blueskyNodeId.toString(),
          bluesky_handle: 'matching-found-target.bsky.social',
        }),
        expect.objectContaining({
          node_id: mastodonNodeId.toString(),
          bluesky_handle: null,
          mastodon_id: mastodonId,
          mastodon_username: 'matchingfound',
          mastodon_instance: 'https://target.social',
          has_follow_bluesky: false,
          has_follow_mastodon: false,
          dismissed: false,
        }),
      ])
    )

    await pg.query(
      `DELETE FROM network.sources_targets
       WHERE source_id = $1 AND node_id = $2`,
      [testUserId, blueskyNodeId.toString()]
    )

    response = asMockRouteResponse(await GET({} as any))
    body = response.data

    expect(body.matches.stats).toEqual({
      total_following: 1,
      matched_following: 1,
      bluesky_matches: 0,
      mastodon_matches: 1,
    })
    expect(body.matches.following).toEqual([
      expect.objectContaining({
        node_id: mastodonNodeId.toString(),
        bluesky_handle: null,
        mastodon_id: mastodonId,
        mastodon_username: 'matchingfound',
        mastodon_instance: 'https://target.social',
      }),
    ])
  })

  it('returns node refreshes triggered by another user sharing the same target node', async () => {
    const { GET } = await import('@/app/api/migrate/matching_found/route')

    await pg.query(
      `INSERT INTO network.sources_targets (source_id, node_id)
       VALUES ($1, $2), ($3, $2)
       ON CONFLICT (source_id, node_id) DO NOTHING`,
      [testUserId, mastodonNodeId.toString(), parallelUserId]
    )

    let response = asMockRouteResponse(await GET({} as any))
    let body = response.data

    expect(body.matches.following).toEqual([
      expect.objectContaining({
        node_id: mastodonNodeId.toString(),
        mastodon_id: mastodonId,
        mastodon_username: 'matchingfound',
        mastodon_instance: 'https://target.social',
      }),
    ])

    await pg.query(
      `UPDATE network.nodes
       SET bluesky_handle = $2,
           mastodon_id = $3,
           mastodon_username = $4,
           mastodon_instance = $5,
           bluesky_unavailable = false,
           mastodon_unavailable = false
       WHERE twitter_id = $1`,
      [mastodonNodeId.toString(), 'refreshed-target.bsky.social', `${mastodonId}-updated`, 'matchingfound-updated', 'https://refreshed.social']
    )

    response = asMockRouteResponse(await GET({} as any))
    body = response.data

    expect(body.matches.stats).toEqual({
      total_following: 1,
      matched_following: 1,
      bluesky_matches: 1,
      mastodon_matches: 1,
    })
    expect(body.matches.following).toEqual([
      expect.objectContaining({
        node_id: mastodonNodeId.toString(),
        bluesky_handle: 'refreshed-target.bsky.social',
        mastodon_id: `${mastodonId}-updated`,
        mastodon_username: 'matchingfound-updated',
        mastodon_instance: 'https://refreshed.social',
      }),
    ])
  })

  it('surfaces updates propagated from twitter_bluesky_users and twitter_mastodon_users through network.nodes', async () => {
    const { GET } = await import('@/app/api/migrate/matching_found/route')

    await pg.query(
      `INSERT INTO network.nodes (twitter_id, bluesky_unavailable, mastodon_unavailable)
       VALUES ($1, false, false)
       ON CONFLICT (twitter_id) DO NOTHING`,
      [syncedNodeId.toString()]
    )

    await pg.query(
      `INSERT INTO network.sources_targets (source_id, node_id)
       VALUES ($1, $2)
       ON CONFLICT (source_id, node_id) DO NOTHING`,
      [testUserId, syncedNodeId.toString()]
    )

    let response = asMockRouteResponse(await GET({} as any))
    let body = response.data

    expect(body.matches.following).toEqual([])

    const syncedMastodonId = `synced-${randomUUID().slice(0, 8)}`

    await pg.query(
      `INSERT INTO public.twitter_bluesky_users (
         id, name, email, twitter_id, twitter_username, bluesky_id, bluesky_username
       )
       VALUES ($1, $2, $3, $4, $5, $6, $7)
       ON CONFLICT (id) DO UPDATE SET
         twitter_id = EXCLUDED.twitter_id,
         twitter_username = EXCLUDED.twitter_username,
         bluesky_id = EXCLUDED.bluesky_id,
         bluesky_username = EXCLUDED.bluesky_username,
         updated_at = now()`,
      [
        parallelUserId,
        'parallel-source',
        `parallel-${parallelUserId}@example.com`,
        syncedNodeId.toString(),
        'parallel-twitter',
        `did:plc:${randomUUID().replace(/-/g, '')}`,
        'synced-target.bsky.social',
      ]
    )

    await pg.query(
      `INSERT INTO public.twitter_mastodon_users (
         id, name, email, twitter_id, twitter_username, mastodon_id, mastodon_username, mastodon_instance
       )
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
       ON CONFLICT (id) DO UPDATE SET
         twitter_id = EXCLUDED.twitter_id,
         twitter_username = EXCLUDED.twitter_username,
         mastodon_id = EXCLUDED.mastodon_id,
         mastodon_username = EXCLUDED.mastodon_username,
         mastodon_instance = EXCLUDED.mastodon_instance,
         updated_at = now()`,
      [
        parallelUserId,
        'parallel-source',
        `parallel-${parallelUserId}@example.com`,
        syncedNodeId.toString(),
        'parallel-twitter',
        syncedMastodonId,
        'syncedmasto',
        'https://synced.social',
      ]
    )

    const nodeResult = await pg.query(
      `SELECT twitter_id::text as twitter_id, bluesky_handle, mastodon_id, mastodon_username, mastodon_instance
       FROM network.nodes
       WHERE twitter_id = $1`,
      [syncedNodeId.toString()]
    )

    expect(nodeResult.rows[0]).toMatchObject({
      twitter_id: syncedNodeId.toString(),
      bluesky_handle: 'synced-target.bsky.social',
      mastodon_id: syncedMastodonId,
      mastodon_username: 'syncedmasto',
      mastodon_instance: 'https://synced.social',
    })

    response = asMockRouteResponse(await GET({} as any))
    body = response.data

    expect(body.matches.stats).toEqual({
      total_following: 1,
      matched_following: 1,
      bluesky_matches: 1,
      mastodon_matches: 1,
    })
    expect(body.matches.following).toEqual([
      expect.objectContaining({
        node_id: syncedNodeId.toString(),
        bluesky_handle: 'synced-target.bsky.social',
        mastodon_id: syncedMastodonId,
        mastodon_username: 'syncedmasto',
        mastodon_instance: 'https://synced.social',
      }),
    ])
  })

  it('returns matching sources for a non-onboarded user via sources_followers', async () => {
    const { GET } = await import('@/app/api/migrate/matching_found/route')

    mockContext.session = {
      user: {
        id: nonOnboardedUserId,
        twitter_id: nonOnboardedTwitterId.toString(),
        has_onboarded: false,
      },
    }

    await pg.query(
      `INSERT INTO network.sources_followers (source_id, node_id)
       VALUES ($1, $2), ($3, $2)
       ON CONFLICT DO NOTHING`,
      [sourceBlueskyUserId, nonOnboardedTwitterId.toString(), sourceMastodonUserId]
    )

    await pg.query(
      `INSERT INTO public.twitter_bluesky_users (
         id, name, email, twitter_id, twitter_username, bluesky_id, bluesky_username
       )
       VALUES ($1, $2, $3, $4, $5, $6, $7)
       ON CONFLICT (id) DO UPDATE SET
         twitter_id = EXCLUDED.twitter_id,
         twitter_username = EXCLUDED.twitter_username,
         bluesky_id = EXCLUDED.bluesky_id,
         bluesky_username = EXCLUDED.bluesky_username,
         updated_at = now()`,
      [
        sourceBlueskyUserId,
        'source-bluesky-user',
        `source-bluesky-${sourceBlueskyUserId}@example.com`,
        sourceBlueskyTwitterId.toString(),
        'source-bluesky-twitter',
        `did:plc:${randomUUID().replace(/-/g, '')}`,
        'source-match.bsky.social',
      ]
    )

    const nonOnboardedMastodonId = `source-masto-${randomUUID().slice(0, 8)}`
    await pg.query(
      `INSERT INTO public.twitter_mastodon_users (
         id, name, email, twitter_id, twitter_username, mastodon_id, mastodon_username, mastodon_instance
       )
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
       ON CONFLICT (id) DO UPDATE SET
         twitter_id = EXCLUDED.twitter_id,
         twitter_username = EXCLUDED.twitter_username,
         mastodon_id = EXCLUDED.mastodon_id,
         mastodon_username = EXCLUDED.mastodon_username,
         mastodon_instance = EXCLUDED.mastodon_instance,
         updated_at = now()`,
      [
        sourceMastodonUserId,
        'source-mastodon-user',
        `source-masto-${sourceMastodonUserId}@example.com`,
        sourceMastodonTwitterId.toString(),
        'source-mastodon-twitter',
        nonOnboardedMastodonId,
        'sourcemasto',
        'https://source.social',
      ]
    )

    const response = asMockRouteResponse(await GET({} as any))
    const body = response.data

    expect(body.matches.stats).toEqual({
      total_following: 2,
      matched_following: 2,
      bluesky_matches: 1,
      mastodon_matches: 1,
    })
    expect(body.matches.following).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          node_id: sourceBlueskyTwitterId.toString(),
          bluesky_handle: 'source-match.bsky.social',
          mastodon_id: null,
          mastodon_username: null,
          mastodon_instance: null,
          has_follow_bluesky: false,
          has_follow_mastodon: false,
        }),
        expect.objectContaining({
          node_id: sourceMastodonTwitterId.toString(),
          bluesky_handle: null,
          mastodon_id: nonOnboardedMastodonId,
          mastodon_username: 'sourcemasto',
          mastodon_instance: 'https://source.social',
          has_follow_bluesky: false,
          has_follow_mastodon: false,
        }),
      ])
    )
  })
})
