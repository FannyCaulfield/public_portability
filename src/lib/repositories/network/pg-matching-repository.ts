import { MatchingTarget, StoredProcedureTarget, MatchedFollower, FollowerOfSource } from '../../types/matching'
import { queryNetwork, queryPublic, queryNextAuth } from '../../database'
import logger from '../../log_utils'
import { pgMatchingHashRepository } from './pg-matching-hash-repository'

export const pgMatchingRepository = {
  async getFollowableTargets(
    userId: string,
    pageSize: number = 1000,
    pageNumber: number = 0
  ): Promise<{ data: StoredProcedureTarget[] | null; error: any }> {
    try {
      logger.logDebug(
        'Repository',
        'pgMatchingRepository.getFollowableTargets',
        `Fetching followable targets - userId: ${userId}, pageSize: ${pageSize}, pageNumber: ${pageNumber}`
      )

      const safePageSize = Math.max(1, pageSize)
      const safePageNumber = Math.max(0, pageNumber)

      const result = await queryNetwork(
        `WITH resolved_targets AS (
           SELECT
             st.node_id,
             bluesky_pa.platform_username AS bluesky_handle,
             mastodon_pa.platform_account_id AS mastodon_id,
             mastodon_pa.platform_username AS mastodon_username,
             NULLIF(mastodon_pa.platform_instance, '') AS mastodon_instance,
             st.has_follow_bluesky,
             st.has_follow_mastodon,
             st.followed_at_bluesky,
             st.followed_at_mastodon,
             st.dismissed
           FROM network.sources_targets st
           LEFT JOIN identity.platform_accounts twitter_pa
             ON twitter_pa.platform = 'twitter'
            AND twitter_pa.platform_account_id = st.node_id::text
            AND twitter_pa.platform_instance = ''
           LEFT JOIN identity.platform_accounts bluesky_pa
             ON bluesky_pa.identity_id = twitter_pa.identity_id
            AND bluesky_pa.platform = 'bluesky'
           LEFT JOIN identity.platform_accounts mastodon_pa
             ON mastodon_pa.identity_id = twitter_pa.identity_id
            AND mastodon_pa.platform = 'mastodon'
           WHERE st.source_id = $1
             AND (
               (bluesky_pa.platform_username IS NOT NULL AND bluesky_pa.platform_username <> '')
               OR
               (mastodon_pa.platform_username IS NOT NULL AND mastodon_pa.platform_username <> '')
             )
         )
         SELECT
           rt.node_id::text as node_id,
           rt.bluesky_handle,
           rt.mastodon_id,
           rt.mastodon_username,
           rt.mastodon_instance,
           rt.has_follow_bluesky,
           rt.has_follow_mastodon,
           rt.followed_at_bluesky,
           rt.followed_at_mastodon,
           rt.dismissed,
           COUNT(*) OVER() as total_count
         FROM resolved_targets rt
         ORDER BY rt.node_id
         LIMIT $2::integer
         OFFSET ($3::integer * $2::integer)`,
        [userId, safePageSize, safePageNumber]
      )

      const data = result.rows.map((row: any) => ({
        node_id: String(row.node_id),
        bluesky_handle: row.bluesky_handle ?? null,
        mastodon_id: row.mastodon_id ?? null,
        mastodon_username: row.mastodon_username ?? null,
        mastodon_instance: row.mastodon_instance ?? null,
        has_follow_bluesky: row.has_follow_bluesky ?? false,
        has_follow_mastodon: row.has_follow_mastodon ?? false,
        followed_at_bluesky: row.followed_at_bluesky ?? null,
        followed_at_mastodon: row.followed_at_mastodon ?? null,
        dismissed: row.dismissed ?? false,
        total_count: Number(row.total_count) ?? 0,
      })) as StoredProcedureTarget[]

      logger.logDebug(
        'Repository',
        'pgMatchingRepository.getFollowableTargets',
        `Retrieved ${data.length} followable targets`
      )

      return { data, error: null }
    } catch (error) {
      const errorString = error instanceof Error ? error.message : String(error)
      logger.logError(
        'Repository',
        'pgMatchingRepository.getFollowableTargets',
        errorString,
        userId,
        { pageSize, pageNumber }
      )
      return { data: null, error }
    }
  },

  async updateFollowStatus(
    userId: string,
    targetId: string,
    platform: 'bluesky' | 'mastodon',
    success: boolean,
    error?: string
  ): Promise<void> {
    try {
      const now = new Date().toISOString()

      const updates =
        platform === 'bluesky'
          ? {
              has_follow_bluesky: success,
              followed_at_bluesky: success ? now : null,
            }
          : {
              has_follow_mastodon: success,
              followed_at_mastodon: success ? now : null,
            }

      const updateQuery =
        platform === 'bluesky'
          ? `UPDATE sources_targets SET has_follow_bluesky = $1, followed_at_bluesky = $2 WHERE source_id = $3 AND node_id = $4`
          : `UPDATE sources_targets SET has_follow_mastodon = $1, followed_at_mastodon = $2 WHERE source_id = $3 AND node_id = $4`

      await queryNetwork(updateQuery, [
        updates[platform === 'bluesky' ? 'has_follow_bluesky' : 'has_follow_mastodon'],
        updates[platform === 'bluesky' ? 'followed_at_bluesky' : 'followed_at_mastodon'],
        userId,
        BigInt(targetId),
      ])

      logger.logDebug(
        'Repository',
        'pgMatchingRepository.updateFollowStatus',
        `Updated follow status - userId: ${userId}, targetId: ${targetId}, platform: ${platform}, success: ${success}`
      )
    } catch (err) {
      const errorString = err instanceof Error ? err.message : String(err)
      logger.logError(
        'Repository',
        'pgMatchingRepository.updateFollowStatus',
        errorString,
        userId,
        { targetId, platform, success }
      )
      throw err
    }
  },

  async updateFollowStatusBatch(
    userId: string,
    targetIds: string[],
    platform: 'bluesky' | 'mastodon',
    success: boolean,
    error?: string
  ): Promise<void> {
    try {
      const now = new Date().toISOString()
      const nodeIdsBigInt = targetIds.map(id => BigInt(id))

      const updateQuery =
        platform === 'bluesky'
          ? `UPDATE sources_targets SET has_follow_bluesky = $1, followed_at_bluesky = $2 WHERE source_id = $3 AND node_id = ANY($4)`
          : `UPDATE sources_targets SET has_follow_mastodon = $1, followed_at_mastodon = $2 WHERE source_id = $3 AND node_id = ANY($4)`

      await queryNetwork(updateQuery, [success, now, userId, nodeIdsBigInt])

      logger.logDebug(
        'Repository',
        'pgMatchingRepository.updateFollowStatusBatch',
        `Updated follow status for ${targetIds.length} targets - userId: ${userId}, platform: ${platform}, success: ${success}`
      )
    } catch (err) {
      const errorString = err instanceof Error ? err.message : String(err)
      logger.logError(
        'Repository',
        'pgMatchingRepository.updateFollowStatusBatch',
        errorString,
        userId,
        { targetCount: targetIds.length, platform, success }
      )
      throw err
    }
  },

  async updateSourcesFollowersStatusBatch(
    followerTwitterId: string,
    sourceTwitterIds: string[],
    platform: 'bluesky' | 'mastodon',
    success: boolean,
    error?: string
  ): Promise<void> {
    try {
      const usersResult = await queryNextAuth(
        `SELECT sa.user_id as id, sa.provider_account_id as twitter_id
         FROM "next-auth".social_accounts sa
         WHERE sa.provider = 'twitter'
           AND sa.provider_account_id = ANY($1::text[])`,
        [sourceTwitterIds.map(id => BigInt(id))]
      )

      if (!usersResult.rows || usersResult.rows.length === 0) {
        logger.logWarning(
          'Repository',
          'pgMatchingRepository.updateSourcesFollowersStatusBatch',
          'No users found for Twitter IDs',
          'unknown',
          { followerTwitterId, sourceTwitterIds }
        )
        throw new Error('No users found for the given Twitter IDs')
      }

      const sourceUUIDs = usersResult.rows.map(row => row.id)
      const now = new Date().toISOString()
      const followerNodeId = BigInt(followerTwitterId)

      const updateQuery =
        platform === 'bluesky'
          ? `UPDATE network.sources_followers SET has_been_followed_on_bluesky = $1, followed_at_bluesky = $2 WHERE node_id = $3 AND source_id = ANY($4)`
          : `UPDATE network.sources_followers SET has_been_followed_on_mastodon = $1, followed_at_mastodon = $2 WHERE node_id = $3 AND source_id = ANY($4)`

      await queryNetwork(updateQuery, [success, success ? now : null, followerNodeId, sourceUUIDs])

      logger.logDebug(
        'Repository',
        'pgMatchingRepository.updateSourcesFollowersStatusBatch',
        `Updated followers status for ${sourceUUIDs.length} sources - followerTwitterId: ${followerTwitterId}, platform: ${platform}, success: ${success}`
      )
    } catch (err) {
      const errorString = err instanceof Error ? err.message : String(err)
      logger.logError(
        'Repository',
        'pgMatchingRepository.updateSourcesFollowersStatusBatch',
        errorString,
        'unknown',
        { followerTwitterId, sourceTwitterIds, platform }
      )
      throw err
    }
  },

  async updateSourcesFollowersStatus(
    followerTwitterId: string,
    sourceId: string,
    platform: 'bluesky' | 'mastodon',
    success: boolean,
    error?: string
  ): Promise<void> {
    return pgMatchingRepository.updateSourcesFollowersStatusBatch(
      followerTwitterId,
      [sourceId],
      platform,
      success,
      error
    )
  },

  async updateSourcesFollowersByNodeIds(
    followerTwitterId: string,
    targetNodeIds: string[],
    platform: 'bluesky' | 'mastodon',
    success: boolean,
    error?: string
  ): Promise<void> {
    try {
      if (!targetNodeIds || targetNodeIds.length === 0) {
        logger.logWarning(
          'Repository',
          'pgMatchingRepository.updateSourcesFollowersByNodeIds',
          'No target node IDs provided',
          'unknown',
          { followerTwitterId }
        )
        return
      }

      const targetNodeIdsBigInt = targetNodeIds.map(id => BigInt(id))
      const followerNodeId = BigInt(followerTwitterId)
      const now = new Date().toISOString()

      const sourcesResult = await queryNextAuth(
        `SELECT sa.user_id as source_id, sa.provider_account_id as node_id
         FROM "next-auth".social_accounts sa
         JOIN network.sources s ON s.id = sa.user_id
         WHERE sa.provider = 'twitter'
           AND sa.provider_account_id = ANY($1::text[])`,
        [targetNodeIdsBigInt]
      )

      if (!sourcesResult.rows || sourcesResult.rows.length === 0) {
        logger.logWarning(
          'Repository',
          'pgMatchingRepository.updateSourcesFollowersByNodeIds',
          'No sources found for target node IDs',
          'unknown',
          { followerTwitterId, targetNodeIds }
        )
        return
      }

      const sourceUUIDs = sourcesResult.rows.map(row => row.source_id)

      const updateQuery =
        platform === 'bluesky'
          ? `UPDATE network.sources_followers SET has_been_followed_on_bluesky = $1, followed_at_bluesky = $2 WHERE node_id = $3 AND source_id = ANY($4)`
          : `UPDATE network.sources_followers SET has_been_followed_on_mastodon = $1, followed_at_mastodon = $2 WHERE node_id = $3 AND source_id = ANY($4)`

      await queryNetwork(updateQuery, [success, now, followerNodeId, sourceUUIDs])

      logger.logDebug(
        'Repository',
        'pgMatchingRepository.updateSourcesFollowersByNodeIds',
        `Updated sources_followers for non-onboarded user - followerTwitterId: ${followerTwitterId}, targetCount: ${sourceUUIDs.length}, platform: ${platform}, success: ${success}`
      )
    } catch (err) {
      const errorString = err instanceof Error ? err.message : String(err)
      logger.logError(
        'Repository',
        'pgMatchingRepository.updateSourcesFollowersByNodeIds',
        errorString,
        'unknown',
        { followerTwitterId, targetNodeIds, platform }
      )
      throw err
    }
  },

  async getSourcesFromFollower(
    twitterId: string,
    pageSize: number = 1000,
    pageNumber: number = 0
  ): Promise<{ data: MatchedFollower[] | null; error: any }> {
    try {
      logger.logDebug(
        'Repository',
        'pgMatchingRepository.getSourcesFromFollower',
        `Fetching sources from follower - twitterId: ${twitterId}, pageSize: ${pageSize}, pageNumber: ${pageNumber}`
      )

      const safePageSize = Math.max(1, Math.min(pageSize, 5000))
      const safePageNumber = Math.max(1, pageNumber + 1)

      const result = await queryNetwork(
        `WITH twitter_sources AS (
           SELECT
             sf.source_id,
             COALESCE(twitter_pa.platform_account_id, sa.provider_account_id)::bigint AS twitter_id,
             bluesky_pa.platform_username AS bluesky_handle,
             mastodon_pa.platform_account_id AS mastodon_id,
             mastodon_pa.platform_username AS mastodon_username,
             NULLIF(mastodon_pa.platform_instance, '') AS mastodon_instance,
             sf.has_been_followed_on_bluesky,
             sf.has_been_followed_on_mastodon
           FROM network.sources_followers sf
           INNER JOIN "next-auth".social_accounts sa
             ON sa.user_id = sf.source_id
            AND sa.provider = 'twitter'
           LEFT JOIN identity.platform_accounts twitter_pa
             ON twitter_pa.platform = 'twitter'
            AND twitter_pa.platform_account_id = sa.provider_account_id
            AND twitter_pa.platform_instance = ''
           LEFT JOIN identity.platform_accounts bluesky_pa
             ON bluesky_pa.identity_id = twitter_pa.identity_id
            AND bluesky_pa.platform = 'bluesky'
           LEFT JOIN identity.platform_accounts mastodon_pa
             ON mastodon_pa.identity_id = twitter_pa.identity_id
            AND mastodon_pa.platform = 'mastodon'
           WHERE sf.node_id = $1::bigint
             AND (
               (bluesky_pa.platform_username IS NOT NULL AND bluesky_pa.platform_username <> '')
               OR (mastodon_pa.platform_username IS NOT NULL AND mastodon_pa.platform_username <> '')
             )
         )
         SELECT
           ts.twitter_id::text AS source_twitter_id,
           ts.bluesky_handle,
           ts.mastodon_id,
           ts.mastodon_username,
           ts.mastodon_instance,
           ts.has_been_followed_on_bluesky,
           ts.has_been_followed_on_mastodon,
           COUNT(*) OVER() AS total_count
         FROM twitter_sources ts
         WHERE ts.twitter_id IS NOT NULL
         ORDER BY ts.twitter_id
         LIMIT $2
         OFFSET (($3 - 1) * $2)`,
        [twitterId, safePageSize, safePageNumber]
      )

      const data = result.rows.map((row: { source_twitter_id: string; bluesky_handle?: string; mastodon_id?: string; mastodon_username?: string; mastodon_instance?: string; has_been_followed_on_bluesky?: boolean; has_been_followed_on_mastodon?: boolean; followed_at_bluesky?: string; followed_at_mastodon?: string; total_count?: number }) => ({
        source_twitter_id: String(row.source_twitter_id),
        bluesky_handle: row.bluesky_handle ?? null,
        mastodon_id: row.mastodon_id ?? null,
        mastodon_username: row.mastodon_username ?? null,
        mastodon_instance: row.mastodon_instance ?? null,
        has_been_followed_on_bluesky: row.has_been_followed_on_bluesky ?? false,
        has_been_followed_on_mastodon: row.has_been_followed_on_mastodon ?? false,
        followed_at_bluesky: row.followed_at_bluesky ?? null,
        followed_at_mastodon: row.followed_at_mastodon ?? null,
        full_count: Number(row.total_count) ?? 0,
      })) as MatchedFollower[]

      logger.logDebug(
        'Repository',
        'pgMatchingRepository.getSourcesFromFollower',
        `Retrieved ${data.length} sources from follower`
      )

      return { data, error: null }
    } catch (error) {
      const errorString = error instanceof Error ? error.message : String(error)
      logger.logError(
        'Repository',
        'pgMatchingRepository.getSourcesFromFollower',
        errorString,
        'unknown',
        { twitterId, pageSize, pageNumber }
      )
      return { data: null, error }
    }
  },

  async ignoreTarget(userId: string, targetTwitterId: string): Promise<void> {
    try {
      await queryNetwork(
        `UPDATE sources_targets SET dismissed = true WHERE source_id = $1 AND node_id = $2`,
        [userId, BigInt(targetTwitterId)]
      )

      logger.logDebug(
        'Repository',
        'pgMatchingRepository.ignoreTarget',
        `Marked target as dismissed - userId: ${userId}, targetTwitterId: ${targetTwitterId}`
      )
    } catch (err) {
      const errorString = err instanceof Error ? err.message : String(err)
      logger.logError(
        'Repository',
        'pgMatchingRepository.ignoreTarget',
        errorString,
        userId,
        { targetTwitterId }
      )
      throw err
    }
  },

  async unignoreTarget(userId: string, targetTwitterId: string): Promise<void> {
    try {
      await queryNetwork(
        `UPDATE sources_targets SET dismissed = false WHERE source_id = $1 AND node_id = $2`,
        [userId, BigInt(targetTwitterId)]
      )

      logger.logDebug(
        'Repository',
        'pgMatchingRepository.unignoreTarget',
        `Marked target as not dismissed - userId: ${userId}, targetTwitterId: ${targetTwitterId}`
      )
    } catch (err) {
      const errorString = err instanceof Error ? err.message : String(err)
      logger.logError(
        'Repository',
        'pgMatchingRepository.unignoreTarget',
        errorString,
        userId,
        { targetTwitterId }
      )
      throw err
    }
  },

  async markNodesAsUnavailableBatch(
    nodeIds: string[],
    platform: 'bluesky' | 'mastodon',
    reason: string
  ): Promise<void> {
    try {
      const userTokenErrors = [
        '"exp" claim timestamp check failed',
        'token expired',
        'invalid token',
        'authentication required',
        'unauthorized',
        'session expired',
      ]

      const reasonLower = reason.toLowerCase()
      const isUserTokenError = userTokenErrors.some(err => reasonLower.includes(err.toLowerCase()))

      if (isUserTokenError) {
        logger.logDebug(
          'Repository',
          'pgMatchingRepository.markNodesAsUnavailableBatch',
          `Skipping marking ${nodeIds.length} nodes as unavailable - reason is a user token error: ${reason}`
        )
        return
      }

      const nodeIdsBigInt = nodeIds.map(id => BigInt(id))

      const updateQuery =
        platform === 'bluesky'
          ? `UPDATE nodes SET bluesky_unavailable = true, failure_reason_bluesky = $1 WHERE twitter_id = ANY($2)`
          : `UPDATE nodes SET mastodon_unavailable = true, failure_reason_mastodon = $1 WHERE twitter_id = ANY($2)`

      await queryNetwork(updateQuery, [reason, nodeIdsBigInt])

      logger.logDebug(
        'Repository',
        'pgMatchingRepository.markNodesAsUnavailableBatch',
        `Marked ${nodeIds.length} nodes as unavailable - platform: ${platform}, reason: ${reason}`
      )
    } catch (err) {
      const errorString = err instanceof Error ? err.message : String(err)
      logger.logError(
        'Repository',
        'pgMatchingRepository.markNodesAsUnavailableBatch',
        errorString,
        'unknown',
        { nodeCount: nodeIds.length, platform, reason }
      )
      throw err
    }
  },

  async getSourcesOfTarget(
    nodeId: string,
    pageSize: number = 1000,
    pageNumber: number = 0
  ): Promise<{ data: { source_id: string; total_count: number }[] | null; error: any }> {
    try {
      logger.logDebug(
        'Repository',
        'pgMatchingRepository.getSourcesOfTarget',
        `Fetching sources of target - nodeId: ${nodeId}, pageSize: ${pageSize}, pageNumber: ${pageNumber}`
      )

      const result = await queryPublic(
        `SELECT * FROM public.get_sources_of_target($1, $2, $3)`,
        [nodeId, pageSize, pageNumber]
      )

      const data = result.rows.map((row: any) => ({
        source_id: String(row.source_id),
        total_count: Number(row.total_count) ?? 0,
      }))

      logger.logDebug(
        'Repository',
        'pgMatchingRepository.getSourcesOfTarget',
        `Retrieved ${data.length} sources of target`
      )

      return { data, error: null }
    } catch (error) {
      const errorString = error instanceof Error ? error.message : String(error)
      logger.logError(
        'Repository',
        'pgMatchingRepository.getSourcesOfTarget',
        errorString,
        'system',
        { nodeId, pageSize, pageNumber }
      )
      return { data: null, error }
    }
  },

  async getSourcesOfTargetWithTwitterId(
    nodeId: string,
    pageSize: number = 1000,
    pageNumber: number = 0
  ): Promise<{ data: { source_id: string; twitter_id: string; total_count: number }[] | null; error: any }> {
    try {
      logger.logDebug(
        'Repository',
        'pgMatchingRepository.getSourcesOfTargetWithTwitterId',
        `Fetching sources of target with twitter_id - nodeId: ${nodeId}, pageSize: ${pageSize}, pageNumber: ${pageNumber}`
      )

      const sourcesResult = await queryPublic(
        `SELECT * FROM public.get_sources_of_target($1, $2, $3)`,
        [nodeId, pageSize, pageNumber]
      )

      if (sourcesResult.rows.length === 0) {
        return { data: [], error: null }
      }

      const sourceIds = sourcesResult.rows.map((row: any) => row.source_id)
      const totalCount = sourcesResult.rows[0]?.total_count || sourcesResult.rows.length

      const usersResult = await queryNextAuth(
        `SELECT sa.user_id as id, sa.provider_account_id as twitter_id
         FROM "next-auth".social_accounts sa
         WHERE sa.provider = 'twitter'
           AND sa.user_id = ANY($1::uuid[])
           AND sa.provider_account_id IS NOT NULL`,
        [sourceIds]
      )

      const twitterIdMap = new Map<string, string>()
      usersResult.rows.forEach((row: any) => {
        if (row.twitter_id) {
          twitterIdMap.set(row.id, String(row.twitter_id))
        }
      })

      const data = sourcesResult.rows
        .filter((row: any) => twitterIdMap.has(row.source_id))
        .map((row: any) => ({
          source_id: String(row.source_id),
          twitter_id: twitterIdMap.get(row.source_id)!,
          total_count: Number(totalCount),
        }))

      logger.logDebug(
        'Repository',
        'pgMatchingRepository.getSourcesOfTargetWithTwitterId',
        `Retrieved ${data.length} sources of target with twitter_id`
      )

      return { data, error: null }
    } catch (error) {
      const errorString = error instanceof Error ? error.message : String(error)
      logger.logError(
        'Repository',
        'pgMatchingRepository.getSourcesOfTargetWithTwitterId',
        errorString,
        'system',
        { nodeId, pageSize, pageNumber }
      )
      return { data: null, error }
    }
  },

  async getFollowableTargetsTwitterIds(
    userId: string
  ): Promise<{ data: string[] | null; error: any }> {
    try {
      const result = await queryNetwork(
        `SELECT DISTINCT node_id::text as twitter_id 
         FROM sources_targets 
         WHERE source_id = $1`,
        [userId]
      )

      const data = result.rows.map((row: any) => row.twitter_id)
      return { data, error: null }
    } catch (error) {
      const errorString = error instanceof Error ? error.message : String(error)
      logger.logError(
        'Repository',
        'pgMatchingRepository.getFollowableTargetsTwitterIds',
        errorString,
        userId
      )
      return { data: null, error }
    }
  },

  async getSourcesTwitterIdsForFollower(
    followerTwitterId: string
  ): Promise<{ data: string[] | null; error: any }> {
    try {
      const result = await queryNetwork(
        `SELECT DISTINCT sa.provider_account_id::text as twitter_id
         FROM network.sources_followers sf
         INNER JOIN "next-auth".social_accounts sa
           ON sa.user_id = sf.source_id
          AND sa.provider = 'twitter'
         WHERE sf.node_id = $1::bigint
           AND sa.provider_account_id IS NOT NULL`,
        [followerTwitterId]
      )

      const data = result.rows.map((row: any) => row.twitter_id)

      console.log('📊 [getSourcesTwitterIdsForFollower] Found', data.length, 'sources for follower', followerTwitterId)
      return { data, error: null }
    } catch (error) {
      const errorString = error instanceof Error ? error.message : String(error)
      logger.logError(
        'Repository',
        'pgMatchingRepository.getSourcesTwitterIdsForFollower',
        errorString,
        'system',
        { followerTwitterId }
      )
      return { data: null, error }
    }
  },

  async getFollowersTwitterIdsForSource(
    userId: string
  ): Promise<{ data: string[] | null; error: any }> {
    try {
      const userResult = await queryNextAuth(
        `SELECT provider_account_id as twitter_id
         FROM "next-auth".social_accounts
         WHERE user_id = $1
           AND provider = 'twitter'
         LIMIT 1`,
        [userId]
      )

      if (userResult.rows.length === 0 || !userResult.rows[0].twitter_id) {
        return { data: [], error: null }
      }

      const sourceTwitterId = String(userResult.rows[0].twitter_id)

      const sourceIdResult = await queryNetwork(
        `SELECT sa.user_id as source_id
         FROM "next-auth".social_accounts sa
         JOIN network.sources s ON s.id = sa.user_id
         WHERE sa.provider = 'twitter'
           AND sa.provider_account_id = $1
         LIMIT 1`,
        [sourceTwitterId]
      )

      if (sourceIdResult.rows.length === 0) {
        console.log('📊 [getFollowersTwitterIdsForSource] No source_id found for twitter_id', sourceTwitterId)
        return { data: [], error: null }
      }

      const sourceId = sourceIdResult.rows[0].source_id

      const result = await queryNetwork(
        `SELECT DISTINCT node_id::text as twitter_id 
         FROM network.sources_followers 
         WHERE source_id = $1`,
        [sourceId]
      )

      const data = result.rows.map((row: any) => row.twitter_id)
      console.log('📊 [getFollowersTwitterIdsForSource] Found', data.length, 'followers for source', sourceTwitterId)
      return { data, error: null }
    } catch (error) {
      const errorString = error instanceof Error ? error.message : String(error)
      logger.logError(
        'Repository',
        'pgMatchingRepository.getFollowersTwitterIdsForSource',
        errorString,
        userId
      )
      return { data: null, error }
    }
  },

  async getSourcesOfTargetTwitterIds(
    targetTwitterId: string
  ): Promise<{ data: string[] | null; error: any }> {
    try {
      const result = await queryNetwork(
        `SELECT DISTINCT st.source_id
         FROM sources_targets st
         WHERE st.node_id = $1`,
        [targetTwitterId]
      )

      if (result.rows.length === 0) {
        return { data: [], error: null }
      }

      const sourceIds = result.rows.map((row: any) => row.source_id)
      const usersResult = await queryNextAuth(
        `SELECT sa.provider_account_id::text as twitter_id
         FROM "next-auth".social_accounts sa
         WHERE sa.provider = 'twitter'
           AND sa.user_id = ANY($1::uuid[])
           AND sa.provider_account_id IS NOT NULL`,
        [sourceIds]
      )

      const data = usersResult.rows.map((row: any) => row.twitter_id)
      return { data, error: null }
    } catch (error) {
      const errorString = error instanceof Error ? error.message : String(error)
      logger.logError(
        'Repository',
        'pgMatchingRepository.getSourcesOfTargetTwitterIds',
        errorString,
        'system',
        { targetTwitterId }
      )
      return { data: null, error }
    }
  },

  async getFollowerHashesForSourceUuid(
    sourceUuid: string
  ): Promise<{ data: string[] | null; error: any }> {
    return pgMatchingHashRepository.getFollowerHashesForSourceUuid(sourceUuid)
  },

  async getEffectiveFollowerHashesForSource(
    twitterId: string
  ): Promise<{ data: string[] | null; error: any }> {
    return pgMatchingHashRepository.getEffectiveFollowerHashesForSource(twitterId)
  },

  async getFollowingHashesForFollower(
    followerTwitterId: string
  ): Promise<{ data: string[] | null; error: any }> {
    return pgMatchingHashRepository.getFollowingHashesForFollower(followerTwitterId)
  },

  async getSourcesOfTargetWithHashes(
    targetTwitterId: string,
    pageSize: number = 1000,
    pageNumber: number = 0
  ): Promise<{ data: { hashes: string[]; total_count: number } | null; error: any }> {
    return pgMatchingHashRepository.getSourcesOfTargetWithHashes(targetTwitterId, pageSize, pageNumber)
  },

  async getFollowingHashesForOnboardedUser(
    userId: string
  ): Promise<{ data: { coord_hash: string; node_id: string; has_follow_bluesky: boolean; has_follow_mastodon: boolean }[] | null; error: any }> {
    return pgMatchingHashRepository.getFollowingHashesForOnboardedUser(userId)
  },

  async getCoordHashesByNodeIds(
    nodeIds: string[]
  ): Promise<{ data: Map<string, string> | null; error: any }> {
    return pgMatchingHashRepository.getCoordHashesByNodeIds(nodeIds)
  },

  async getFollowerCommunityStats(
    sourceUuid: string
  ): Promise<{ data: { communities: Array<{ community: number; count: number; percentage: number }>; totalFollowersInGraph: number } | null; error: any }> {
    return pgMatchingHashRepository.getFollowerCommunityStats(sourceUuid)
  },

  async getFollowerCommunityStatsForTarget(
    targetTwitterId: string
  ): Promise<{ data: { communities: Array<{ community: number; count: number; percentage: number }>; totalFollowersInGraph: number } | null; error: any }> {
    return pgMatchingHashRepository.getFollowerCommunityStatsForTarget(targetTwitterId)
  },

  async getFollowersOfSource(
    sourceId: string,
    pageSize: number = 1000,
    pageNumber: number = 0
  ): Promise<{ data: FollowerOfSource[] | null; error: any }> {
    try {
      const result = await queryPublic(
        `SELECT * FROM public.get_followers_of_source($1, $2, $3)`,
        [sourceId, pageSize, pageNumber]
      )

      const data = result.rows.map((row: any) => ({
        node_id: String(row.node_id ?? row.twitter_id),
        bluesky_handle: row.bluesky_handle ?? null,
        has_follow_bluesky: row.has_follow_bluesky ?? false,
        followed_at_bluesky: row.followed_at_bluesky ?? null,
        followed_at_mastodon: row.followed_at_mastodon ?? null,
        has_been_followed_on_bluesky: row.has_been_followed_on_bluesky ?? false,
        has_been_followed_on_mastodon: row.has_been_followed_on_mastodon ?? false,
        total_count: Number(row.total_count) ?? 0,
      })) as FollowerOfSource[]

      return { data, error: null }
    } catch (error) {
      const errorString = error instanceof Error ? error.message : String(error)
      logger.logError(
        'Repository',
        'pgMatchingRepository.getFollowersOfSource',
        errorString,
        sourceId,
        { pageSize, pageNumber }
      )
      return { data: null, error }
    }
  },
}
