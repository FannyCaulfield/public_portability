import { MatchingTarget, StoredProcedureTarget, MatchedFollower, FollowerOfSource } from '../../types/matching'
import { queryNetwork, queryPublic, queryNextAuth } from '../../database'
import logger from '../../log_utils'

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
        `SELECT
           st.node_id::text as node_id,
           n.bluesky_handle,
           n.mastodon_id,
           n.mastodon_username,
           n.mastodon_instance,
           st.has_follow_bluesky,
           st.has_follow_mastodon,
           st.followed_at_bluesky,
           st.followed_at_mastodon,
           st.dismissed,
           COUNT(*) OVER() as total_count
         FROM network.sources_targets st
         JOIN network.nodes n ON n.twitter_id = st.node_id
         WHERE st.source_id = $1
           AND (
             (n.bluesky_handle IS NOT NULL AND n.bluesky_unavailable IS NOT TRUE)
             OR
             (n.mastodon_username IS NOT NULL AND n.mastodon_unavailable IS NOT TRUE)
           )
         ORDER BY st.node_id
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
        `WITH bluesky_sources AS (
           SELECT
             sf.source_id,
             tbu.twitter_id,
             tbu.bluesky_username AS bluesky_handle,
             NULL::text AS mastodon_id,
             NULL::text AS mastodon_username,
             NULL::text AS mastodon_instance,
             sf.has_been_followed_on_bluesky,
             false AS has_been_followed_on_mastodon
           FROM network.sources_followers sf
           INNER JOIN public.twitter_bluesky_users tbu ON tbu.id = sf.source_id
           WHERE sf.node_id = $1::bigint
             AND tbu.bluesky_username IS NOT NULL
             AND tbu.bluesky_username <> ''
         ),
         mastodon_sources AS (
           SELECT
             sf.source_id,
             tmu.twitter_id,
             NULL::text AS bluesky_handle,
             tmu.mastodon_id,
             tmu.mastodon_username,
             tmu.mastodon_instance,
             false AS has_been_followed_on_bluesky,
             sf.has_been_followed_on_mastodon
           FROM network.sources_followers sf
           INNER JOIN public.twitter_mastodon_users tmu ON tmu.id = sf.source_id
           WHERE sf.node_id = $1::bigint
             AND tmu.mastodon_username IS NOT NULL
             AND tmu.mastodon_username <> ''
         ),
         combined_sources AS (
           SELECT
             COALESCE(b.source_id, m.source_id) AS source_id,
             COALESCE(b.twitter_id, m.twitter_id) AS twitter_id,
             COALESCE(b.bluesky_handle, m.bluesky_handle) AS bluesky_handle,
             COALESCE(b.mastodon_id, m.mastodon_id) AS mastodon_id,
             COALESCE(b.mastodon_username, m.mastodon_username) AS mastodon_username,
             COALESCE(b.mastodon_instance, m.mastodon_instance) AS mastodon_instance,
             COALESCE(b.has_been_followed_on_bluesky, m.has_been_followed_on_bluesky, false) AS has_been_followed_on_bluesky,
             COALESCE(m.has_been_followed_on_mastodon, b.has_been_followed_on_mastodon, false) AS has_been_followed_on_mastodon
           FROM bluesky_sources b
           FULL OUTER JOIN mastodon_sources m
             ON b.source_id = m.source_id
         )
         SELECT
           cs.twitter_id::text AS source_twitter_id,
           cs.bluesky_handle,
           cs.mastodon_id,
           cs.mastodon_username,
           cs.mastodon_instance,
           cs.has_been_followed_on_bluesky,
           cs.has_been_followed_on_mastodon,
           COUNT(*) OVER() AS total_count
         FROM combined_sources cs
         WHERE cs.twitter_id IS NOT NULL
         ORDER BY cs.twitter_id
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
        `SELECT DISTINCT COALESCE(tbu.twitter_id, tmu.twitter_id)::text as twitter_id
         FROM network.sources_followers sf
         LEFT JOIN public.twitter_bluesky_users tbu ON tbu.id = sf.source_id
         LEFT JOIN public.twitter_mastodon_users tmu ON tmu.id = sf.source_id
         WHERE sf.node_id = $1::bigint
           AND (tbu.twitter_id IS NOT NULL OR tmu.twitter_id IS NOT NULL)`,
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
        `SELECT id as source_id FROM public.twitter_bluesky_users WHERE twitter_id = $1
         UNION
         SELECT id as source_id FROM public.twitter_mastodon_users WHERE twitter_id = $1
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
    try {
      const result = await queryNetwork(
        `SELECT DISTINCT ROUND(gn.x::numeric, 6)::text || '_' || ROUND(gn.y::numeric, 6)::text as coord_hash
         FROM network.sources_followers sf
         INNER JOIN graph.graph_nodes_03_11_25 gn ON gn.id = sf.node_id
         WHERE sf.source_id = $1::uuid`,
        [sourceUuid]
      )

      const data = result.rows.map((row: any) => row.coord_hash)
      console.log('📊 [getFollowerHashesForSourceUuid] Found', data.length, 'follower hashes for source', sourceUuid)
      return { data, error: null }
    } catch (error) {
      const errorString = error instanceof Error ? error.message : String(error)
      logger.logError(
        'Repository',
        'pgMatchingRepository.getFollowerHashesForSourceUuid',
        errorString,
        'system',
        { sourceUuid }
      )
      return { data: null, error }
    }
  },

  async getEffectiveFollowerHashesForSource(
    twitterId: string
  ): Promise<{ data: string[] | null; error: any }> {
    try {
      const result = await queryPublic(
        `SELECT DISTINCT 
           CONCAT(gn.x::text, '_', gn.y::text) as coord_hash
         FROM network.sources_targets st
         INNER JOIN "next-auth".social_accounts sa
           ON sa.user_id = st.source_id
          AND sa.provider = 'twitter'
         INNER JOIN graph.graph_nodes_03_11_25 gn ON gn.id = sa.provider_account_id::bigint
         WHERE st.node_id = $1::bigint
           AND (st.has_follow_bluesky = TRUE OR st.has_follow_mastodon = TRUE)`,
        [twitterId]
      )

      const data = result.rows.map((row: any) => {
        const parts = row.coord_hash.split('_')
        const x = parseFloat(parts[0])
        const y = parseFloat(parts[1])
        return `${x.toFixed(6)}_${y.toFixed(6)}`
      })

      console.log('📊 [getEffectiveFollowerHashesForSource] Found', data.length, 'effective follower hashes for twitter_id', twitterId)
      return { data, error: null }
    } catch (error) {
      const errorString = error instanceof Error ? error.message : String(error)
      logger.logError(
        'Repository',
        'pgMatchingRepository.getEffectiveFollowerHashesForSource',
        errorString,
        'system',
        { twitterId }
      )
      return { data: null, error }
    }
  },

  async getFollowingHashesForFollower(
    followerTwitterId: string
  ): Promise<{ data: string[] | null; error: any }> {
    try {
      const result = await queryNetwork(
        `WITH source_twitter_ids AS (
           SELECT DISTINCT COALESCE(tbu.twitter_id, tmu.twitter_id) as twitter_id
           FROM network.sources_followers sf
           LEFT JOIN public.twitter_bluesky_users tbu ON tbu.id = sf.source_id
           LEFT JOIN public.twitter_mastodon_users tmu ON tmu.id = sf.source_id
           WHERE sf.node_id = $1::bigint
             AND (tbu.twitter_id IS NOT NULL OR tmu.twitter_id IS NOT NULL)
         )
         SELECT ROUND(gn.x::numeric, 6)::text || '_' || ROUND(gn.y::numeric, 6)::text as coord_hash
         FROM source_twitter_ids s
         INNER JOIN graph.graph_nodes_03_11_25 gn ON gn.id = s.twitter_id`,
        [followerTwitterId]
      )

      const data = result.rows.map((row: any) => row.coord_hash)
      console.log('📊 [getFollowingHashesForFollower] Found', data.length, 'following hashes for follower', followerTwitterId)
      return { data, error: null }
    } catch (error) {
      const errorString = error instanceof Error ? error.message : String(error)
      logger.logError(
        'Repository',
        'pgMatchingRepository.getFollowingHashesForFollower',
        errorString,
        'system',
        { followerTwitterId }
      )
      return { data: null, error }
    }
  },

  async getSourcesOfTargetWithHashes(
    targetTwitterId: string,
    pageSize: number = 1000,
    pageNumber: number = 0
  ): Promise<{ data: { hashes: string[]; total_count: number } | null; error: any }> {
    try {
      const safePageSize = Math.max(1, pageSize)
      const safePageNumber = Math.max(0, pageNumber)

      const result = await queryNetwork(
        `WITH sources AS (
           SELECT DISTINCT st.source_id
           FROM network.sources_targets st
           WHERE st.node_id = $1::bigint
         ),
         source_twitter_ids AS (
           SELECT sa.provider_account_id::bigint as twitter_id
           FROM "next-auth".social_accounts sa
           WHERE sa.provider = 'twitter'
             AND sa.user_id IN (SELECT source_id FROM sources)
             AND sa.provider_account_id IS NOT NULL
         ),
         hashes_with_count AS (
           SELECT 
             ROUND(gn.x::numeric, 6)::text || '_' || ROUND(gn.y::numeric, 6)::text as hash,
             COUNT(*) OVER() as total_count
           FROM source_twitter_ids s
           INNER JOIN graph.graph_nodes_03_11_25 gn ON gn.id = s.twitter_id
         )
         SELECT 
           h.hash as coord_hash,
           COALESCE(h.total_count, 0) as total_count
         FROM hashes_with_count h
         ORDER BY h.hash
         LIMIT $2::integer
         OFFSET ($3::integer * $2::integer)`,
        [targetTwitterId, safePageSize, safePageNumber]
      )

      const hashes = result.rows.map((row: any) => row.coord_hash)
      const total_count = result.rows[0]?.total_count || 0

      console.log('📊 [getSourcesOfTargetWithHashes] Found', hashes.length, 'source hashes for target', targetTwitterId)
      return { data: { hashes, total_count }, error: null }
    } catch (error) {
      const errorString = error instanceof Error ? error.message : String(error)
      logger.logError(
        'Repository',
        'pgMatchingRepository.getSourcesOfTargetWithHashes',
        errorString,
        'system',
        { targetTwitterId, pageSize, pageNumber }
      )
      return { data: null, error }
    }
  },

  async getFollowingHashesForOnboardedUser(
    userId: string
  ): Promise<{ data: { coord_hash: string; node_id: string; has_follow_bluesky: boolean; has_follow_mastodon: boolean }[] | null; error: any }> {
    try {
      const result = await queryNetwork(
        `SELECT CONCAT(
           ROUND(gn.x::numeric, 6)::text, '_', 
           ROUND(gn.y::numeric, 6)::text
         ) as coord_hash,
         st.node_id::text as node_id,
         COALESCE(st.has_follow_bluesky, false) as has_follow_bluesky,
         COALESCE(st.has_follow_mastodon, false) as has_follow_mastodon
         FROM sources_targets st
         INNER JOIN graph.graph_nodes_03_11_25 gn ON gn.id = st.node_id
         WHERE st.source_id = $1`,
        [userId]
      )

      const data = result.rows.map((row: any) => ({
        coord_hash: row.coord_hash,
        node_id: row.node_id,
        has_follow_bluesky: row.has_follow_bluesky,
        has_follow_mastodon: row.has_follow_mastodon,
      }))
      const followedCount = data.filter((d: { has_follow_bluesky: boolean; has_follow_mastodon: boolean }) => d.has_follow_bluesky || d.has_follow_mastodon).length
      console.log('📊 [getFollowingHashesForOnboardedUser] Found', data.length, 'following hashes for user', userId, `(${followedCount} already followed)`)
      return { data, error: null }
    } catch (error) {
      const errorString = error instanceof Error ? error.message : String(error)
      logger.logError(
        'Repository',
        'pgMatchingRepository.getFollowingHashesForOnboardedUser',
        errorString,
        'system',
        { userId }
      )
      return { data: null, error }
    }
  },

  async getCoordHashesByNodeIds(
    nodeIds: string[]
  ): Promise<{ data: Map<string, string> | null; error: any }> {
    if (nodeIds.length === 0) {
      return { data: new Map(), error: null }
    }

    try {
      const nodeIdsBigInt = nodeIds.map(id => BigInt(id))

      const result = await queryNetwork(
        `SELECT 
           id::text as node_id,
           CONCAT(
             ROUND(x::numeric, 6)::text, '_', 
             ROUND(y::numeric, 6)::text
           ) as coord_hash
         FROM graph.graph_nodes_03_11_25
         WHERE id = ANY($1::bigint[])`,
        [nodeIdsBigInt]
      )

      const hashMap = new Map<string, string>()
      result.rows.forEach((row: any) => {
        hashMap.set(row.node_id, row.coord_hash)
      })

      return { data: hashMap, error: null }
    } catch (error) {
      const errorString = error instanceof Error ? error.message : String(error)
      logger.logError(
        'Repository',
        'pgMatchingRepository.getCoordHashesByNodeIds',
        errorString,
        'system',
        { nodeIdsCount: nodeIds.length }
      )
      return { data: null, error }
    }
  },

  async getFollowerCommunityStats(
    sourceUuid: string
  ): Promise<{ data: { communities: Array<{ community: number; count: number; percentage: number }>; totalFollowersInGraph: number } | null; error: any }> {
    try {
      const result = await queryPublic(
        `SELECT 
           gn.community,
           COUNT(*) as count
         FROM network.sources_followers sf
         INNER JOIN graph.graph_nodes_03_11_25 gn ON gn.id = sf.node_id
         WHERE sf.source_id = $1::uuid
           AND gn.community IS NOT NULL
         GROUP BY gn.community
         ORDER BY count DESC`,
        [sourceUuid]
      )

      const totalFollowersInGraph = result.rows.reduce((sum: number, row: any) => sum + parseInt(row.count), 0)

      const communities = result.rows.map((row: any) => ({
        community: parseInt(row.community),
        count: parseInt(row.count),
        percentage: totalFollowersInGraph > 0
          ? parseFloat(((parseInt(row.count) / totalFollowersInGraph) * 100).toFixed(1))
          : 0,
      }))

      return { data: { communities, totalFollowersInGraph }, error: null }
    } catch (error) {
      const errorString = error instanceof Error ? error.message : String(error)
      logger.logError(
        'Repository',
        'pgMatchingRepository.getFollowerCommunityStats',
        errorString,
        'system',
        { sourceUuid }
      )
      return { data: null, error }
    }
  },

  async getFollowerCommunityStatsForTarget(
    targetTwitterId: string
  ): Promise<{ data: { communities: Array<{ community: number; count: number; percentage: number }>; totalFollowersInGraph: number } | null; error: any }> {
    try {
      const result = await queryNetwork(
        `SELECT 
           gn.community,
           COUNT(*) as count
         FROM network.sources_followers sf
         INNER JOIN graph.graph_nodes_03_11_25 gn ON gn.id = sf.node_id
         LEFT JOIN public.twitter_bluesky_users tbu ON tbu.id = sf.source_id
         LEFT JOIN public.twitter_mastodon_users tmu ON tmu.id = sf.source_id
         WHERE COALESCE(tbu.twitter_id, tmu.twitter_id)::text = $1
           AND gn.community IS NOT NULL
         GROUP BY gn.community
         ORDER BY count DESC`,
        [targetTwitterId]
      )

      const totalFollowersInGraph = result.rows.reduce((sum: number, row: any) => sum + parseInt(row.count), 0)

      const communities = result.rows.map((row: any) => ({
        community: parseInt(row.community),
        count: parseInt(row.count),
        percentage: totalFollowersInGraph > 0
          ? parseFloat(((parseInt(row.count) / totalFollowersInGraph) * 100).toFixed(1))
          : 0,
      }))

      return { data: { communities, totalFollowersInGraph }, error: null }
    } catch (error) {
      const errorString = error instanceof Error ? error.message : String(error)
      logger.logError(
        'Repository',
        'pgMatchingRepository.getFollowerCommunityStatsForTarget',
        errorString,
        'system',
        { targetTwitterId }
      )
      return { data: null, error }
    }
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
