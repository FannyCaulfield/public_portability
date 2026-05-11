import { queryNetwork, queryPublic } from '../../database'
import logger from '../../log_utils'

export const pgMatchingHashRepository = {
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
        'pgMatchingHashRepository.getFollowerHashesForSourceUuid',
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
        'pgMatchingHashRepository.getEffectiveFollowerHashesForSource',
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
           SELECT DISTINCT twitter_pa.platform_account_id::bigint as twitter_id
           FROM network.sources_followers sf
           INNER JOIN identity.identities i
             ON i.app_user_id = sf.source_id
           INNER JOIN identity.platform_accounts twitter_pa
             ON twitter_pa.identity_id = i.id
            AND twitter_pa.platform = 'twitter'
            AND twitter_pa.platform_instance = ''
           WHERE sf.node_id = $1::bigint
             AND twitter_pa.platform_account_id IS NOT NULL
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
        'pgMatchingHashRepository.getFollowingHashesForFollower',
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
        'pgMatchingHashRepository.getSourcesOfTargetWithHashes',
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
        'pgMatchingHashRepository.getFollowingHashesForOnboardedUser',
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
        'pgMatchingHashRepository.getCoordHashesByNodeIds',
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
        'pgMatchingHashRepository.getFollowerCommunityStats',
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
         INNER JOIN identity.identities i
           ON i.app_user_id = sf.source_id
         INNER JOIN identity.platform_accounts twitter_pa
           ON twitter_pa.identity_id = i.id
          AND twitter_pa.platform = 'twitter'
          AND twitter_pa.platform_instance = ''
         WHERE twitter_pa.platform_account_id = $1
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
        'pgMatchingHashRepository.getFollowerCommunityStatsForTarget',
        errorString,
        'system',
        { targetTwitterId }
      )
      return { data: null, error }
    }
  },
}
