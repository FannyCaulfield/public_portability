import { queryCache, queryPublic } from '../../database'
import { redis } from '../../redis'
import { UserCompleteStats, GlobalStats } from '../../types/stats'
import logger from '../../log_utils'

export const pgStatsRepository = {
  async getUserCompleteStats(userId: string, has_onboard: boolean): Promise<UserCompleteStats> {
    let data: any
    try {
      if (!has_onboard) {
        const result = await queryPublic(
          `SELECT public.get_user_complete_stats_from_sources($1) as stats`,
          [userId]
        )
        data = result.rows[0]?.stats
      } else {
        const result = await queryPublic(
          `SELECT public.get_user_complete_stats($1) as stats`,
          [userId]
        )
        data = result.rows[0]?.stats
      }

      if (!data) {
        throw new Error(`No stats returned for user ${userId}`)
      }
    } catch (error) {
      const errorString = error instanceof Error ? error.message : String(error)
      logger.logError(
        'Repository',
        'pgStatsRepository.getUserCompleteStats',
        errorString,
        userId,
        { has_onboard }
      )
      throw error
    }

    try {
      const cacheKey = `user:stats:${userId}`
      await redis.set(cacheKey, JSON.stringify(data), 86400)

      logger.logInfo(
        'Repository',
        'pgStatsRepository.getUserCompleteStats',
        'User stats cached in Redis',
        userId,
        { context: 'Database result cached for 24 hours' }
      )
    } catch (redisError) {
      logger.logWarning(
        'Repository',
        'pgStatsRepository.getUserCompleteStats',
        'Failed to cache in Redis',
        userId,
        {
          context: 'Redis caching failed, continuing without cache',
          error: redisError instanceof Error ? redisError.message : 'Unknown Redis error',
        }
      )
    }

    return data as UserCompleteStats
  },

  async getGlobalStats(): Promise<GlobalStats> {
    const fetchGlobalStatsFromDb = async (): Promise<GlobalStats> => {
      let data: GlobalStats | null = null

      try {
        const result = await queryPublic(`SELECT public.get_global_stats_v2() as stats`)
        data = result.rows[0]?.stats ?? null
      } catch (error) {
        logger.logWarning(
          'Repository',
          'pgStatsRepository.getGlobalStats',
          'get_global_stats_v2 failed, falling back to legacy',
          'system',
          { context: 'Fallback to legacy global stats function', error: error instanceof Error ? error.message : String(error) }
        )
      }

      if (!data) {
        data = await pgStatsRepository.getGlobalStatsFromCacheV2()
      }

      if (!data) {
        try {
          const result = await queryPublic(`SELECT public.get_global_stats() as stats`)
          data = result.rows[0]?.stats ?? null
        } catch (error) {
          logger.logWarning(
            'Repository',
            'pgStatsRepository.getGlobalStats',
            'get_global_stats failed, no legacy JSON cache available',
            'system',
            { context: 'Legacy global stats function failed', error: error instanceof Error ? error.message : String(error) }
          )
        }
      }

      if (!data) {
        throw new Error('No global stats returned from database')
      }

      return data as GlobalStats
    }

    try {
      const cached = await redis.get('stats:global')
      if (cached) {
        logger.logInfo(
          'Repository',
          'pgStatsRepository.getGlobalStats',
          'Global stats served from Redis cache',
          'system',
          { context: 'Redis cache hit for global stats' }
        )
        return JSON.parse(cached) as GlobalStats
      }

      logger.logInfo(
        'Repository',
        'pgStatsRepository.getGlobalStats',
        'Redis cache miss, fetching from database',
        'system',
        { context: 'Fallback to database for global stats' }
      )

      const data = await fetchGlobalStatsFromDb()
      await redis.set('stats:global', JSON.stringify(data), 86400)

      logger.logInfo(
        'Repository',
        'pgStatsRepository.getGlobalStats',
        'Global stats fetched from DB and cached',
        'system',
        { context: 'Database fallback successful, data cached in Redis' }
      )

      return data as GlobalStats
    } catch (redisError) {
      logger.logWarning(
        'Repository',
        'pgStatsRepository.getGlobalStats',
        'Redis unavailable, using database fallback',
        'system',
        {
          context: 'Redis error, direct database access',
          error: redisError instanceof Error ? redisError.message : 'Unknown Redis error',
        }
      )

      try {
        return await fetchGlobalStatsFromDb()
      } catch (dbError) {
        const errorString = dbError instanceof Error ? dbError.message : String(dbError)
        logger.logError(
          'Repository',
          'pgStatsRepository.getGlobalStats',
          errorString,
          'system',
          { context: 'Database fallback also failed' }
        )
        throw dbError
      }
    }
  },

  async getGlobalStatsFromCache(): Promise<GlobalStats | null> {
    try {
      const result = await queryCache(
        `SELECT stats FROM global_stats_cache WHERE id = true`
      )

      if (!result.rows[0]) {
        logger.logWarning(
          'Repository',
          'pgStatsRepository.getGlobalStatsFromCache',
          'No data in global_stats_cache',
          'system',
          { context: 'Cache table empty or error' }
        )
        return null
      }

      return result.rows[0].stats as GlobalStats
    } catch (error) {
      const errorString = error instanceof Error ? error.message : String(error)
      logger.logWarning(
        'Repository',
        'pgStatsRepository.getGlobalStatsFromCache',
        'Legacy JSON cache unavailable, falling back to v2 columns',
        'system',
        { context: 'Failed to read stats column from global_stats_cache', error: errorString }
      )
      return pgStatsRepository.getGlobalStatsFromCacheV2()
    }
  },

  async getGlobalStatsFromCacheV2(): Promise<GlobalStats | null> {
    try {
      const fetchFromTable = async (tableName: 'global_stats_cache' | 'global_stats_cache_v2') => {
        const result = await queryCache(`
          SELECT 
            users_total,
            users_onboarded,
            followers,
            following,
            with_handle,
            with_handle_bluesky,
            with_handle_mastodon,
            followed_on_bluesky,
            followed_on_mastodon,
            updated_at
          FROM ${tableName}
          WHERE id = true
        `)

        if (!result.rows[0]) {
          logger.logWarning(
            'Repository',
            'pgStatsRepository.getGlobalStatsFromCacheV2',
            `No data in ${tableName}`,
            'system',
            { context: 'Cache v2 table empty' }
          )
          return null
        }

        const row = result.rows[0]
        return {
          users: {
            total: Number(row.users_total) || 0,
            onboarded: Number(row.users_onboarded) || 0,
          },
          connections: {
            followers: Number(row.followers) || 0,
            following: Number(row.following) || 0,
            withHandle: Number(row.with_handle) || 0,
            withHandleBluesky: Number(row.with_handle_bluesky) || 0,
            withHandleMastodon: Number(row.with_handle_mastodon) || 0,
            followedOnBluesky: Number(row.followed_on_bluesky) || 0,
            followedOnMastodon: Number(row.followed_on_mastodon) || 0,
          },
          updated_at: row.updated_at?.toISOString?.() || row.updated_at || new Date().toISOString(),
        }
      }

      try {
        return await fetchFromTable('global_stats_cache')
      } catch (error) {
        logger.logWarning(
          'Repository',
          'pgStatsRepository.getGlobalStatsFromCacheV2',
          'global_stats_cache v2 columns unavailable, trying legacy v2 table',
          'system',
          { context: 'Fallback to global_stats_cache_v2', error: error instanceof Error ? error.message : String(error) }
        )
      }

      return await fetchFromTable('global_stats_cache_v2')
    } catch (error) {
      const errorString = error instanceof Error ? error.message : String(error)
      logger.logError(
        'Repository',
        'pgStatsRepository.getGlobalStatsFromCacheV2',
        errorString,
        'system',
        { context: 'Failed to read from v2 cache tables' }
      )
      return null
    }
  },

  async refreshUserStatsCache(userId: string, has_onboard: boolean): Promise<void> {
    try {
      if (!has_onboard) {
        await queryPublic(
          `SELECT public.get_user_complete_stats_from_sources($1)`,
          [userId]
        )
      } else {
        await queryPublic(
          `SELECT public.refresh_user_stats_cache($1)`,
          [userId]
        )
      }

      try {
        const cacheKey = `user:stats:${userId}`
        await redis.del(cacheKey)
        logger.logInfo(
          'Repository',
          'pgStatsRepository.refreshUserStatsCache',
          'User stats cache refreshed and Redis invalidated',
          userId,
          { has_onboard }
        )
      } catch (redisError) {
        logger.logWarning(
          'Repository',
          'pgStatsRepository.refreshUserStatsCache',
          'Failed to invalidate Redis cache',
          userId,
          {
            error: redisError instanceof Error ? redisError.message : 'Unknown Redis error',
          }
        )
      }
    } catch (error) {
      const errorString = error instanceof Error ? error.message : String(error)
      logger.logError(
        'Repository',
        'pgStatsRepository.refreshUserStatsCache',
        errorString,
        userId,
        { has_onboard }
      )
      throw error
    }
  },

  async refreshGlobalStatsCache(): Promise<void> {
    try {
      await queryPublic(`SELECT public.refresh_global_stats_cache()`)

      try {
        await redis.del('stats:global')
        logger.logInfo(
          'Repository',
          'pgStatsRepository.refreshGlobalStatsCache',
          'Global stats cache refreshed and Redis invalidated',
          'system'
        )
      } catch (redisError) {
        logger.logWarning(
          'Repository',
          'pgStatsRepository.refreshGlobalStatsCache',
          'Failed to invalidate Redis cache',
          'system',
          {
            error: redisError instanceof Error ? redisError.message : 'Unknown Redis error',
          }
        )
      }
    } catch (error) {
      const errorString = error instanceof Error ? error.message : String(error)
      logger.logError(
        'Repository',
        'pgStatsRepository.refreshGlobalStatsCache',
        errorString,
        'unknown'
      )
      throw error
    }
  },

  async refreshGlobalStatsUsers(): Promise<void> {
    try {
      await queryPublic(`SELECT public.refresh_global_stats_users()`)

      try {
        await redis.del('stats:global')
        logger.logInfo(
          'Repository',
          'pgStatsRepository.refreshGlobalStatsUsers',
          'Users stats refreshed and Redis invalidated',
          'system'
        )
      } catch (redisError) {
        logger.logWarning(
          'Repository',
          'pgStatsRepository.refreshGlobalStatsUsers',
          'Failed to invalidate Redis cache',
          'system',
          { error: redisError instanceof Error ? redisError.message : 'Unknown Redis error' }
        )
      }
    } catch (error) {
      const errorString = error instanceof Error ? error.message : String(error)
      logger.logError(
        'Repository',
        'pgStatsRepository.refreshGlobalStatsUsers',
        errorString,
        'system'
      )
      throw error
    }
  },

  async refreshGlobalStatsConnections(): Promise<void> {
    try {
      await queryPublic(`SELECT public.refresh_global_stats_connections()`)

      try {
        await redis.del('stats:global')
        logger.logInfo(
          'Repository',
          'pgStatsRepository.refreshGlobalStatsConnections',
          'Connections stats refreshed and Redis invalidated',
          'system'
        )
      } catch (redisError) {
        logger.logWarning(
          'Repository',
          'pgStatsRepository.refreshGlobalStatsConnections',
          'Failed to invalidate Redis cache',
          'system',
          { error: redisError instanceof Error ? redisError.message : 'Unknown Redis error' }
        )
      }
    } catch (error) {
      const errorString = error instanceof Error ? error.message : String(error)
      logger.logError(
        'Repository',
        'pgStatsRepository.refreshGlobalStatsConnections',
        errorString,
        'system'
      )
      throw error
    }
  },

  async refreshGlobalStatsHeavy(): Promise<void> {
    try {
      await queryPublic(`SELECT public.refresh_global_stats_heavy()`)

      try {
        await redis.del('stats:global')
        logger.logInfo(
          'Repository',
          'pgStatsRepository.refreshGlobalStatsHeavy',
          'Heavy stats refreshed and Redis invalidated',
          'system'
        )
      } catch (redisError) {
        logger.logWarning(
          'Repository',
          'pgStatsRepository.refreshGlobalStatsHeavy',
          'Failed to invalidate Redis cache',
          'system',
          { error: redisError instanceof Error ? redisError.message : 'Unknown Redis error' }
        )
      }
    } catch (error) {
      const errorString = error instanceof Error ? error.message : String(error)
      logger.logError(
        'Repository',
        'pgStatsRepository.refreshGlobalStatsHeavy',
        errorString,
        'system'
      )
      throw error
    }
  },

  async refreshGlobalStatsCacheV2(): Promise<void> {
    try {
      await queryPublic(`SELECT public.refresh_global_stats_cache_v2()`)

      try {
        await redis.del('stats:global')
        logger.logInfo(
          'Repository',
          'pgStatsRepository.refreshGlobalStatsCacheV2',
          'Full stats v2 refreshed and Redis invalidated',
          'system'
        )
      } catch (redisError) {
        logger.logWarning(
          'Repository',
          'pgStatsRepository.refreshGlobalStatsCacheV2',
          'Failed to invalidate Redis cache',
          'system',
          { error: redisError instanceof Error ? redisError.message : 'Unknown Redis error' }
        )
      }
    } catch (error) {
      const errorString = error instanceof Error ? error.message : String(error)
      logger.logError(
        'Repository',
        'pgStatsRepository.refreshGlobalStatsCacheV2',
        errorString,
        'system'
      )
      throw error
    }
  },
}
