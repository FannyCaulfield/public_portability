import { BlueskySessionData, BlueskyProfile } from '../../types/bluesky'
import { queryNetwork, queryNextAuth } from '../../database'
import { pgAccountRepository } from './pg-account-repository'
import { pgSocialAccountRepository } from './pg-social-account-repository'
import { pgUserRepository } from './pg-user-repository'
import logger from '../../log_utils'

export const pgBlueskyRepository = {
  async getUserByBlueskyId(did: string) {
    try {
      const account = await pgAccountRepository.getAccount('bluesky', did)
      if (!account) return null

      return await pgUserRepository.getUser(account.user_id)
    } catch (error) {
      logger.logWarning(
        'Repository',
        'pgBlueskyRepository.getUserByBlueskyId',
        `Error getting user by Bluesky ID: ${did}`,
        'unknown',
        { did, error }
      )
      return null
    }
  },

  async linkBlueskyAccount(userId: string, blueskyData: BlueskySessionData): Promise<void> {
    try {
      await pgAccountRepository.upsertAccount({
        user_id: userId,
        provider: 'bluesky',
        provider_account_id: blueskyData.did,
        type: 'oauth',
        access_token: blueskyData.accessJwt,
        refresh_token: blueskyData.refreshJwt,
        token_type: ((blueskyData.token_type || 'bearer') as string).toLowerCase() as Lowercase<string>,
        scope: blueskyData.scope,
      })
    } catch (error) {
      const errorString = error instanceof Error ? error.message : String(error)
      logger.logError(
        'Repository',
        'pgBlueskyRepository.linkBlueskyAccount',
        errorString,
        userId,
        {
          did: blueskyData.did,
          context: 'Linking Bluesky account',
        }
      )
      throw error
    }
  },

  async updateBlueskyProfile(userId: string, profile: BlueskyProfile): Promise<void> {
    try {
      const user = await pgUserRepository.getUser(userId)
      if (!user) {
        throw new Error(`User not found: ${userId}`)
      }

      await pgSocialAccountRepository.upsertSocialAccount({
        user_id: userId,
        provider: 'bluesky',
        provider_account_id: profile.did,
        username: profile.handle,
        instance: '',
        email: user.email,
        is_primary: true,
        last_seen_at: new Date(),
      })
    } catch (error) {
      const errorString = error instanceof Error ? error.message : String(error)
      logger.logError(
        'Repository',
        'pgBlueskyRepository.updateBlueskyProfile',
        errorString,
        userId,
        {
          did: profile.did,
          handle: profile.handle,
          context: 'Updating Bluesky profile',
        }
      )
      throw error
    }
  },

  async updateFollowStatus(userId: string, targetTwitterId: string): Promise<void> {
    try {
      await queryNetwork(
        `UPDATE sources_targets 
         SET has_follow_bluesky = true
         WHERE source_id = $1 AND node_id = $2`,
        [userId, targetTwitterId]
      )
    } catch (error) {
      const errorString = error instanceof Error ? error.message : String(error)
      logger.logError(
        'Repository',
        'pgBlueskyRepository.updateFollowStatus',
        errorString,
        userId,
        {
          targetTwitterId,
          context: 'Updating follow status',
        }
      )
      throw error
    }
  },
}
