import { queryNextAuth } from '../../database'
import type { DBSocialAccount } from '../../types/database'
import logger from '../../log_utils'

type SocialProvider = 'twitter' | 'bluesky' | 'mastodon' | 'linkedin' | 'facebook'

interface UpsertSocialAccountInput {
  user_id: string
  provider: SocialProvider
  provider_account_id: string
  username?: string | null
  instance?: string | null
  email?: string | null
  is_primary?: boolean
  last_seen_at?: Date | null
}

export const pgSocialAccountRepository = {
  async getSocialAccountsByUserId(userId: string): Promise<DBSocialAccount[]> {
    try {
      const result = await queryNextAuth<DBSocialAccount>(
        `SELECT *
         FROM "next-auth".social_accounts
         WHERE user_id = $1
         ORDER BY is_primary DESC, updated_at DESC`,
        [userId]
      )

      return result.rows
    } catch (error) {
      logger.logError('Repository', 'pgSocialAccountRepository.getSocialAccountsByUserId', 'Error fetching social accounts', userId, { error })
      throw error
    }
  },

  async getSocialAccountByProviderAccountId(
    provider: SocialProvider,
    providerAccountId: string,
    instance?: string | null
  ): Promise<DBSocialAccount | null> {
    try {
      const normalizedInstance = instance ?? ''
      const result = provider === 'mastodon'
        ? await queryNextAuth<DBSocialAccount>(
            `SELECT *
             FROM "next-auth".social_accounts
             WHERE provider = $1
               AND provider_account_id = $2
               AND instance = $3
             LIMIT 1`,
            [provider, providerAccountId, normalizedInstance]
          )
        : await queryNextAuth<DBSocialAccount>(
            `SELECT *
             FROM "next-auth".social_accounts
             WHERE provider = $1
               AND provider_account_id = $2
             LIMIT 1`,
            [provider, providerAccountId]
          )

      return result.rows[0] || null
    } catch (error) {
      logger.logError('Repository', 'pgSocialAccountRepository.getSocialAccountByProviderAccountId', 'Error fetching social account by provider account', undefined, {
        provider,
        providerAccountId,
        instance,
        error,
      })
      throw error
    }
  },

  async countSocialAccountsByUserId(userId: string): Promise<number> {
    try {
      const result = await queryNextAuth<{ count: string }>(
        `SELECT COUNT(*)::text AS count
         FROM "next-auth".social_accounts
         WHERE user_id = $1`,
        [userId]
      )

      return Number(result.rows[0]?.count ?? 0)
    } catch (error) {
      logger.logError('Repository', 'pgSocialAccountRepository.countSocialAccountsByUserId', 'Error counting social accounts', userId, { error })
      throw error
    }
  },

  async upsertSocialAccount(input: UpsertSocialAccountInput): Promise<DBSocialAccount> {
    try {
      const normalizedInstance = input.provider === 'mastodon' ? (input.instance ?? '') : ''
      const isPrimary = input.is_primary ?? true
      const lastSeenAt = input.last_seen_at ?? new Date()

      const updated = input.provider === 'mastodon'
        ? await queryNextAuth<DBSocialAccount>(
            `UPDATE "next-auth".social_accounts
             SET user_id = $1,
                 username = $4,
                 instance = $5,
                 email = COALESCE($6, email),
                 is_primary = $7,
                 last_seen_at = $8,
                 updated_at = NOW()
             WHERE provider = $2
               AND provider_account_id = $3
               AND instance = $5
             RETURNING *`,
            [input.user_id, input.provider, input.provider_account_id, input.username ?? null, normalizedInstance, input.email ?? null, isPrimary, lastSeenAt]
          )
        : await queryNextAuth<DBSocialAccount>(
            `UPDATE "next-auth".social_accounts
             SET user_id = $1,
                 username = $4,
                 instance = $5,
                 email = COALESCE($6, email),
                 is_primary = $7,
                 last_seen_at = $8,
                 updated_at = NOW()
             WHERE provider = $2
               AND provider_account_id = $3
             RETURNING *`,
            [input.user_id, input.provider, input.provider_account_id, input.username ?? null, normalizedInstance, input.email ?? null, isPrimary, lastSeenAt]
          )

      if (updated.rows[0]) {
        return updated.rows[0]
      }

      const inserted = await queryNextAuth<DBSocialAccount>(
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
         SELECT $1, $2, $3, $4, $5, $6, $7, $8
         WHERE NOT EXISTS (
           SELECT 1
           FROM "next-auth".social_accounts sa
           WHERE sa.provider = $2
             AND sa.provider_account_id = $3
             AND ($2 <> 'mastodon' OR sa.instance = $5)
         )
         RETURNING *`,
        [input.user_id, input.provider, input.provider_account_id, input.username ?? null, normalizedInstance, input.email ?? null, isPrimary, lastSeenAt]
      )

      if (inserted.rows[0]) {
        return inserted.rows[0]
      }

      const existing = await this.getSocialAccountByProviderAccountId(input.provider, input.provider_account_id, normalizedInstance)
      if (!existing) {
        throw new Error('Failed to upsert social account')
      }

      return existing
    } catch (error) {
      logger.logError('Repository', 'pgSocialAccountRepository.upsertSocialAccount', 'Error upserting social account', input.user_id, { input, error })
      throw error
    }
  },

  async deleteSocialAccount(
    userId: string,
    provider: SocialProvider,
    providerAccountId?: string | null,
    instance?: string | null
  ): Promise<void> {
    try {
      if (provider === 'mastodon' && providerAccountId) {
        await queryNextAuth(
          `DELETE FROM "next-auth".social_accounts
           WHERE user_id = $1
             AND provider = $2
             AND provider_account_id = $3
             AND instance = $4`,
          [userId, provider, providerAccountId, instance ?? '']
        )
        return
      }

      if (providerAccountId) {
        await queryNextAuth(
          `DELETE FROM "next-auth".social_accounts
           WHERE user_id = $1
             AND provider = $2
             AND provider_account_id = $3`,
          [userId, provider, providerAccountId]
        )
        return
      }

      await queryNextAuth(
        `DELETE FROM "next-auth".social_accounts
         WHERE user_id = $1
           AND provider = $2`,
        [userId, provider]
      )
    } catch (error) {
      logger.logError('Repository', 'pgSocialAccountRepository.deleteSocialAccount', 'Error deleting social account', userId, {
        provider,
        providerAccountId,
        instance,
        error,
      })
      throw error
    }
  },
}
