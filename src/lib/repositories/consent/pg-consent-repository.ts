import { queryConsent, transactionConsent } from '../../database'
import logger from '../../log_utils'

export interface NewsletterConsent {
  id?: string
  user_id: string
  consent_type: string
  consent_value: boolean
  ip_address?: string | null
  user_agent?: string | null
  ip_address_full?: string | null
  consent_timestamp?: string
  is_active?: boolean
  created_at?: string
  updated_at?: string
}

export interface ConsentHistoryItem {
  consent_type: string
  consent_value: boolean
  consent_timestamp: string
  is_active: boolean
}

export const pgConsentRepository = {
  async getUserActiveConsents(userId: string): Promise<Record<string, boolean>> {
    try {
      const result = await queryConsent<{ consent_type: string; consent_value: boolean }>(
        `SELECT consent_type, consent_value 
         FROM newsletter_consents 
         WHERE user_id = $1 AND is_active = true`,
        [userId]
      )

      const consents: Record<string, boolean> = {}
      result.rows.forEach((item: { consent_type: string; consent_value: boolean }) => {
        consents[item.consent_type] = item.consent_value
      })

      return consents
    } catch (error) {
      logger.logError('Repository', 'pgConsentRepository.getUserActiveConsents', 'Error fetching active consents', userId, { error })
      throw error
    }
  },

  async getConsentHistory(userId: string, consentType?: string): Promise<ConsentHistoryItem[]> {
    try {
      let query = `SELECT consent_type, consent_value, consent_timestamp, is_active 
                   FROM newsletter_consents 
                   WHERE user_id = $1`
      const params: any[] = [userId]

      if (consentType) {
        query += ` AND consent_type = $2`
        params.push(consentType)
      }

      query += ` ORDER BY consent_timestamp DESC`

      const result = await queryConsent<ConsentHistoryItem>(query, params)
      return result.rows
    } catch (error) {
      logger.logError('Repository', 'pgConsentRepository.getConsentHistory', 'Error fetching consent history', userId, { consentType, error })
      throw error
    }
  },

  async updateConsent(
    userId: string,
    type: string,
    value: boolean,
    metadata?: {
      ip_address?: string
      user_agent?: string
    }
  ): Promise<void> {
    try {
      let firstIpAddress: string | null = null
      let fullIpAddressChain: string | null = null

      if (metadata?.ip_address) {
        fullIpAddressChain = metadata.ip_address
        const ips = metadata.ip_address.split(',').map((ip) => ip.trim()).filter(Boolean)
        if (ips.length > 0) {
          firstIpAddress = ips[0]
        }
      }

      await transactionConsent(async (client) => {
        await client.query(
          `UPDATE newsletter_consents
           SET is_active = false, updated_at = CURRENT_TIMESTAMP
           WHERE user_id = $1 AND consent_type = $2 AND is_active = true`,
          [userId, type]
        )

        try {
          await client.query(
            `INSERT INTO newsletter_consents(
              user_id, consent_type, consent_value, ip_address, user_agent, is_active, ip_address_full, consent_timestamp, created_at, updated_at
            )
            VALUES ($1, $2, $3, $4, $5, true, $6, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ON CONFLICT (user_id, consent_type) WHERE is_active = true
            DO UPDATE SET
              consent_value = $3,
              ip_address = $4,
              user_agent = $5,
              ip_address_full = $6,
              updated_at = CURRENT_TIMESTAMP`,
            [userId, type, value, firstIpAddress, metadata?.user_agent || null, fullIpAddressChain]
          )
        } catch (insertError: any) {
          if (insertError.code === '23505' && insertError.message?.includes('unique_active_consent')) {
            logger.logWarning(
              'Repository',
              'pgConsentRepository.updateConsent',
              `Ignoring duplicate consent update: ${insertError.message}`,
              userId
            )
            return
          }
          throw insertError
        }

        await client.query(
          `UPDATE "next-auth".users
           SET have_seen_newsletter = true, updated_at = CURRENT_TIMESTAMP
           WHERE id = $1 AND have_seen_newsletter = false`,
          [userId]
        )
      })
    } catch (error) {
      if (!((error as any)?.code === '23505')) {
        logger.logError('Repository', 'pgConsentRepository.updateConsent', 'Error updating consent', userId, { type, error })
        throw error
      }
    }
  },

  async insertConsent(consent: NewsletterConsent): Promise<NewsletterConsent> {
    try {
      const result = await queryConsent<NewsletterConsent>(
        `INSERT INTO newsletter_consents(
          user_id, consent_type, consent_value, ip_address, user_agent, is_active, ip_address_full, consent_timestamp, created_at, updated_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        RETURNING *`,
        [
          consent.user_id,
          consent.consent_type,
          consent.consent_value,
          consent.ip_address || null,
          consent.user_agent || null,
          consent.is_active !== false,
          consent.ip_address_full || null,
          consent.consent_timestamp || new Date().toISOString(),
        ]
      )

      if (!result.rows[0]) {
        throw new Error('Failed to insert consent')
      }

      return result.rows[0]
    } catch (error) {
      logger.logError('Repository', 'pgConsentRepository.insertConsent', 'Error inserting consent', consent.user_id, { consent, error })
      throw error
    }
  },

  async upsertConsent(consent: NewsletterConsent): Promise<NewsletterConsent> {
    try {
      const result = await queryConsent<NewsletterConsent>(
        `INSERT INTO newsletter_consents(
          user_id, consent_type, consent_value, ip_address, user_agent, is_active, ip_address_full, consent_timestamp, created_at, updated_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        ON CONFLICT (user_id, consent_type) WHERE is_active = true
        DO UPDATE SET
          consent_value = $3,
          ip_address = $4,
          user_agent = $5,
          ip_address_full = $7,
          updated_at = CURRENT_TIMESTAMP
        RETURNING *`,
        [
          consent.user_id,
          consent.consent_type,
          consent.consent_value,
          consent.ip_address || null,
          consent.user_agent || null,
          consent.is_active !== false,
          consent.ip_address_full || null,
          consent.consent_timestamp || new Date().toISOString(),
        ]
      )

      if (!result.rows[0]) {
        throw new Error('Failed to upsert consent')
      }

      return result.rows[0]
    } catch (error) {
      logger.logError('Repository', 'pgConsentRepository.upsertConsent', 'Error upserting consent', consent.user_id, { consent, error })
      throw error
    }
  },

  async getConsent(userId: string, consentType: string): Promise<NewsletterConsent | null> {
    try {
      const result = await queryConsent<NewsletterConsent>(
        `SELECT * FROM newsletter_consents 
         WHERE user_id = $1 AND consent_type = $2 AND is_active = true`,
        [userId, consentType]
      )

      return result.rows[0] || null
    } catch (error) {
      logger.logError('Repository', 'pgConsentRepository.getConsent', 'Error fetching consent', userId, { consentType, error })
      throw error
    }
  },

  async deleteUserConsents(userId: string): Promise<void> {
    try {
      await queryConsent(`DELETE FROM newsletter_consents WHERE user_id = $1`, [userId])
    } catch (error) {
      logger.logError('Repository', 'pgConsentRepository.deleteUserConsents', 'Error deleting user consents', userId, { error })
      throw error
    }
  },

  async deleteConsent(userId: string, consentType: string): Promise<void> {
    try {
      await queryConsent(
        `DELETE FROM newsletter_consents WHERE user_id = $1 AND consent_type = $2`,
        [userId, consentType]
      )
    } catch (error) {
      logger.logError('Repository', 'pgConsentRepository.deleteConsent', 'Error deleting consent', userId, { consentType, error })
      throw error
    }
  },
}
