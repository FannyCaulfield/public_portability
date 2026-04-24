import { queryConsent } from '../../database'
import { pgUserRepository } from '../auth/pg-user-repository'
import logger from '../../log_utils'

export interface NewsletterListing {
  id?: string
  user_id: string
  email: string
  created_at?: string
  updated_at?: string
}

export const pgNewsletterListingRepository = {
  async insertNewsletterListing(userId: string): Promise<void> {
    try {
      const user = await pgUserRepository.getUser(userId)
      if (!user || !user.email) {
        throw new Error('User not found or email missing')
      }

      await queryConsent(
        `INSERT INTO newsletter_listing (user_id, email, created_at, updated_at)
         VALUES ($1, $2, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`,
        [userId, user.email]
      )
    } catch (error: any) {
      if (error.code !== '23505') {
        logger.logError('Repository', 'pgNewsletterListingRepository.insertNewsletterListing', 'Error inserting newsletter listing', userId, { error })
        throw error
      }
    }
  },

  async deleteNewsletterListing(userId: string): Promise<void> {
    try {
      await queryConsent(`DELETE FROM newsletter_listing WHERE user_id = $1`, [userId])
    } catch (error) {
      logger.logError('Repository', 'pgNewsletterListingRepository.deleteNewsletterListing', 'Error deleting newsletter listing', userId, { error })
      throw error
    }
  },

  async getNewsletterListing(userId: string): Promise<NewsletterListing | null> {
    try {
      const result = await queryConsent<NewsletterListing>(
        `SELECT * FROM newsletter_listing WHERE user_id = $1`,
        [userId]
      )
      return result.rows[0] || null
    } catch (error) {
      logger.logError('Repository', 'pgNewsletterListingRepository.getNewsletterListing', 'Error fetching newsletter listing', userId, { error })
      throw error
    }
  },

  async isInNewsletterListing(userId: string): Promise<boolean> {
    try {
      const listing = await this.getNewsletterListing(userId)
      return listing !== null
    } catch (error) {
      logger.logError('Repository', 'pgNewsletterListingRepository.isInNewsletterListing', 'Error checking newsletter listing', userId, { error })
      return false
    }
  },
}
