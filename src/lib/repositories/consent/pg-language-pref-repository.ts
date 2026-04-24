import { queryConsent } from '../../database'
import logger from '../../log_utils'

export interface LanguagePreference {
  id?: string
  user_id: string
  language: string
  created_at?: string
  updated_at?: string
}

export const pgLanguagePrefRepository = {
  async getUserLanguagePreference(userId: string): Promise<LanguagePreference | null> {
    try {
      const result = await queryConsent<LanguagePreference>(
        `SELECT * FROM language_pref WHERE user_id = $1`,
        [userId]
      )
      return result.rows[0] || null
    } catch (error) {
      logger.logWarning('Repository', 'pgLanguagePrefRepository.getUserLanguagePreference', 'Error fetching language preference', userId)
      return null
    }
  },

  async updateLanguagePreference(userId: string, language: string): Promise<void> {
    try {
      await queryConsent(
        `INSERT INTO language_pref (user_id, language, created_at, updated_at)
         VALUES ($1, $2, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
         ON CONFLICT (user_id)
         DO UPDATE SET language = $2, updated_at = CURRENT_TIMESTAMP`,
        [userId, language]
      )
    } catch (error) {
      logger.logError('Repository', 'pgLanguagePrefRepository.updateLanguagePreference', 'Error updating language preference', userId, { language, error })
      throw error
    }
  },

  async deleteLanguagePreference(userId: string): Promise<void> {
    try {
      await queryConsent(`DELETE FROM language_pref WHERE user_id = $1`, [userId])
    } catch (error) {
      logger.logError('Repository', 'pgLanguagePrefRepository.deleteLanguagePreference', 'Error deleting language preference', userId, { error })
      throw error
    }
  },
}
