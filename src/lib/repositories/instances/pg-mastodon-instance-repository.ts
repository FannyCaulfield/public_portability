import { queryInstances } from '../../database'
import type { DBMastodonInstance } from '../../types/database'
import logger from '../../log_utils'

export const pgMastodonInstanceRepository = {
  async getInstance(instance: string): Promise<DBMastodonInstance | null> {
    try {
      const result = await queryInstances<DBMastodonInstance>(
        'SELECT * FROM instances.mastodon_instances WHERE instance = $1',
        [instance.toLowerCase()]
      )
      return result.rows[0] || null
    } catch (error) {
      logger.logError('Repository', 'pgMastodonInstanceRepository.getInstance', 'Error fetching instance', undefined, {
        instance,
        error
      })
      throw error
    }
  },

  async getAllInstances(): Promise<DBMastodonInstance[]> {
    try {
      const result = await queryInstances<DBMastodonInstance>(
        'SELECT * FROM instances.mastodon_instances ORDER BY instance ASC'
      )
      return result.rows
    } catch (error) {
      logger.logError('Repository', 'pgMastodonInstanceRepository.getAllInstances', 'Error fetching all instances', undefined, { error })
      throw error
    }
  },

  async createInstance(instanceData: {
    instance: string
    client_id: string
    client_secret: string
  }): Promise<DBMastodonInstance> {
    try {
      const result = await queryInstances<DBMastodonInstance>(
        `INSERT INTO instances.mastodon_instances (instance, client_id, client_secret)
         VALUES ($1, $2, $3)
         RETURNING *`,
        [instanceData.instance.toLowerCase(), instanceData.client_id, instanceData.client_secret]
      )

      if (!result.rows[0]) {
        throw new Error('Failed to create instance')
      }

      return result.rows[0]
    } catch (error) {
      logger.logError('Repository', 'pgMastodonInstanceRepository.createInstance', 'Error creating instance', undefined, {
        instanceData,
        error
      })
      throw error
    }
  },

  async updateInstance(
    instance: string,
    updates: {
      client_id?: string
      client_secret?: string
    }
  ): Promise<DBMastodonInstance> {
    try {
      const fields = Object.keys(updates)
      const setClauses = fields.map((field, i) => `${field} = $${i + 2}`).join(', ')
      const values = [instance.toLowerCase(), ...fields.map(field => updates[field as keyof typeof updates])]

      const sql = `
        UPDATE instances.mastodon_instances
        SET ${setClauses}
        WHERE instance = $1
        RETURNING *
      `

      const result = await queryInstances<DBMastodonInstance>(sql, values)

      if (!result.rows[0]) {
        throw new Error('Instance not found')
      }

      return result.rows[0]
    } catch (error) {
      logger.logError('Repository', 'pgMastodonInstanceRepository.updateInstance', 'Error updating instance', undefined, {
        instance,
        updates,
        error
      })
      throw error
    }
  },

  async deleteInstance(instance: string): Promise<void> {
    try {
      await queryInstances('DELETE FROM instances.mastodon_instances WHERE instance = $1', [instance.toLowerCase()])
    } catch (error) {
      logger.logError('Repository', 'pgMastodonInstanceRepository.deleteInstance', 'Error deleting instance', undefined, {
        instance,
        error
      })
      throw error
    }
  },

  async getOrCreateInstance(
    instance: string,
    creator: () => Promise<{ client_id: string; client_secret: string }>
  ): Promise<DBMastodonInstance> {
    try {
      const existing = await this.getInstance(instance)
      if (existing) {
        return existing
      }

      const credentials = await creator()
      return await this.createInstance({
        instance,
        client_id: credentials.client_id,
        client_secret: credentials.client_secret,
      })
    } catch (error) {
      logger.logError('Repository', 'pgMastodonInstanceRepository.getOrCreateInstance', 'Error in getOrCreate', undefined, {
        instance,
        error
      })
      throw error
    }
  },
}
