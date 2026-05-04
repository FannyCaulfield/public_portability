import { queryInstances } from '../../database'
import logger from '../../log_utils'

export interface DBFediverseInstance {
  id: string
  host: string
  instance_type: string
  client_id: string
  client_secret: string | null
  authorization_endpoint: string | null
  token_endpoint: string | null
  userinfo_endpoint: string | null
  scopes: string[] | null
  created_at: Date
  updated_at: Date
}

interface CreateFediverseInstanceInput {
  host: string
  instance_type: string
  client_id: string
  client_secret?: string | null
  authorization_endpoint?: string | null
  token_endpoint?: string | null
  userinfo_endpoint?: string | null
  scopes?: string[] | null
}

export const pgFediverseInstanceRepository = {
  async getInstance(host: string): Promise<DBFediverseInstance | null> {
    try {
      const result = await queryInstances<DBFediverseInstance>(
        'SELECT * FROM instances.fediverse_instances WHERE host = $1',
        [host.toLowerCase()]
      )

      return result.rows[0] || null
    } catch (error) {
      logger.logError('Repository', 'pgFediverseInstanceRepository.getInstance', 'Error fetching fediverse instance', undefined, {
        host,
        error,
      })
      throw error
    }
  },

  async upsertInstance(input: CreateFediverseInstanceInput): Promise<DBFediverseInstance> {
    try {
      const result = await queryInstances<DBFediverseInstance>(
        `INSERT INTO instances.fediverse_instances (
           host,
           instance_type,
           client_id,
           client_secret,
           authorization_endpoint,
           token_endpoint,
           userinfo_endpoint,
           scopes,
           updated_at
         )
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
         ON CONFLICT (host)
         DO UPDATE SET
           instance_type = EXCLUDED.instance_type,
           client_id = EXCLUDED.client_id,
           client_secret = EXCLUDED.client_secret,
           authorization_endpoint = EXCLUDED.authorization_endpoint,
           token_endpoint = EXCLUDED.token_endpoint,
           userinfo_endpoint = EXCLUDED.userinfo_endpoint,
           scopes = EXCLUDED.scopes,
           updated_at = NOW()
         RETURNING *`,
        [
          input.host.toLowerCase(),
          input.instance_type,
          input.client_id,
          input.client_secret ?? null,
          input.authorization_endpoint ?? null,
          input.token_endpoint ?? null,
          input.userinfo_endpoint ?? null,
          input.scopes ?? null,
        ]
      )

      if (!result.rows[0]) {
        throw new Error('Failed to upsert fediverse instance')
      }

      return result.rows[0]
    } catch (error) {
      logger.logError('Repository', 'pgFediverseInstanceRepository.upsertInstance', 'Error upserting fediverse instance', undefined, {
        input,
        error,
      })
      throw error
    }
  },
}
