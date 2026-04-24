import { queryPublic } from '@/lib/database'
import logger from '@/lib/log_utils'

const GRAPH_NODES_TABLE = 'graph_nodes_03_11_25'
const COMMUNITY_LABELS_TABLE = 'graph_community_labels_admin'
const GRAPH_NAMESPACE = 'reconnect_deck'

export interface GraphCommunityLabelRow {
  community_id: number
  display_name: string
  description: string | null
  node_count: number
  x: number
  y: number
  updated_at: string | null
  updated_by: string | null
}

async function ensureTableExists() {
  await queryPublic(`
    CREATE TABLE IF NOT EXISTS ${COMMUNITY_LABELS_TABLE} (
      id BIGSERIAL PRIMARY KEY,
      graph_namespace TEXT NOT NULL DEFAULT '${GRAPH_NAMESPACE}',
      community_id INTEGER NOT NULL,
      display_name TEXT NOT NULL,
      description TEXT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_by TEXT NULL,
      UNIQUE (graph_namespace, community_id)
    )
  `)
}

export const pgGraphCommunityLabelsRepository = {
  async listForDeck(): Promise<GraphCommunityLabelRow[]> {
    await ensureTableExists()

    try {
      const result = await queryPublic<GraphCommunityLabelRow>(`
        WITH named_communities AS (
          SELECT community_id, display_name, description, updated_at, updated_by
          FROM ${COMMUNITY_LABELS_TABLE}
          WHERE graph_namespace = $1
        ),
        community_centroids AS (
          SELECT
            community,
            AVG(x)::float8 AS x,
            AVG(y)::float8 AS y,
            COUNT(*)::int AS node_count
          FROM ${GRAPH_NODES_TABLE}
          WHERE community IS NOT NULL
          GROUP BY community
        )
        SELECT
          nc.community_id,
          nc.display_name,
          nc.description,
          cc.node_count,
          cc.x,
          cc.y,
          nc.updated_at::text,
          nc.updated_by
        FROM named_communities nc
        INNER JOIN community_centroids cc ON cc.community = nc.community_id
        ORDER BY cc.node_count DESC, nc.community_id ASC
      `, [GRAPH_NAMESPACE])

      return result.rows
    } catch (error) {
      const err = error instanceof Error ? error : new Error(String(error))
      logger.logError('Repository', 'pgGraphCommunityLabelsRepository.listForDeck', err, 'system')
      throw err
    }
  },

  async upsertForDeck(params: {
    communityId: number
    displayName: string
    description?: string | null
    updatedBy?: string | null
  }): Promise<void> {
    await ensureTableExists()

    try {
      await queryPublic(
        `INSERT INTO ${COMMUNITY_LABELS_TABLE} (
          graph_namespace,
          community_id,
          display_name,
          description,
          updated_at,
          updated_by
        ) VALUES ($1, $2, $3, $4, NOW(), $5)
        ON CONFLICT (graph_namespace, community_id)
        DO UPDATE SET
          display_name = EXCLUDED.display_name,
          description = EXCLUDED.description,
          updated_at = NOW(),
          updated_by = EXCLUDED.updated_by`,
        [GRAPH_NAMESPACE, params.communityId, params.displayName, params.description ?? null, params.updatedBy ?? null]
      )
    } catch (error) {
      const err = error instanceof Error ? error : new Error(String(error))
      logger.logError('Repository', 'pgGraphCommunityLabelsRepository.upsertForDeck', err, 'system', params)
      throw err
    }
  },
}
