/**
 * Repository PostgreSQL pour les opérations sur graph_nodes
 * Gère la récupération des nœuds du graphe depuis la table graph_nodes_03_11_25
 */

import { queryGraph, queryPublic } from '../../database'
import logger from '../../log_utils'

const GRAPH_NODES_TABLE = 'graph.graph_nodes_03_11_25'

export interface GraphNodeRow {
  id: string
  label: string | null
  x: number
  y: number
  size: number | null
  color: string | null
  community: number | null
  degree: number
  tier: string | null
  graph_label: string | null
  node_type: string | null
  created_at: Date | null
  updated_at: Date | null
}

export interface GraphNodeMatch {
  twitter_id: string
  label: string | null
  x: number
  y: number
  community: number | null
  tier: string | null
  graph_label: string | null
  node_type: string | null
  bluesky_handle?: string | null
  mastodon_handle?: string | null
  has_follow_bluesky?: boolean
  has_follow_mastodon?: boolean
}

function coordHash(x: number, y: number): string {
  return `${x.toFixed(6)}_${y.toFixed(6)}`
}

function parseCoordHash(hash: string): { x: number; y: number } | null {
  const parts = hash.split('_')
  if (parts.length !== 2) return null
  const x = parseFloat(parts[0])
  const y = parseFloat(parts[1])
  if (isNaN(x) || isNaN(y)) return null
  return { x, y }
}

export const pgGraphNodesRepository = {
  async getNodesByCoordinates(coordinates: { x: number; y: number }[]): Promise<GraphNodeRow[]> {
    if (coordinates.length === 0) return []

    try {
      const tolerance = 0.0000005
      const conditions: string[] = []
      const values: any[] = []

      coordinates.forEach((coord, index) => {
        const xParam = index * 2 + 1
        const yParam = index * 2 + 2
        conditions.push(`(ABS(x - $${xParam}) < ${tolerance} AND ABS(y - $${yParam}) < ${tolerance})`)
        values.push(coord.x, coord.y)
      })

      const query = `
        SELECT id::text as id, label, x, y, size, color, community, degree, tier, graph_label, node_type, created_at, updated_at
        FROM ${GRAPH_NODES_TABLE}
        WHERE ${conditions.join(' OR ')}
      `

      const result = await queryGraph(query, values)

      logger.logDebug('Repository', 'pgGraphNodesRepository.getNodesByCoordinates', `Found ${result.rows.length} nodes for ${coordinates.length} coordinates`, 'system')

      return result.rows as GraphNodeRow[]
    } catch (error) {
      const errorString = error instanceof Error ? error.message : String(error)
      logger.logError('Repository', 'pgGraphNodesRepository.getNodesByCoordinates', errorString, 'system', { coordinatesCount: coordinates.length })
      throw error
    }
  },

  async getNodesByHashes(hashes: string[]): Promise<GraphNodeRow[]> {
    if (hashes.length === 0) return []

    const coordinates: { x: number; y: number }[] = []
    for (const hash of hashes) {
      const coord = parseCoordHash(hash)
      if (coord) {
        coordinates.push(coord)
      }
    }

    if (coordinates.length === 0) {
      logger.logWarning('Repository', 'pgGraphNodesRepository.getNodesByHashes', 'No valid coordinates parsed from hashes', 'system', { hashesCount: hashes.length })
      return []
    }

    return this.getNodesByCoordinates(coordinates)
  },

  async getNodeByTwitterId(twitterId: string): Promise<GraphNodeRow | null> {
    try {
      const result = await queryGraph(
        `SELECT id::text as id, label, x, y, size, color, community, degree, tier, graph_label, node_type, created_at, updated_at
         FROM ${GRAPH_NODES_TABLE}
         WHERE id = $1`,
        [twitterId]
      )

      if (result.rows.length === 0) {
        return null
      }

      return result.rows[0] as GraphNodeRow
    } catch (error) {
      const errorString = error instanceof Error ? error.message : String(error)
      logger.logError('Repository', 'pgGraphNodesRepository.getNodeByTwitterId', errorString, 'system', { twitterId })
      throw error
    }
  },

  async getNodesByTwitterIds(twitterIds: string[]): Promise<GraphNodeRow[]> {
    if (twitterIds.length === 0) return []

    try {
      const placeholders = twitterIds.map((_, i) => `$${i + 1}`).join(', ')

      const result = await queryGraph(
        `SELECT id::text as id, label, x, y, size, color, community, degree, tier, graph_label, node_type, created_at, updated_at
         FROM ${GRAPH_NODES_TABLE}
         WHERE id IN (${placeholders})`,
        twitterIds
      )

      return result.rows as GraphNodeRow[]
    } catch (error) {
      const errorString = error instanceof Error ? error.message : String(error)
      logger.logError('Repository', 'pgGraphNodesRepository.getNodesByTwitterIds', errorString, 'system', { twitterIdsCount: twitterIds.length })
      throw error
    }
  },

  async getHashesByTwitterIds(twitterIds: string[]): Promise<{ hash: string; hasBluesky?: boolean; hasMastodon?: boolean }[]> {
    if (twitterIds.length === 0) return []

    try {
      const placeholders = twitterIds.map((_, i) => `$${i + 1}`).join(', ')

      const result = await queryGraph(
        `SELECT x, y
         FROM ${GRAPH_NODES_TABLE}
         WHERE id IN (${placeholders})`,
        twitterIds
      )

      return result.rows.map((row: { x: number; y: number }) => ({
        hash: coordHash(row.x, row.y),
      }))
    } catch (error) {
      const errorString = error instanceof Error ? error.message : String(error)
      logger.logError('Repository', 'pgGraphNodesRepository.getHashesByTwitterIds', errorString, 'system', { twitterIdsCount: twitterIds.length })
      throw error
    }
  },

  async getPersonalLabelsWithCoords(): Promise<{ display_label: string; x: number; y: number; degree: number }[]> {
    try {
      const result = await queryGraph(
        `SELECT 
          pl.display_label,
          gn.x,
          gn.y,
          gn.degree
        FROM graph_personal_labels pl
        INNER JOIN ${GRAPH_NODES_TABLE} gn ON gn.id = pl.twitter_id`
      )

      return result.rows as { display_label: string; x: number; y: number; degree: number }[]
    } catch (error) {
      const errorString = error instanceof Error ? error.message : String(error)
      logger.logError('Repository', 'pgGraphNodesRepository.getPersonalLabelsWithCoords', errorString, 'system')
      throw error
    }
  },

  async getMemberNodes(limit: number = 1000): Promise<GraphNodeRow[]> {
    try {
      const result = await queryGraph(
        `SELECT id::text as id, label, x, y, size, color, community, degree, tier, graph_label, node_type, created_at, updated_at
         FROM ${GRAPH_NODES_TABLE}
         WHERE node_type = 'member'
         ORDER BY degree DESC
         LIMIT $1`,
        [limit]
      )

      return result.rows as GraphNodeRow[]
    } catch (error) {
      const errorString = error instanceof Error ? error.message : String(error)
      logger.logError('Repository', 'pgGraphNodesRepository.getMemberNodes', errorString, 'system', { limit })
      throw error
    }
  },

  async getVisibleLabelsForUser(
    userId: string,
    twitterId?: string | null
  ): Promise<{ node_id: string; display_label: string; consent_level: string; visibility_reason: string; x: number; y: number; follower_level: number }[]> {
    try {
      console.log(`[getVisibleLabelsForUser] Fetching all_consent labels for user ${userId}`)

      const result = await queryGraph(
        `SELECT 
          uwnc.twitter_id::text as node_id,
          COALESCE(
            uwnc.name,
            '@' || uwnc.twitter_username,
            '@' || uwnc.bluesky_username,
            '@' || uwnc.mastodon_username,
            'User ' || uwnc.twitter_id
          ) as display_label,
          uwnc.consent_level,
          'all_consent' as visibility_reason,
          gn.x,
          gn.y,
          1 as follower_level
        FROM consent.users_with_name_consent uwnc
        INNER JOIN graph.graph_nodes_03_11_25 gn ON gn.id = uwnc.twitter_id
        WHERE uwnc.consent_level = 'all_consent'`
      )

      console.log(`[getVisibleLabelsForUser] Found ${result.rows.length} labels`)
      if (result.rows.length > 0) {
        console.log(`[getVisibleLabelsForUser] First label:`, JSON.stringify(result.rows[0]))
      }

      return result.rows as { node_id: string; display_label: string; consent_level: string; visibility_reason: string; x: number; y: number; follower_level: number }[]
    } catch (error) {
      const errorString = error instanceof Error ? error.message : String(error)
      console.error(`[getVisibleLabelsForUser] Error:`, errorString)
      logger.logError('Repository', 'pgGraphNodesRepository.getVisibleLabelsForUser', errorString, 'system', { userId, twitterId })
      throw error
    }
  },

  async getPublicConsentLabels(): Promise<{ node_id: string; display_label: string; x: number; y: number }[]> {
    try {
      const countResult = await queryGraph(`SELECT COUNT(*) as cnt FROM consent.users_with_name_consent WHERE consent_level = 'all_consent'`)
      console.log(`[getPublicConsentLabels] Found ${countResult.rows[0]?.cnt || 0} users with all_consent`)

      const result = await queryGraph(
        `SELECT 
          uwnc.twitter_id::text as node_id,
          COALESCE(
            uwnc.name,
            '@' || uwnc.twitter_username,
            '@' || uwnc.bluesky_username,
            '@' || uwnc.mastodon_username,
            'User ' || uwnc.twitter_id
          ) as display_label,
          gn.x,
          gn.y
        FROM consent.users_with_name_consent uwnc
        INNER JOIN graph.graph_nodes_03_11_25 gn ON gn.id = uwnc.twitter_id
        WHERE uwnc.consent_level = 'all_consent'`
      )

      console.log(`[getPublicConsentLabels] After JOIN with graph_nodes: ${result.rows.length} labels found`)

      return result.rows as { node_id: string; display_label: string; x: number; y: number }[]
    } catch (error) {
      const errorString = error instanceof Error ? error.message : String(error)
      logger.logError('Repository', 'pgGraphNodesRepository.getPublicConsentLabels', errorString, 'system')
      throw error
    }
  },

  async getVisibleDeckLabelsForUser(
    userId: string,
    twitterId?: string | null
  ): Promise<{ node_id: string; display_label: string; consent_id: string | null; consent_level: string | null; visibility_reason: string | null; x: number; y: number; follower_level: number | null; is_public_account: boolean | null }[]> {
    try {
      console.log(`[getVisibleDeckLabelsForUser] Fetching deck labels for user ${userId}`)

      const result = await queryGraph(
        `SELECT
          uwnc.twitter_id::text as node_id,
          COALESCE(
            uwnc.name,
            '@' || uwnc.twitter_username,
            '@' || uwnc.bluesky_username,
            '@' || uwnc.mastodon_username,
            'User ' || uwnc.twitter_id
          ) as display_label,
          uwnc.consent_id::text as consent_id,
          uwnc.consent_level,
          'all_consent' as visibility_reason,
          gn.x,
          gn.y,
          1 as follower_level,
          uwnc.is_public_account
        FROM consent.users_with_name_consent uwnc
        INNER JOIN graph.graph_nodes_03_11_25 gn ON gn.id = uwnc.twitter_id
        WHERE uwnc.consent_level = 'all_consent'`
      )

      console.log(`[getVisibleDeckLabelsForUser] Found ${result.rows.length} deck labels`)

      return result.rows as { node_id: string; display_label: string; consent_id: string | null; consent_level: string | null; visibility_reason: string | null; x: number; y: number; follower_level: number | null; is_public_account: boolean | null }[]
    } catch (error) {
      const errorString = error instanceof Error ? error.message : String(error)
      logger.logError('Repository', 'pgGraphNodesRepository.getVisibleDeckLabelsForUser', errorString, 'system', { userId, twitterId })
      throw error
    }
  },

  async getPublicDeckConsentLabels(): Promise<{ node_id: string; display_label: string; consent_id: string | null; consent_level: string | null; visibility_reason: string | null; x: number; y: number; follower_level: number | null; is_public_account: boolean | null }[]> {
    try {
      const result = await queryGraph(
        `SELECT
          uwnc.twitter_id::text as node_id,
          COALESCE(
            uwnc.name,
            '@' || uwnc.twitter_username,
            '@' || uwnc.bluesky_username,
            '@' || uwnc.mastodon_username,
            'User ' || uwnc.twitter_id
          ) as display_label,
          uwnc.consent_id::text as consent_id,
          uwnc.consent_level,
          'all_consent' as visibility_reason,
          gn.x,
          gn.y,
          1 as follower_level,
          uwnc.is_public_account
        FROM consent.users_with_name_consent uwnc
        INNER JOIN ${GRAPH_NODES_TABLE} gn ON gn.id = uwnc.twitter_id
        WHERE uwnc.consent_level = 'all_consent'`
      )

      console.log(`[getPublicDeckConsentLabels] Found ${result.rows.length} deck labels`)

      return result.rows as { node_id: string; display_label: string; consent_id: string | null; consent_level: string | null; visibility_reason: string | null; x: number; y: number; follower_level: number | null; is_public_account: boolean | null }[]
    } catch (error) {
      const errorString = error instanceof Error ? error.message : String(error)
      logger.logError('Repository', 'pgGraphNodesRepository.getPublicDeckConsentLabels', errorString, 'system')
      throw error
    }
  },

  async updateNameConsent(
    userId: string,
    consentValue: 'no_consent' | 'only_to_followers_of_followers' | 'all_consent',
    metadata?: { ip_address?: string; user_agent?: string }
  ): Promise<{ success: boolean; consent_level: string }> {
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

      await queryGraph<{ id: string }>(
        `INSERT INTO consent_names(
          user_id, consent_value, ip_address, user_agent, ip_address_full, is_active, consent_timestamp, created_at, updated_at
        )
        VALUES ($1, $2, $3, $4, $5, true, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        ON CONFLICT (user_id) WHERE is_active = true
        DO UPDATE SET
          consent_value = EXCLUDED.consent_value,
          ip_address = EXCLUDED.ip_address,
          user_agent = EXCLUDED.user_agent,
          ip_address_full = EXCLUDED.ip_address_full,
          consent_timestamp = CURRENT_TIMESTAMP,
          updated_at = CURRENT_TIMESTAMP
        RETURNING id`,
        [userId, consentValue, firstIpAddress, metadata?.user_agent || null, fullIpAddressChain]
      )

      logger.logInfo('Repository', 'pgGraphNodesRepository.updateNameConsent', `Updated name consent to ${consentValue}`, userId)

      return { success: true, consent_level: consentValue }
    } catch (error) {
      const errorString = error instanceof Error ? error.message : String(error)
      logger.logError('Repository', 'pgGraphNodesRepository.updateNameConsent', errorString, userId, { consentValue })
      throw error
    }
  },

  async getMemberNodesByHashesWithConsent(hashes: string[]): Promise<GraphNodeRow[]> {
    if (hashes.length === 0) return []

    const coordinates: { x: number; y: number }[] = []
    for (const hash of hashes) {
      const coord = parseCoordHash(hash)
      if (coord) {
        coordinates.push(coord)
      }
    }

    if (coordinates.length === 0) {
      logger.logWarning('Repository', 'pgGraphNodesRepository.getMemberNodesByHashesWithConsent', 'No valid coordinates parsed from hashes', 'system', { hashesCount: hashes.length })
      return []
    }

    try {
      const tolerance = 0.0000005
      const conditions: string[] = []
      const values: any[] = []

      coordinates.forEach((coord, index) => {
        const xParam = index * 2 + 1
        const yParam = index * 2 + 2
        conditions.push(`(ABS(g.x - $${xParam}) < ${tolerance} AND ABS(g.y - $${yParam}) < ${tolerance})`)
        values.push(coord.x, coord.y)
      })

      const query = `
        SELECT g.id::text as id, g.label, g.x, g.y, g.size, g.color, g.community, g.degree, g.tier, g.graph_label, g.node_type, g.created_at, g.updated_at
        FROM ${GRAPH_NODES_TABLE} g
        INNER JOIN consent.users_with_name_consent u ON g.id = u.twitter_id
        WHERE g.node_type = 'member'
          AND (${conditions.join(' OR ')})
      `

      const result = await queryGraph(query, values)

      logger.logDebug('Repository', 'pgGraphNodesRepository.getMemberNodesByHashesWithConsent', `Found ${result.rows.length} member nodes with consent for ${coordinates.length} coordinates`, 'system')

      return result.rows as GraphNodeRow[]
    } catch (error) {
      const errorString = error instanceof Error ? error.message : String(error)
      logger.logError('Repository', 'pgGraphNodesRepository.getMemberNodesByHashesWithConsent', errorString, 'system', { hashesCount: hashes.length })
      throw error
    }
  },

  async getMemberNodesByCommunityWithConsent(communityId: number): Promise<GraphNodeRow[]> {
    try {
      const query = `
        SELECT g.id::text as id, g.label, g.x, g.y, g.size, g.color, g.community, g.degree, g.tier, g.graph_label, g.node_type, g.created_at, g.updated_at
        FROM ${GRAPH_NODES_TABLE} g
        INNER JOIN consent.users_with_name_consent u ON g.id = u.twitter_id
        WHERE g.node_type = 'member'
          AND g.community = $1
      `

      const result = await queryGraph(query, [communityId])

      logger.logDebug('Repository', 'pgGraphNodesRepository.getMemberNodesByCommunityWithConsent', `Found ${result.rows.length} member nodes with consent for community ${communityId}`, 'system')

      return result.rows as GraphNodeRow[]
    } catch (error) {
      const errorString = error instanceof Error ? error.message : String(error)
      logger.logError('Repository', 'pgGraphNodesRepository.getMemberNodesByCommunityWithConsent', errorString, 'system', { communityId })
      throw error
    }
  },

  async getNameConsent(userId: string): Promise<string | null> {
    try {
      const result = await queryGraph<{ consent_value: string }>(
        `SELECT consent_value FROM consent_names WHERE user_id = $1 AND is_active = true`,
        [userId]
      )
      return result.rows[0]?.consent_value || null
    } catch (error) {
      const errorString = error instanceof Error ? error.message : String(error)
      logger.logError('Repository', 'pgGraphNodesRepository.getNameConsent', errorString, userId)
      throw error
    }
  },

  async searchByDisplayLabel(searchQuery: string): Promise<{ twitter_id: string; display_label: string; description: string | null; x: number; y: number; community: number | null } | null> {
    try {
      const result = await queryGraph<{
        twitter_id: string
        display_label: string
        description: string | null
        x: number
        y: number
        community: number | null
      }>(
        `WITH labeled AS (
          SELECT
            uwnc.twitter_id::text as twitter_id,
            COALESCE(
              uwnc.name,
              '@' || uwnc.twitter_username,
              '@' || uwnc.bluesky_username,
              '@' || uwnc.mastodon_username,
              'User ' || uwnc.twitter_id
            ) as display_label,
            CONCAT_WS(' ',
              uwnc.name,
              uwnc.twitter_username,
              uwnc.bluesky_username,
              uwnc.mastodon_username,
              pa.name,
              pa.twitter_username,
              pa.bluesky_username,
              pa.mastodon_username
            ) as searchable_text,
            CASE
              WHEN uwnc.is_public_account = true THEN pa.raw_description
              ELSE NULL
            END as description,
            gn.x,
            gn.y,
            gn.community
          FROM consent.users_with_name_consent uwnc
          LEFT JOIN public.public_accounts pa
            ON pa.twitter_id = uwnc.twitter_id
            AND uwnc.is_public_account = true
          INNER JOIN graph.graph_nodes_03_11_25 gn ON gn.id = uwnc.twitter_id
          WHERE uwnc.consent_level = 'all_consent'
        )
        SELECT twitter_id, display_label, description, x, y, community
        FROM labeled
        WHERE searchable_text ILIKE $1
        ORDER BY
          CASE WHEN LOWER(display_label) = LOWER($2) THEN 0 ELSE 1 END,
          display_label
        LIMIT 1`,
        [`%${searchQuery}%`, searchQuery]
      )

      if (result.rows.length === 0) {
        return null
      }

      logger.logDebug('Repository', 'pgGraphNodesRepository.searchByDisplayLabel', `Found user: ${result.rows[0].display_label}`, 'system')

      return result.rows[0]
    } catch (error) {
      const errorString = error instanceof Error ? error.message : String(error)
      logger.logError('Repository', 'pgGraphNodesRepository.searchByDisplayLabel', errorString, 'system', { searchQuery })
      throw error
    }
  },

  async searchByDisplayLabelMultiple(searchQuery: string, limit: number = 10): Promise<{ twitter_id: string; display_label: string; description: string | null; x: number; y: number; community: number | null; bluesky_handle: string | null; mastodon_handle: string | null }[]> {
    try {
      const result = await queryPublic<{
        twitter_id: string
        display_label: string
        description: string | null
        x: number
        y: number
        community: number | null
        bluesky_handle: string | null
        mastodon_handle: string | null
      }>(
        `WITH labeled AS (
          SELECT
            uwnc.twitter_id::text as twitter_id,
            COALESCE(
              uwnc.name,
              '@' || uwnc.twitter_username,
              '@' || uwnc.bluesky_username,
              '@' || uwnc.mastodon_username,
              'User ' || uwnc.twitter_id
            ) as display_label,
            CONCAT_WS(' ',
              uwnc.name,
              uwnc.twitter_username,
              uwnc.bluesky_username,
              uwnc.mastodon_username,
              pa.name,
              pa.twitter_username,
              pa.bluesky_username,
              pa.mastodon_username
            ) as searchable_text,
            CASE
              WHEN uwnc.is_public_account = true THEN pa.raw_description
              ELSE NULL
            END as description,
            gn.x,
            gn.y,
            gn.community,
            COALESCE(uwnc.bluesky_username, pa.bluesky_username) as bluesky_handle,
            CASE
              WHEN uwnc.mastodon_username IS NOT NULL AND uwnc.mastodon_instance IS NOT NULL
              THEN uwnc.mastodon_username || '@' || uwnc.mastodon_instance
              WHEN pa.mastodon_username IS NOT NULL AND pa.mastodon_instance IS NOT NULL
              THEN pa.mastodon_username || '@' || pa.mastodon_instance
              ELSE NULL
            END as mastodon_handle
          FROM consent.users_with_name_consent uwnc
          LEFT JOIN public.public_accounts pa
            ON pa.twitter_id = uwnc.twitter_id
            AND uwnc.is_public_account = true
          INNER JOIN graph.graph_nodes_03_11_25 gn ON gn.id = uwnc.twitter_id
          WHERE uwnc.consent_level = 'all_consent'
        )
        SELECT twitter_id, display_label, description, x, y, community, bluesky_handle, mastodon_handle
        FROM labeled
        WHERE searchable_text ILIKE $1
        ORDER BY
          CASE WHEN LOWER(display_label) = LOWER($2) THEN 0 ELSE 1 END,
          LENGTH(display_label),
          display_label
        LIMIT $3`,
        [`%${searchQuery}%`, searchQuery, limit]
      )

      console.log(`!!!!! Found ${result.rows.length} users matching "${searchQuery}"`)

      return result.rows
    } catch (error) {
      const errorString = error instanceof Error ? error.message : String(error)
      logger.logError('Repository', 'pgGraphNodesRepository.searchByDisplayLabelMultiple', errorString, 'system', { searchQuery, limit })
      throw error
    }
  },

  async getNodeCoordHashByTwitterId(twitterId: string): Promise<{ coord_hash: string; node_type: string; x: number; y: number } | null> {
    try {
      const result = await queryGraph<{ x: number; y: number; node_type: string }>(
        `SELECT x, y, node_type FROM ${GRAPH_NODES_TABLE} WHERE id = $1`,
        [twitterId]
      )

      if (result.rows.length === 0) {
        return null
      }

      const row = result.rows[0]
      return {
        coord_hash: coordHash(row.x, row.y),
        node_type: row.node_type || 'generic',
        x: row.x,
        y: row.y,
      }
    } catch (error) {
      const errorString = error instanceof Error ? error.message : String(error)
      logger.logError('Repository', 'pgGraphNodesRepository.getNodeCoordHashByTwitterId', errorString, 'system', { twitterId })
      throw error
    }
  },
}
