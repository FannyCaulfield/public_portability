import { NextResponse } from 'next/server'
import { z } from 'zod'
import logger from '@/lib/log_utils'
import { withPublicValidation } from '@/lib/validation/middleware'
import { pgGraphCommunityLabelsRepository } from '@/lib/repositories/graph/pg-graph-community-labels-repository'

export const dynamic = 'force-dynamic'
export const revalidate = 60

const EmptySchema = z.object({}).strict()

async function getDeckCommunityLabelsHandler() {
  try {
    const labels = await pgGraphCommunityLabelsRepository.listForDeck()

    return NextResponse.json({
      success: true,
      count: labels.length,
      communities: labels,
    })
  } catch (error) {
    const err = error instanceof Error ? error : new Error(String(error))
    logger.logError('API', 'GET /api/graph/community_labels/deck', err, 'system', {
      context: 'Error fetching deck community labels',
    })
    return NextResponse.json({ error: 'Internal server error' }, { status: 500 })
  }
}

export const GET = withPublicValidation(EmptySchema, getDeckCommunityLabelsHandler, {
  applySecurityChecks: false,
  skipRateLimit: false,
})
