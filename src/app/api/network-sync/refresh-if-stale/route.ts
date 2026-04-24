import { NextRequest, NextResponse } from 'next/server'
import { z } from 'zod'

import logger from '@/lib/log_utils'
import { queryJobs, queryNextAuth } from '@/lib/database'
import { withValidation } from '@/lib/validation/middleware'
import { pgUserRepository } from '@/lib/repositories/auth/pg-user-repository'

const EmptySchema = z.object({})
const MASTODON_REFRESH_STALE_HOURS = 24

type NetworkSyncJobRow = {
  id: string
  status: 'pending' | 'running' | 'retrying' | 'success' | 'partial' | 'failed' | 'needs_reauth' | 'cancelled'
  finished_at: string | null
}

async function refreshIfStaleHandler(_request: NextRequest, _data: z.infer<typeof EmptySchema>, session: any) {
  try {
    const userId = session?.user?.id
    if (!userId) {
      return NextResponse.json({ error: 'Unauthorized' }, { status: 401 })
    }

    const user = await pgUserRepository.getUser(userId)
    if (!user) {
      return NextResponse.json({ error: 'User not found' }, { status: 404 })
    }

    const mastodonSocialAccountResult = await queryNextAuth<{ provider_account_id: string | null; username: string | null; instance: string | null }>(
      `SELECT provider_account_id, username, instance
       FROM "next-auth".social_accounts
       WHERE user_id = $1::uuid
         AND provider = 'mastodon'
       LIMIT 1`,
      [userId],
    )

    const mastodonSocialAccount = mastodonSocialAccountResult.rows[0] ?? null

    if (!mastodonSocialAccount?.provider_account_id || !mastodonSocialAccount.username || !mastodonSocialAccount.instance) {
      return NextResponse.json({ success: true, action: 'skipped', reason: 'mastodon_not_linked' })
    }

    const mastodonAccountResult = await queryNextAuth<{ user_id: string }>(
      `SELECT user_id
       FROM "next-auth".accounts
       WHERE user_id = $1::uuid
         AND provider = 'mastodon'
       LIMIT 1`,
      [userId],
    )

    if (!mastodonAccountResult.rows[0]) {
      return NextResponse.json({ success: true, action: 'skipped', reason: 'mastodon_account_missing' })
    }

    const inflightResult = await queryJobs<{ id: string }>(
      `SELECT id
       FROM jobs.network_sync_jobs
       WHERE user_id = $1::uuid
         AND provider = 'mastodon'
         AND status IN ('pending', 'running', 'retrying')
       ORDER BY created_at DESC
       LIMIT 1`,
      [userId],
    )

    if (inflightResult.rows[0]) {
      return NextResponse.json({
        success: true,
        action: 'skipped',
        reason: 'job_inflight',
        jobId: Number(inflightResult.rows[0].id),
      })
    }

    const latestResult = await queryJobs<NetworkSyncJobRow>(
      `SELECT id, status, finished_at
       FROM jobs.network_sync_jobs
       WHERE user_id = $1::uuid
         AND provider = 'mastodon'
       ORDER BY created_at DESC
       LIMIT 1`,
      [userId],
    )

    const latestJob = latestResult.rows[0] ?? null
    if (
      latestJob &&
      (latestJob.status === 'success' || latestJob.status === 'partial') &&
      latestJob.finished_at
    ) {
      const latestFinishedAt = new Date(latestJob.finished_at)
      const staleBefore = Date.now() - MASTODON_REFRESH_STALE_HOURS * 60 * 60 * 1000
      if (latestFinishedAt.getTime() > staleBefore) {
        return NextResponse.json({
          success: true,
          action: 'skipped',
          reason: 'fresh_enough',
          lastFinishedAt: latestJob.finished_at,
          staleAfterHours: MASTODON_REFRESH_STALE_HOURS,
        })
      }
    }

    const dedupeKey = `reconnect_return:${userId}:mastodon:full_sync`
    const insertResult = await queryJobs<{ id: string }>(
      `INSERT INTO jobs.network_sync_jobs (user_id, provider, scope, dedupe_key, status, triggered_by)
       VALUES ($1::uuid, 'mastodon', 'full_sync', $2::text, 'pending', 'reconnect_page_return')
       ON CONFLICT (dedupe_key)
       WHERE status IN ('pending', 'running', 'retrying')
       DO NOTHING
       RETURNING id`,
      [userId, dedupeKey],
    )

    if (!insertResult.rows[0]) {
      return NextResponse.json({ success: true, action: 'skipped', reason: 'deduped' })
    }

    logger.logInfo('API', 'POST /api/network-sync/refresh-if-stale', 'Queued stale Mastodon network sync from ReconnectPage', userId, {
      provider: 'mastodon',
      dedupeKey,
      jobId: Number(insertResult.rows[0].id),
      staleAfterHours: MASTODON_REFRESH_STALE_HOURS,
    })

    return NextResponse.json({
      success: true,
      action: 'queued',
      provider: 'mastodon',
      jobId: Number(insertResult.rows[0].id),
      staleAfterHours: MASTODON_REFRESH_STALE_HOURS,
    })
  } catch (error) {
    const err = error instanceof Error ? error : new Error(String(error))
    logger.logError('API', 'POST /api/network-sync/refresh-if-stale', err, session?.user?.id || 'unknown', {
      name: err.name,
      message: err.message,
    })
    return NextResponse.json({ error: 'Failed to refresh network sync state' }, { status: 500 })
  }
}

export const POST = withValidation(EmptySchema, refreshIfStaleHandler, {
  requireAuth: true,
  applySecurityChecks: false,
  skipRateLimit: false,
})
