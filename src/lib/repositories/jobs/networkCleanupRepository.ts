import { queryJobs, queryNetwork } from '@/lib/database'

export type NetworkCleanupProvider = 'bluesky' | 'mastodon'

export async function cancelQueuedNetworkSyncJobs(userId: string, provider: NetworkCleanupProvider): Promise<number> {
  const result = await queryJobs(
    `UPDATE jobs.network_sync_jobs
     SET status = 'cancelled',
         updated_at = now(),
         finished_at = COALESCE(finished_at, now()),
         next_retry_at = NULL,
         last_error = COALESCE(last_error, 'cancelled: provider unlinked')
     WHERE user_id = $1::uuid
       AND provider = $2::text
       AND status IN ('pending', 'retrying')`,
    [userId, provider],
  )

  return result.rowCount ?? 0
}

export async function deleteNetworkEdgesByUserId(userId: string, provider: NetworkCleanupProvider): Promise<number> {
  const result = await queryNetwork(
    provider === 'bluesky'
      ? `WITH deleted_followers AS (
       DELETE FROM network.bluesky_followers_edges
       WHERE user_id = $1::uuid
       RETURNING 1
     ), deleted_followings AS (
       DELETE FROM network.bluesky_followings_edges
       WHERE user_id = $1::uuid
       RETURNING 1
     )
     SELECT
       (SELECT COUNT(*) FROM deleted_followers) + (SELECT COUNT(*) FROM deleted_followings) AS deleted_count`
      : `WITH deleted_followers AS (
       DELETE FROM network.mastodon_followers_edges
       WHERE user_id = $1::uuid
       RETURNING 1
     ), deleted_followings AS (
       DELETE FROM network.mastodon_followings_edges
       WHERE user_id = $1::uuid
       RETURNING 1
     )
     SELECT
       (SELECT COUNT(*) FROM deleted_followers) + (SELECT COUNT(*) FROM deleted_followings) AS deleted_count`,
    [userId],
  )

  return Number(result.rows[0]?.deleted_count ?? 0)
}

export async function deleteAllUserNetworkProjectionData(userId: string): Promise<{
  blueskyDeleted: number
  mastodonDeleted: number
  cancelledJobs: number
}> {
  const [blueskyDeleted, mastodonDeleted, cancelledBluesky, cancelledMastodon] = await Promise.all([
    deleteNetworkEdgesByUserId(userId, 'bluesky'),
    deleteNetworkEdgesByUserId(userId, 'mastodon'),
    cancelQueuedNetworkSyncJobs(userId, 'bluesky'),
    cancelQueuedNetworkSyncJobs(userId, 'mastodon'),
  ])

  return {
    blueskyDeleted,
    mastodonDeleted,
    cancelledJobs: cancelledBluesky + cancelledMastodon,
  }
}
