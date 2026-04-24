import logger from '../../log_utils';
import { redis } from '../../redis';

const NETWORK_WORKER_PENDING_KEY = 'network_worker:jobs:pending';

export interface NetworkSyncJobsPayload {
  job_id: number | string;
  user_id: string;
  provider: 'bluesky' | 'mastodon';
  scope: 'followers' | 'followings' | 'full_sync';
  status: 'pending' | 'retrying' | string;
  dedupe_key?: string;
  timestamp?: number;
}

export async function handleNetworkSyncJobs(payload: NetworkSyncJobsPayload): Promise<void> {
  try {
    const { job_id, user_id, provider, scope, status } = payload;

    if (!job_id || !user_id || !provider || !scope || !status) {
      logger.logError('PgNotify', 'Invalid network sync jobs payload', JSON.stringify(payload).substring(0, 200), 'system');
      return;
    }

    const enqueued = await redis.lpush(NETWORK_WORKER_PENDING_KEY, JSON.stringify(payload));

    if (enqueued <= 0) {
      logger.logWarning('PgNotify', 'Failed to enqueue network sync job in Redis', JSON.stringify(payload).substring(0, 200), 'system');
      return;
    }

    logger.logInfo('PgNotify', 'Network sync job enqueued', `job_id=${job_id}`, 'system');
  } catch (error) {
    logger.logError(
      'PgNotify',
      'Failed to enqueue network sync job',
      error instanceof Error ? error.message : String(error),
      'system'
    );
  }
}
