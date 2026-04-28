import { redis } from '../redis'
import logger from '../log_utils'
import { pgNewsletterListingRepository } from '../repositories/consent/pg-newsletter-listing-repository'
import { pgPythonTasksRepository } from '../repositories/jobs/pg-python-tasks-repository'

export type ConsentProcessingPayload = {
  user_id: string
  consent_type: 'bluesky_dm' | 'mastodon_dm' | 'email_newsletter' | 'oep_newsletter' | 'research_participation'
  consent_value: boolean
  old_consent_value?: boolean | null
  handle?: string | null
  metadata: {
    userAgent?: string | null
    ip?: string | null
    trigger_operation: 'INSERT' | 'UPDATE'
    timestamp: string
  }
}

export async function processConsentChange(payload: ConsentProcessingPayload): Promise<void> {
  const { user_id, consent_type, consent_value, handle, metadata } = payload

  logger.logInfo('ConsentProcessing', 'Processing consent change', `${consent_type}=${consent_value}`, user_id, {
    consent_type,
    consent_value,
    handle,
    metadata,
  })

  switch (consent_type) {
    case 'bluesky_dm':
    case 'mastodon_dm':
      await handlePlatformDMConsent(user_id, consent_type, consent_value, handle)
      break

    case 'email_newsletter':
      await handleEmailNewsletterConsent(user_id, consent_value)
      break

    default:
      logger.logInfo('ConsentProcessing', 'Ignoring unsupported consent type', consent_type, user_id)
  }

  logger.logInfo('ConsentProcessing', 'Consent change processed successfully', consent_type, user_id)
}

async function handlePlatformDMConsent(
  userId: string,
  consentType: 'bluesky_dm' | 'mastodon_dm',
  consentValue: boolean,
  handle: string | null | undefined
) {
  const platform = consentType === 'bluesky_dm' ? 'bluesky' : 'mastodon'

  if (consentValue) {
    if (handle) {
      await createTestDMTask(userId, platform, handle)
      logger.logInfo('ConsentProcessing', 'Created pending test-dm task', `${platform}:${handle}`, userId)
      return
    }

    await createWaitingTestDMTask(userId, platform)
    logger.logInfo('ConsentProcessing', 'Created waiting test-dm task', platform, userId)
    return
  }

  await pgPythonTasksRepository.deleteTasks(userId, platform, ['pending', 'waiting'])
  logger.logInfo('ConsentProcessing', 'Deleted pending/waiting DM tasks', platform, userId)
}

async function handleEmailNewsletterConsent(userId: string, consentValue: boolean) {
  if (consentValue) {
    await pgNewsletterListingRepository.insertNewsletterListing(userId)
    logger.logInfo('ConsentProcessing', 'Added user to newsletter listing', 'email_newsletter=true', userId)
    return
  }

  await pgNewsletterListingRepository.deleteNewsletterListing(userId)
  logger.logInfo('ConsentProcessing', 'Removed user from newsletter listing', 'email_newsletter=false', userId)
}

async function createTestDMTask(
  userId: string,
  platform: 'bluesky' | 'mastodon',
  handle: string
) {
  const exists = await pgPythonTasksRepository.pendingTaskExists(userId, platform, 'test-dm')
  if (exists) {
    logger.logInfo('ConsentProcessing', 'Pending test-dm task already exists', platform, userId)
    return
  }

  const taskPayload = { handle }
  const newTaskId = await pgPythonTasksRepository.createPendingTask(userId, platform, 'test-dm', taskPayload)

  try {
    await addTaskToRedis(userId, platform, handle, newTaskId)
  } catch (error) {
    logger.logError(
      'ConsentProcessing',
      'Failed to enqueue consent task in Redis (DB task still created)',
      error instanceof Error ? error.message : String(error),
      userId,
      { platform, taskId: newTaskId }
    )
  }
}

async function createWaitingTestDMTask(
  userId: string,
  platform: 'bluesky' | 'mastodon'
) {
  const exists = await pgPythonTasksRepository.waitingTaskExists(userId, platform, 'test-dm')
  if (exists) {
    logger.logInfo('ConsentProcessing', 'Waiting test-dm task already exists', platform, userId)
    return
  }

  await pgPythonTasksRepository.createWaitingTask(userId, platform, 'test-dm')
}

async function addTaskToRedis(
  userId: string,
  platform: 'bluesky' | 'mastodon',
  handle: string,
  taskId: string
) {
  const today = new Date().toISOString().split('T')[0]
  const queueKey = `consent_tasks:${today}`
  const dedupeKey = `task_dedup:${userId}:${platform}:test-dm`

  const existingTask = await redis.get(dedupeKey)
  if (existingTask) {
    logger.logInfo('ConsentProcessing', 'Task already present in Redis dedupe', platform, userId)
    return
  }

  const taskData = {
    id: taskId,
    user_id: userId,
    task_type: 'test-dm',
    platform,
    handle,
    created_at: new Date().toISOString(),
    status: 'pending',
  }

  await redis.lpush(queueKey, JSON.stringify(taskData))
  await redis.setex(dedupeKey, 3600, JSON.stringify(taskData))
}
