import logger from '../../log_utils'
import type { ConsentProcessingNotifyPayload } from '../types'
import { processConsentChange } from '../../consent/process-consent-change'

export async function handleConsentProcessing(payload: ConsentProcessingNotifyPayload): Promise<void> {
  try {
    await processConsentChange({
      user_id: payload.user_id,
      consent_type: payload.consent_type,
      consent_value: payload.consent_value,
      old_consent_value: payload.old_consent_value ?? null,
      handle: payload.handle ?? null,
      metadata: {
        userAgent: payload.metadata?.userAgent ?? null,
        ip: payload.metadata?.ip ?? null,
        trigger_operation: payload.metadata?.trigger_operation ?? payload.operation,
        timestamp: payload.metadata?.timestamp ?? new Date((payload.timestamp ?? Date.now() / 1000) * 1000).toISOString(),
      },
    })
  } catch (error) {
    logger.logError(
      'PgNotify',
      'Failed to process consent notification',
      error instanceof Error ? error.message : String(error),
      payload.user_id,
      payload
    )
  }
}
