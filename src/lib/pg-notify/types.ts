export interface CacheInvalidationPayload {
  operation: 'INSERT' | 'UPDATE' | 'DELETE';
  twitter_id: string;
  consent_level: string | null;
  user_id: string;
  timestamp: number;
}

export interface ConsentChangePayload {
  operation: 'INSERT' | 'UPDATE' | 'DELETE';
  twitter_id: string;
  old_consent_level?: string | null;
  new_consent_level?: string | null;
  user_id: string;
  timestamp: number;
}

export interface ConsentProcessingNotifyPayload {
  operation: 'INSERT' | 'UPDATE';
  user_id: string;
  consent_type: 'bluesky_dm' | 'mastodon_dm' | 'email_newsletter' | 'oep_newsletter' | 'research_participation';
  consent_value: boolean;
  old_consent_value?: boolean | null;
  handle?: string | null;
  metadata?: {
    userAgent?: string | null;
    ip?: string | null;
    trigger_operation: 'INSERT' | 'UPDATE';
    timestamp: string;
  };
  timestamp?: number;
}

export interface MastodonCacheInvalidationPayload {
  operation?: string;
  timestamp?: number;
}

export interface UserStatsCacheInvalidationPayload {
  user_id: string;
  stats: Record<string, any>;
  updated_at?: string;
  timestamp?: number;
}

export interface GlobalStatsCacheInvalidationPayload {
  stats: Record<string, any>;
  timestamp?: number;
}

export interface NetworkSyncJobsPayload {
  job_id: number | string;
  user_id: string;
  provider: 'bluesky' | 'mastodon';
  scope: 'followers' | 'followings' | 'full_sync';
  status: 'pending' | 'retrying' | string;
  dedupe_key?: string;
  timestamp?: number;
}

export interface SyncRedisMappingPayload {
  action: 'upsert' | 'delete';
  platform: 'bluesky' | 'mastodon';
  twitter_id: string;
  bluesky_username?: string;
  bluesky_id?: string;
  mastodon_id?: string;
  mastodon_username?: string;
  mastodon_instance?: string;
  data?: {
    bluesky_username?: string;
    bluesky_id?: string;
    mastodon_id?: string;
    mastodon_username?: string;
    mastodon_instance?: string;
  };
  timestamp?: number;
}

export interface NetworkSyncJobNotifyPayload {
  job_id: number;
  user_id: string;
  provider: 'bluesky' | 'mastodon';
  scope: 'followers' | 'followings' | 'full_sync' | string;
  status: 'pending' | 'retrying' | string;
  dedupe_key?: string;
  timestamp?: number;
}
