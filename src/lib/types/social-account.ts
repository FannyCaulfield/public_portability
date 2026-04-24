import type { DBUser, DBSocialAccount } from './database'
import type { User as LegacyUser } from './user'

type BaseUserForLegacyHydration = Pick<
  DBUser,
  | 'id'
  | 'name'
  | 'email'
  | 'email_verified'
  | 'image'
  | 'created_at'
  | 'updated_at'
  | 'has_onboarded'
  | 'hqx_newsletter'
  | 'oep_accepted'
  | 'automatic_reconnect'
  | 'research_accepted'
  | 'have_seen_newsletter'
  | 'have_seen_v2'
>

export type SocialProvider = 'twitter' | 'bluesky' | 'mastodon' | 'linkedin' | string

export interface SocialAccount {
  id: string
  user_id: string
  provider: SocialProvider
  provider_account_id?: string | null
  username?: string | null
  instance?: string | null
  email?: string | null
  is_primary: boolean
  created_at: Date
  updated_at: Date
  last_seen_at?: Date | null
}

export interface SessionSocialAccount {
  provider: SocialProvider
  username?: string | null
  instance?: string | null
}

export function mapDbSocialAccount(account: DBSocialAccount): SocialAccount {
  return {
    id: account.id,
    user_id: account.user_id,
    provider: account.provider,
    provider_account_id: account.provider_account_id,
    username: account.username,
    instance: account.instance,
    email: account.email,
    is_primary: account.is_primary,
    created_at: account.created_at,
    updated_at: account.updated_at,
    last_seen_at: account.last_seen_at,
  }
}

export function mapSocialAccountToSessionSocialAccount(account: SocialAccount): SessionSocialAccount {
  return {
    provider: account.provider,
    username: account.username,
    instance: account.instance,
  }
}

function getAccount(accounts: SocialAccount[], provider: string): SocialAccount | undefined {
  return accounts.find((account) => account.provider === provider)
}

export function hydrateLegacyUserFromSocialAccounts(
  user: BaseUserForLegacyHydration,
  socialAccounts: SocialAccount[]
): LegacyUser {
  const twitter = getAccount(socialAccounts, 'twitter')
  const bluesky = getAccount(socialAccounts, 'bluesky')
  const mastodon = getAccount(socialAccounts, 'mastodon')

  return {
    id: user.id,
    name: user.name ?? undefined,
    twitter_id: twitter?.provider_account_id ?? undefined,
    twitter_username: twitter?.username ?? undefined,
    twitter_image: undefined,
    bluesky_id: bluesky?.provider_account_id ?? undefined,
    bluesky_username: bluesky?.username ?? undefined,
    bluesky_image: undefined,
    mastodon_id: mastodon?.provider_account_id ?? undefined,
    mastodon_username: mastodon?.username ?? undefined,
    mastodon_image: undefined,
    mastodon_instance: mastodon?.instance ?? undefined,
    email: user.email ?? undefined,
    email_verified: user.email_verified ?? undefined,
    image: user.image ?? undefined,
    created_at: user.created_at,
    updated_at: user.updated_at,
    has_onboarded: user.has_onboarded,
    hqx_newsletter: user.hqx_newsletter,
    oep_accepted: user.oep_accepted,
    automatic_reconnect: user.automatic_reconnect,
    research_accepted: user.research_accepted,
    have_seen_newsletter: user.have_seen_newsletter,
    have_seen_v2: user.have_seen_v2,
    personalized_support: false,
  }
}
