import type { SessionSocialAccount } from '@/lib/types/social-account'

export type ConnectedProvider = 'twitter' | 'bluesky' | 'mastodon' | 'youtube' | 'linkedin'

type SessionUserLike = {
  social_accounts?: SessionSocialAccount[]
  has_onboarded?: boolean
  bluesky_username?: string | null
  mastodon_username?: string | null
  youtube_username?: string | null
  linkedin_username?: string | null
} | null | undefined

export interface ConnectedAccountView {
  provider: ConnectedProvider
  username?: string
  instance?: string
}

export function getConnectedAccountsFromSessionUser(user: SessionUserLike): ConnectedAccountView[] {
  if (!user) {
    return []
  }

  const socialAccounts = (user.social_accounts ?? []) as SessionSocialAccount[]
  const normalizedAccounts = socialAccounts
    .filter((account): account is SessionSocialAccount & { provider: ConnectedProvider } => (
      account.provider === 'twitter' ||
      account.provider === 'bluesky' ||
      account.provider === 'mastodon' ||
      account.provider === 'youtube' ||
      account.provider === 'linkedin'
    ))
    .map((account) => ({
      provider: account.provider,
      username: account.username ?? undefined,
      instance: account.instance ?? undefined,
    }))

  return normalizedAccounts
}

export function getConnectedAccount(user: SessionUserLike, provider: ConnectedProvider): ConnectedAccountView | undefined {
  return getConnectedAccountsFromSessionUser(user).find((account) => account.provider === provider)
}

export function getConnectedMastodonInstance(user: SessionUserLike): string | undefined {
  const mastodonAccount = getConnectedAccount(user, 'mastodon')
  return mastodonAccount?.instance ?? undefined
}

export function hasConnectedProvider(user: SessionUserLike, provider: ConnectedProvider): boolean {
  return Boolean(getConnectedAccount(user, provider))
}

export function countConnectedProviders(user: SessionUserLike): number {
  return getConnectedAccountsFromSessionUser(user).length
}
