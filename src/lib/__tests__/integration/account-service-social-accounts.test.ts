import { describe, it, expect } from 'vitest'
import { randomUUID } from 'crypto'
import { pgUserRepository } from '@/lib/repositories/auth/pg-user-repository'
import { pgSocialAccountRepository } from '@/lib/repositories/auth/pg-social-account-repository'
import { pgAccountRepository } from '@/lib/repositories/auth/pg-account-repository'
import { AccountService } from '@/lib/services/accountService'
import { mockBlueskyAccount, mockMastodonAccount } from '../fixtures/account-fixtures'

function uniqueUser() {
  return {
    name: 'Lookup Test User',
    email: `lookup-${randomUUID()}@example.com`,
    has_onboarded: false,
    hqx_newsletter: false,
    oep_accepted: false,
    research_accepted: false,
    have_seen_newsletter: false,
    automatic_reconnect: false,
  }
}

describe('Account lookup with users + social_accounts + accounts', () => {
  it('resolves a Bluesky user via social_accounts and returns the provider account via AccountService', async () => {
    const user = await pgUserRepository.createUser(uniqueUser())

    const blueskyDid = `did:plc:${randomUUID().replace(/-/g, '')}`
    await pgSocialAccountRepository.upsertSocialAccount({
      user_id: user.id,
      provider: 'bluesky',
      provider_account_id: blueskyDid,
      username: `lookup-${randomUUID().slice(0, 8)}.bsky.social`,
      instance: '',
      is_primary: true,
    })

    await pgAccountRepository.upsertAccount({
      ...mockBlueskyAccount(user.id),
      provider_account_id: blueskyDid,
      user_id: user.id,
      access_token: 'plain_bluesky_access_token',
      refresh_token: 'plain_bluesky_refresh_token',
    })

    const resolvedUser = await pgUserRepository.getUserByProviderId('bluesky', blueskyDid)
    const accountService = new AccountService()
    const providerAccount = await accountService.getAccountByProviderAndUserId('bluesky', user.id)

    expect(resolvedUser).not.toBeNull()
    expect(resolvedUser?.id).toBe(user.id)
    expect(providerAccount).not.toBeNull()
    expect(providerAccount?.user_id).toBe(user.id)
    expect(providerAccount?.provider).toBe('bluesky')
    expect(providerAccount?.provider_account_id).toBe(blueskyDid)
    expect(providerAccount?.access_token).toBe('plain_bluesky_access_token')
    expect(providerAccount?.refresh_token).toBe('plain_bluesky_refresh_token')
  })

  it('resolves a Mastodon user via social_accounts instance and returns the provider account via AccountService', async () => {
    const user = await pgUserRepository.createUser(uniqueUser())

    const mastodonId = `mastodon-${randomUUID().slice(0, 8)}`
    const mastodonInstance = 'https://target.social'
    await pgSocialAccountRepository.upsertSocialAccount({
      user_id: user.id,
      provider: 'mastodon',
      provider_account_id: mastodonId,
      username: `masto-${randomUUID().slice(0, 8)}`,
      instance: mastodonInstance,
      is_primary: true,
    })

    await pgAccountRepository.upsertAccount({
      ...mockMastodonAccount(user.id),
      provider_account_id: mastodonId,
      user_id: user.id,
      access_token: 'plain_mastodon_access_token',
    })

    const resolvedUser = await pgUserRepository.getUserByProviderId('mastodon', mastodonId, mastodonInstance)
    const accountService = new AccountService()
    const providerAccount = await accountService.getAccountByProviderAndUserId('mastodon', user.id)

    expect(resolvedUser).not.toBeNull()
    expect(resolvedUser?.id).toBe(user.id)
    expect(providerAccount).not.toBeNull()
    expect(providerAccount?.user_id).toBe(user.id)
    expect(providerAccount?.provider).toBe('mastodon')
    expect(providerAccount?.provider_account_id).toBe(mastodonId)
    expect(providerAccount?.access_token).toBe('plain_mastodon_access_token')
  })
})
