import { beforeEach, describe, expect, it, vi } from 'vitest'

const mockContext = {
  data: null as any,
  session: null as any,
}

type MockJsonResponse = {
  data: any
  status?: number
}

const mockGetAccountByProviderAndUserId = vi.fn()
const mockVerifyAndRefreshBlueskyToken = vi.fn()
const mockVerifyAndRefreshMastodonToken = vi.fn()
const mockBatchFollowOAuth = vi.fn()
const mockBatchFollow = vi.fn()
const mockMastodonBatchFollow = vi.fn()
const mockResumeSession = vi.fn()
const mockUpdateFollowStatusBatch = vi.fn()
const mockUpdateSourcesFollowersByNodeIds = vi.fn()
const mockUpdateSourcesFollowersStatusBatch = vi.fn()
const mockRefreshUserStatsCache = vi.fn()
const mockCheckRateLimit = vi.fn()
const mockConsumeRateLimit = vi.fn()
const mockGetCoordHashesByNodeIds = vi.fn()
const mockPublishFollowingStatusUpdate = vi.fn()

vi.mock('@/lib/validation/middleware', () => ({
  withValidation: (_schema: unknown, handler: (request: Request, data: any, session: any) => Promise<any>) => {
    return async (request: Request) => handler(request, mockContext.data, mockContext.session)
  },
}))

vi.mock('@/lib/services/accountService', () => ({
  AccountService: class {
    getAccountByProviderAndUserId = mockGetAccountByProviderAndUserId
    verifyAndRefreshBlueskyToken = mockVerifyAndRefreshBlueskyToken
    verifyAndRefreshMastodonToken = mockVerifyAndRefreshMastodonToken
  },
}))

vi.mock('@/lib/services/blueskyServices', () => ({
  BlueskyService: class {
    batchFollowOAuth = mockBatchFollowOAuth
    batchFollow = mockBatchFollow
    resumeSession = mockResumeSession
  },
}))

vi.mock('@/lib/services/mastodonService', () => ({
  MastodonService: class {
    batchFollow = mockMastodonBatchFollow
  },
}))

vi.mock('@/lib/repositories/blueskyRepository', () => ({
  BlueskyRepository: class {},
}))

vi.mock('@/lib/services/matchingService', () => ({
  MatchingService: class {
    updateFollowStatusBatch = mockUpdateFollowStatusBatch
    updateSourcesFollowersByNodeIds = mockUpdateSourcesFollowersByNodeIds
    updateSourcesFollowersStatusBatch = mockUpdateSourcesFollowersStatusBatch
  },
}))

vi.mock('@/lib/repositories/statsRepository', () => ({
  StatsRepository: class {
    refreshUserStatsCache = mockRefreshUserStatsCache
  },
}))

vi.mock('@/lib/services/rateLimitService', () => ({
  checkRateLimit: mockCheckRateLimit,
  consumeRateLimit: mockConsumeRateLimit,
}))

vi.mock('@/lib/repositories/network/pg-matching-repository', () => ({
  pgMatchingRepository: {
    getCoordHashesByNodeIds: mockGetCoordHashesByNodeIds,
  },
}))

vi.mock('@/lib/sse-publisher', () => ({
  publishFollowingStatusUpdate: mockPublishFollowingStatusUpdate,
}))

vi.mock('@/lib/log_utils', () => ({
  default: {
    logError: vi.fn(),
    logWarning: vi.fn(),
    logInfo: vi.fn(),
    logDebug: vi.fn(),
  },
}))

describe('POST /api/migrate/send_follow', () => {
  beforeEach(() => {
    vi.clearAllMocks()

    mockContext.data = {
      accounts: [
        {
          node_id: '12345',
          bluesky_handle: 'target.bsky.social',
          has_follow_bluesky: false,
          mastodon_username: null,
          mastodon_instance: null,
          has_follow_mastodon: false,
        },
      ],
    }

    mockContext.session = {
      user: {
        id: 'user-123',
        twitter_id: '555666777',
        has_onboarded: true,
        mastodon_instance: 'https://home.social',
      },
    }

    mockGetAccountByProviderAndUserId.mockImplementation(async (provider: string) => {
      if (provider === 'bluesky') {
        return {
          provider_account_id: 'did:plc:alice',
          token_type: 'DPOP',
          scope: 'atproto transition:generic',
          access_token: 'access-token',
          refresh_token: 'refresh-token',
          username: 'alice.bsky.social',
        }
      }

      return null
    })

    mockVerifyAndRefreshBlueskyToken.mockResolvedValue({ success: true })
    mockVerifyAndRefreshMastodonToken.mockResolvedValue({ success: true })
    mockCheckRateLimit.mockResolvedValue({
      allowed: true,
      remainingHour: 100,
      remainingDay: 1000,
      maxFollowsAllowed: 50,
    })
    mockBatchFollowOAuth.mockResolvedValue({
      succeeded: 1,
      failures: [],
    })
    mockBatchFollow.mockResolvedValue({
      succeeded: 1,
      failures: [],
    })
    mockMastodonBatchFollow.mockResolvedValue({
      attempted: 1,
      succeeded: 1,
      failures: [],
      successfulHandles: ['target_user@target.social'],
    })
    mockGetCoordHashesByNodeIds.mockResolvedValue({
      data: new Map([['12345', 'coord-hash-1']]),
      error: null,
    })
    mockPublishFollowingStatusUpdate.mockResolvedValue(true)
  })

  it('follows Bluesky targets successfully for an onboarded user', async () => {
    const { POST } = await import('@/app/api/migrate/send_follow/route')

    const response = await POST({} as any) as unknown as MockJsonResponse

    expect(mockGetAccountByProviderAndUserId).toHaveBeenCalledWith('bluesky', 'user-123')
    expect(mockGetAccountByProviderAndUserId).toHaveBeenCalledWith('mastodon', 'user-123')
    expect(mockVerifyAndRefreshBlueskyToken).toHaveBeenCalledWith('user-123')
    expect(mockCheckRateLimit).toHaveBeenCalledWith('user-123', 1)
    expect(mockBatchFollowOAuth).toHaveBeenCalledWith('did:plc:alice', ['target.bsky.social'])
    expect(mockConsumeRateLimit).toHaveBeenCalledWith('user-123', 1)
    expect(mockUpdateFollowStatusBatch).toHaveBeenCalledWith('user-123', ['12345'], 'bluesky', true, undefined)
    expect(mockUpdateSourcesFollowersByNodeIds).not.toHaveBeenCalled()
    expect(mockUpdateSourcesFollowersStatusBatch).not.toHaveBeenCalled()
    expect(mockRefreshUserStatsCache).not.toHaveBeenCalled()
    expect(mockGetCoordHashesByNodeIds).toHaveBeenCalledWith(['12345'])
    expect(mockPublishFollowingStatusUpdate).toHaveBeenCalledWith('user-123', [
      { coord_hash: 'coord-hash-1', platform: 'bluesky', followed: true },
    ])

    expect(response.status).toBeUndefined()
    expect(response.data).toEqual({
      bluesky: {
        succeeded: 1,
        failed: 0,
        failures: [],
        coordHashes: ['coord-hash-1'],
      },
      mastodon: null,
    })
  })

  it('follows Mastodon targets successfully for an onboarded user', async () => {
    mockContext.data = {
      accounts: [
        {
          node_id: '67890',
          bluesky_handle: null,
          has_follow_bluesky: false,
          mastodon_username: 'target_user',
          mastodon_instance: 'https://target.social',
          mastodon_id: 'mastodon-target-1',
          has_follow_mastodon: false,
        },
      ],
    }

    mockContext.session = {
      user: {
        id: 'user-123',
        twitter_id: '555666777',
        has_onboarded: true,
        mastodon_instance: 'https://home.social',
        social_accounts: [
          { provider: 'mastodon', username: 'home_user', instance: 'https://home.social' },
        ],
      },
    }

    mockGetAccountByProviderAndUserId.mockImplementation(async (provider: string) => {
      if (provider === 'mastodon') {
        return {
          provider_account_id: 'mastodon-home-account',
          access_token: 'mastodon-access-token',
          scope: 'read write follow',
          username: 'home_user',
        }
      }

      return null
    })

    mockGetCoordHashesByNodeIds.mockResolvedValue({
      data: new Map([['67890', 'coord-hash-2']]),
      error: null,
    })

    const { POST } = await import('@/app/api/migrate/send_follow/route')

    const response = await POST({} as any) as unknown as MockJsonResponse

    expect(mockGetAccountByProviderAndUserId).toHaveBeenCalledWith('bluesky', 'user-123')
    expect(mockGetAccountByProviderAndUserId).toHaveBeenCalledWith('mastodon', 'user-123')
    expect(mockVerifyAndRefreshMastodonToken).toHaveBeenCalledWith('user-123')
    expect(mockCheckRateLimit).not.toHaveBeenCalled()
    expect(mockBatchFollowOAuth).not.toHaveBeenCalled()
    expect(mockBatchFollow).not.toHaveBeenCalled()
    expect(mockMastodonBatchFollow).toHaveBeenCalledWith(
      'mastodon-access-token',
      'https://home.social',
      [
        {
          username: 'target_user',
          instance: 'https://target.social',
          id: 'mastodon-target-1',
        },
      ]
    )
    expect(mockConsumeRateLimit).not.toHaveBeenCalled()
    expect(mockUpdateFollowStatusBatch).toHaveBeenCalledWith('user-123', ['67890'], 'mastodon', true, undefined)
    expect(mockUpdateSourcesFollowersByNodeIds).not.toHaveBeenCalled()
    expect(mockUpdateSourcesFollowersStatusBatch).not.toHaveBeenCalled()
    expect(mockRefreshUserStatsCache).not.toHaveBeenCalled()
    expect(mockGetCoordHashesByNodeIds).toHaveBeenCalledWith(['67890'])
    expect(mockPublishFollowingStatusUpdate).toHaveBeenCalledWith('user-123', [
      { coord_hash: 'coord-hash-2', platform: 'mastodon', followed: true },
    ])

    expect(response.status).toBeUndefined()
    expect(response.data).toEqual({
      bluesky: null,
      mastodon: {
        succeeded: 1,
        failed: 0,
        failures: [],
        coordHashes: ['coord-hash-2'],
      },
    })
  })

  it('returns null results when the user has no connected Bluesky or Mastodon account', async () => {
    mockGetAccountByProviderAndUserId.mockResolvedValue(null)

    const { POST } = await import('@/app/api/migrate/send_follow/route')

    const response = await POST({} as any) as unknown as MockJsonResponse

    expect(mockVerifyAndRefreshBlueskyToken).not.toHaveBeenCalled()
    expect(mockVerifyAndRefreshMastodonToken).not.toHaveBeenCalled()
    expect(mockBatchFollowOAuth).not.toHaveBeenCalled()
    expect(mockMastodonBatchFollow).not.toHaveBeenCalled()
    expect(mockUpdateFollowStatusBatch).not.toHaveBeenCalled()
    expect(mockPublishFollowingStatusUpdate).not.toHaveBeenCalled()
    expect(response.data).toEqual({
      bluesky: null,
      mastodon: null,
    })
  })

  it('returns reauth response when Bluesky token verification fails', async () => {
    mockGetAccountByProviderAndUserId.mockImplementation(async (provider: string) => {
      if (provider === 'bluesky') {
        return {
          provider_account_id: 'did:plc:alice',
          token_type: 'DPOP',
          scope: 'atproto transition:generic',
          access_token: 'access-token',
          refresh_token: 'refresh-token',
          username: 'alice.bsky.social',
        }
      }

      return null
    })
    mockVerifyAndRefreshBlueskyToken.mockResolvedValue({ success: false, requiresReauth: true })

    const { POST } = await import('@/app/api/migrate/send_follow/route')

    const response = await POST({} as any) as unknown as MockJsonResponse

    expect(mockVerifyAndRefreshBlueskyToken).toHaveBeenCalledWith('user-123')
    expect(mockBatchFollowOAuth).not.toHaveBeenCalled()
    expect(mockConsumeRateLimit).not.toHaveBeenCalled()
    expect(response.status).toBe(401)
    expect(response.data).toEqual({
      error: 'Bluesky authentication required',
      requiresReauth: true,
      providers: ['bluesky'],
    })
  })

  it('returns reauth response when Mastodon token verification fails', async () => {
    mockContext.data = {
      accounts: [
        {
          node_id: '67890',
          bluesky_handle: null,
          has_follow_bluesky: false,
          mastodon_username: 'target_user',
          mastodon_instance: 'https://target.social',
          mastodon_id: 'mastodon-target-1',
          has_follow_mastodon: false,
        },
      ],
    }

    mockContext.session = {
      user: {
        id: 'user-123',
        twitter_id: '555666777',
        has_onboarded: true,
        mastodon_instance: 'https://home.social',
        social_accounts: [
          { provider: 'mastodon', username: 'home_user', instance: 'https://home.social' },
        ],
      },
    }

    mockGetAccountByProviderAndUserId.mockImplementation(async (provider: string) => {
      if (provider === 'mastodon') {
        return {
          provider_account_id: 'mastodon-home-account',
          access_token: 'mastodon-access-token',
          scope: 'read write follow',
          username: 'home_user',
        }
      }

      return null
    })
    mockVerifyAndRefreshMastodonToken.mockResolvedValue({
      success: false,
      requiresReauth: true,
      errorCode: 'MastodonRateLimit',
    })

    const { POST } = await import('@/app/api/migrate/send_follow/route')

    const response = await POST({} as any) as unknown as MockJsonResponse

    expect(mockVerifyAndRefreshMastodonToken).toHaveBeenCalledWith('user-123')
    expect(mockMastodonBatchFollow).not.toHaveBeenCalled()
    expect(response.status).toBe(401)
    expect(response.data).toEqual({
      error: 'Mastodon authentication required',
      requiresReauth: true,
      providers: ['mastodon'],
      errorCode: 'MastodonRateLimit',
    })
  })

  it('updates sources_followers and refreshes stats for a non-onboarded Bluesky user', async () => {
    mockContext.data = {
      accounts: [
        {
          node_id: '24680',
          bluesky_handle: 'target2.bsky.social',
          has_follow_bluesky: false,
          mastodon_username: null,
          mastodon_instance: null,
          has_follow_mastodon: false,
        },
      ],
    }

    mockContext.session = {
      user: {
        id: 'user-123',
        twitter_id: '555666777',
        has_onboarded: false,
        mastodon_instance: 'https://home.social',
      },
    }

    mockGetAccountByProviderAndUserId.mockImplementation(async (provider: string) => {
      if (provider === 'bluesky') {
        return {
          provider_account_id: 'did:plc:alice',
          token_type: 'DPOP',
          scope: 'atproto transition:generic',
          access_token: 'access-token',
          refresh_token: 'refresh-token',
          username: 'alice.bsky.social',
        }
      }

      return null
    })
    mockGetCoordHashesByNodeIds.mockResolvedValue({
      data: new Map([['24680', 'coord-hash-3']]),
      error: null,
    })

    const { POST } = await import('@/app/api/migrate/send_follow/route')

    const response = await POST({} as any) as unknown as MockJsonResponse

    expect(mockUpdateFollowStatusBatch).not.toHaveBeenCalledWith('user-123', ['24680'], 'bluesky', true, undefined)
    expect(mockUpdateSourcesFollowersByNodeIds).toHaveBeenCalledWith('555666777', ['24680'], 'bluesky', true, undefined)
    expect(mockRefreshUserStatsCache).toHaveBeenCalledWith('user-123', false)
    expect(mockPublishFollowingStatusUpdate).toHaveBeenCalledWith('user-123', [
      { coord_hash: 'coord-hash-3', platform: 'bluesky', followed: true },
    ])
    expect(response.data).toEqual({
      bluesky: {
        succeeded: 1,
        failed: 0,
        failures: [],
        coordHashes: ['coord-hash-3'],
      },
      mastodon: null,
    })
  })

  it('returns 429 when Bluesky rate limit is exceeded', async () => {
    mockGetAccountByProviderAndUserId.mockImplementation(async (provider: string) => {
      if (provider === 'bluesky') {
        return {
          provider_account_id: 'did:plc:alice',
          token_type: 'DPOP',
          scope: 'atproto transition:generic',
          access_token: 'access-token',
          refresh_token: 'refresh-token',
          username: 'alice.bsky.social',
        }
      }

      return null
    })
    mockCheckRateLimit.mockResolvedValue({
      allowed: false,
      reason: 'Hourly rate limit exceeded. 0 follows remaining this hour.',
      maxFollowsAllowed: 0,
      retryAfterSeconds: 120,
    })

    const { POST } = await import('@/app/api/migrate/send_follow/route')

    const response = await POST({} as any) as unknown as MockJsonResponse

    expect(mockCheckRateLimit).toHaveBeenCalledWith('user-123', 1)
    expect(mockVerifyAndRefreshBlueskyToken).not.toHaveBeenCalled()
    expect(mockBatchFollowOAuth).not.toHaveBeenCalled()
    expect(mockConsumeRateLimit).not.toHaveBeenCalled()
    expect(response.status).toBe(429)
    expect(response.data).toEqual({
      error: 'Rate limit exceeded',
      rateLimited: true,
      reason: 'Hourly rate limit exceeded. 0 follows remaining this hour.',
      maxFollowsAllowed: 0,
      retryAfterSeconds: 120,
    })
  })

  it('handles partial Bluesky failures for onboarded users', async () => {
    mockContext.data = {
      accounts: [
        {
          node_id: '11111',
          bluesky_handle: 'success.bsky.social',
          has_follow_bluesky: false,
          mastodon_username: null,
          mastodon_instance: null,
          has_follow_mastodon: false,
        },
        {
          node_id: '22222',
          bluesky_handle: 'failed.bsky.social',
          has_follow_bluesky: false,
          mastodon_username: null,
          mastodon_instance: null,
          has_follow_mastodon: false,
        },
      ],
    }

    mockBatchFollowOAuth.mockResolvedValue({
      succeeded: 1,
      failures: [
        {
          handle: 'failed.bsky.social',
          error: 'Already blocked',
        },
      ],
    })
    mockGetCoordHashesByNodeIds.mockResolvedValue({
      data: new Map([['11111', 'coord-hash-success']]),
      error: null,
    })

    const { POST } = await import('@/app/api/migrate/send_follow/route')

    const response = await POST({} as any) as unknown as MockJsonResponse

    expect(mockConsumeRateLimit).toHaveBeenCalledWith('user-123', 1)
    expect(mockUpdateFollowStatusBatch).toHaveBeenNthCalledWith(1, 'user-123', ['11111'], 'bluesky', true, undefined)
    expect(mockUpdateFollowStatusBatch).toHaveBeenNthCalledWith(2, 'user-123', ['22222'], 'bluesky', false, 'Already blocked')
    expect(mockGetCoordHashesByNodeIds).toHaveBeenCalledWith(['11111'])
    expect(mockPublishFollowingStatusUpdate).toHaveBeenCalledWith('user-123', [
      { coord_hash: 'coord-hash-success', platform: 'bluesky', followed: true },
    ])
    expect(response.data).toEqual({
      bluesky: {
        succeeded: 1,
        failed: 1,
        failures: [
          {
            handle: 'failed.bsky.social',
            error: 'Already blocked',
          },
        ],
        coordHashes: ['coord-hash-success'],
      },
      mastodon: null,
    })
  })

  it('updates matched follower statuses for Bluesky accounts with source_twitter_id', async () => {
    mockContext.data = {
      accounts: [
        {
          source_twitter_id: '99999',
          bluesky_handle: 'matched.bsky.social',
          has_been_followed_on_bluesky: false,
          mastodon_username: null,
          mastodon_instance: null,
          has_been_followed_on_mastodon: false,
        },
      ],
    }

    const { POST } = await import('@/app/api/migrate/send_follow/route')

    const response = await POST({} as any) as unknown as MockJsonResponse

    expect(mockUpdateSourcesFollowersStatusBatch).toHaveBeenCalledWith(
      '555666777',
      ['99999'],
      'bluesky',
      true,
      undefined
    )
    expect(mockUpdateFollowStatusBatch).not.toHaveBeenCalled()
    expect(mockGetCoordHashesByNodeIds).not.toHaveBeenCalled()
    expect(mockPublishFollowingStatusUpdate).not.toHaveBeenCalled()
    expect(response.data).toEqual({
      bluesky: {
        succeeded: 1,
        failed: 0,
        failures: [],
        coordHashes: [],
      },
      mastodon: null,
    })
  })

  it('updates sources_followers and refreshes stats for a non-onboarded Mastodon user', async () => {
    mockContext.data = {
      accounts: [
        {
          node_id: '86420',
          bluesky_handle: null,
          has_follow_bluesky: false,
          mastodon_username: 'target_user',
          mastodon_instance: 'https://target.social',
          mastodon_id: 'mastodon-target-2',
          has_follow_mastodon: false,
        },
      ],
    }
    mockContext.session = {
      user: {
        id: 'user-123',
        twitter_id: '555666777',
        has_onboarded: false,
        mastodon_instance: 'https://home.social',
        social_accounts: [
          { provider: 'mastodon', username: 'home_user', instance: 'https://home.social' },
        ],
      },
    }
    mockGetAccountByProviderAndUserId.mockImplementation(async (provider: string) => {
      if (provider === 'mastodon') {
        return {
          provider_account_id: 'mastodon-home-account',
          access_token: 'mastodon-access-token',
          scope: 'read write follow',
          username: 'home_user',
        }
      }
      return null
    })
    mockGetCoordHashesByNodeIds.mockResolvedValue({
      data: new Map([['86420', 'coord-hash-4']]),
      error: null,
    })

    const { POST } = await import('@/app/api/migrate/send_follow/route')
    const response = await POST({} as any) as unknown as MockJsonResponse

    expect(mockUpdateFollowStatusBatch).not.toHaveBeenCalledWith('user-123', ['86420'], 'mastodon', true, undefined)
    expect(mockUpdateSourcesFollowersByNodeIds).toHaveBeenCalledWith('555666777', ['86420'], 'mastodon', true, undefined)
    expect(mockRefreshUserStatsCache).toHaveBeenCalledWith('user-123', false)
    expect(mockPublishFollowingStatusUpdate).toHaveBeenCalledWith('user-123', [
      { coord_hash: 'coord-hash-4', platform: 'mastodon', followed: true },
    ])
    expect(response.data).toEqual({
      bluesky: null,
      mastodon: {
        succeeded: 1,
        failed: 0,
        failures: [],
        coordHashes: ['coord-hash-4'],
      },
    })
  })

  it('handles partial Mastodon failures for onboarded users', async () => {
    mockContext.data = {
      accounts: [
        {
          node_id: '33333',
          bluesky_handle: null,
          has_follow_bluesky: false,
          mastodon_username: 'success_user',
          mastodon_instance: 'https://target.social',
          mastodon_id: 'mastodon-target-3',
          has_follow_mastodon: false,
        },
        {
          node_id: '44444',
          bluesky_handle: null,
          has_follow_bluesky: false,
          mastodon_username: 'failed_user',
          mastodon_instance: 'https://target.social',
          mastodon_id: 'mastodon-target-4',
          has_follow_mastodon: false,
        },
      ],
    }
    mockContext.session = {
      user: {
        id: 'user-123',
        twitter_id: '555666777',
        has_onboarded: true,
        mastodon_instance: 'https://home.social',
        social_accounts: [
          { provider: 'mastodon', username: 'home_user', instance: 'https://home.social' },
        ],
      },
    }
    mockGetAccountByProviderAndUserId.mockImplementation(async (provider: string) => {
      if (provider === 'mastodon') {
        return {
          provider_account_id: 'mastodon-home-account',
          access_token: 'mastodon-access-token',
          scope: 'read write follow',
          username: 'home_user',
        }
      }
      return null
    })
    mockMastodonBatchFollow.mockResolvedValue({
      attempted: 2,
      succeeded: 1,
      failures: [
        { handle: 'failed_user@target.social', error: 'Remote account suspended' },
      ],
      successfulHandles: ['success_user@target.social'],
    })
    mockGetCoordHashesByNodeIds.mockResolvedValue({
      data: new Map([['33333', 'coord-hash-masto-success']]),
      error: null,
    })

    const { POST } = await import('@/app/api/migrate/send_follow/route')
    const response = await POST({} as any) as unknown as MockJsonResponse

    expect(mockUpdateFollowStatusBatch).toHaveBeenNthCalledWith(1, 'user-123', ['33333'], 'mastodon', true, undefined)
    expect(mockUpdateFollowStatusBatch).toHaveBeenNthCalledWith(2, 'user-123', ['44444'], 'mastodon', false, 'Remote account suspended')
    expect(mockGetCoordHashesByNodeIds).toHaveBeenCalledWith(['33333'])
    expect(mockPublishFollowingStatusUpdate).toHaveBeenCalledWith('user-123', [
      { coord_hash: 'coord-hash-masto-success', platform: 'mastodon', followed: true },
    ])
    expect(response.data).toEqual({
      bluesky: null,
      mastodon: {
        succeeded: 1,
        failed: 1,
        failures: [
          { handle: 'failed_user@target.social', error: 'Remote account suspended' },
        ],
        coordHashes: ['coord-hash-masto-success'],
      },
    })
  })

  it('updates matched follower statuses for Mastodon accounts with source_twitter_id', async () => {
    mockContext.data = {
      accounts: [
        {
          source_twitter_id: '88888',
          mastodon_username: 'matched_user',
          mastodon_instance: 'https://target.social',
          mastodon_id: 'matched-mastodon-id',
          has_been_followed_on_mastodon: false,
          bluesky_handle: null,
          has_been_followed_on_bluesky: false,
        },
      ],
    }
    mockContext.session = {
      user: {
        id: 'user-123',
        twitter_id: '555666777',
        has_onboarded: true,
        mastodon_instance: 'https://home.social',
        social_accounts: [
          { provider: 'mastodon', username: 'home_user', instance: 'https://home.social' },
        ],
      },
    }
    mockGetAccountByProviderAndUserId.mockImplementation(async (provider: string) => {
      if (provider === 'mastodon') {
        return {
          provider_account_id: 'mastodon-home-account',
          access_token: 'mastodon-access-token',
          scope: 'read write follow',
          username: 'home_user',
        }
      }
      return null
    })

    const { POST } = await import('@/app/api/migrate/send_follow/route')
    const response = await POST({} as any) as unknown as MockJsonResponse

    expect(mockUpdateSourcesFollowersStatusBatch).toHaveBeenCalledWith('555666777', ['88888'], 'mastodon', true, undefined)
    expect(mockUpdateFollowStatusBatch).not.toHaveBeenCalled()
    expect(mockGetCoordHashesByNodeIds).not.toHaveBeenCalled()
    expect(mockPublishFollowingStatusUpdate).not.toHaveBeenCalled()
    expect(response.data).toEqual({
      bluesky: null,
      mastodon: {
        succeeded: 1,
        failed: 0,
        failures: [],
        coordHashes: [],
      },
    })
  })

  it('uses Bluesky app-password session flow when account is not OAuth', async () => {
    mockGetAccountByProviderAndUserId.mockImplementation(async (provider: string) => {
      if (provider === 'bluesky') {
        return {
          provider_account_id: 'did:plc:alice',
          token_type: 'Bearer',
          scope: 'legacy-scope',
          access_token: 'access-token',
          refresh_token: 'refresh-token',
          username: 'alice.bsky.social',
        }
      }
      return null
    })

    const { POST } = await import('@/app/api/migrate/send_follow/route')
    const response = await POST({} as any) as unknown as MockJsonResponse

    expect(mockResumeSession).toHaveBeenCalledWith({
      accessJwt: 'access-token',
      refreshJwt: 'refresh-token',
      handle: 'alice.bsky.social',
      did: 'did:plc:alice',
    })
    expect(mockBatchFollow).toHaveBeenCalledWith(['target.bsky.social'])
    expect(mockBatchFollowOAuth).not.toHaveBeenCalled()
    expect(response.data.bluesky.succeeded).toBe(1)
  })

  it('combines successful Bluesky and Mastodon follows in one response and SSE update', async () => {
    mockContext.data = {
      accounts: [
        {
          node_id: '12345',
          bluesky_handle: 'target.bsky.social',
          has_follow_bluesky: false,
          mastodon_username: null,
          mastodon_instance: null,
          has_follow_mastodon: false,
        },
        {
          node_id: '67890',
          bluesky_handle: null,
          has_follow_bluesky: false,
          mastodon_username: 'target_user',
          mastodon_instance: 'https://target.social',
          mastodon_id: 'mastodon-target-1',
          has_follow_mastodon: false,
        },
      ],
    }
    mockContext.session = {
      user: {
        id: 'user-123',
        twitter_id: '555666777',
        has_onboarded: true,
        mastodon_instance: 'https://home.social',
        social_accounts: [
          { provider: 'mastodon', username: 'home_user', instance: 'https://home.social' },
        ],
      },
    }
    mockGetAccountByProviderAndUserId.mockImplementation(async (provider: string) => {
      if (provider === 'bluesky') {
        return {
          provider_account_id: 'did:plc:alice',
          token_type: 'DPOP',
          scope: 'atproto transition:generic',
          access_token: 'access-token',
          refresh_token: 'refresh-token',
          username: 'alice.bsky.social',
        }
      }
      if (provider === 'mastodon') {
        return {
          provider_account_id: 'mastodon-home-account',
          access_token: 'mastodon-access-token',
          scope: 'read write follow',
          username: 'home_user',
        }
      }
      return null
    })
    mockGetCoordHashesByNodeIds.mockResolvedValue({
      data: new Map([
        ['12345', 'coord-hash-1'],
        ['67890', 'coord-hash-2'],
      ]),
      error: null,
    })

    const { POST } = await import('@/app/api/migrate/send_follow/route')
    const response = await POST({} as any) as unknown as MockJsonResponse

    expect(mockPublishFollowingStatusUpdate).toHaveBeenCalledWith('user-123', [
      { coord_hash: 'coord-hash-1', platform: 'bluesky', followed: true },
      { coord_hash: 'coord-hash-2', platform: 'mastodon', followed: true },
    ])
    expect(response.data).toEqual({
      bluesky: {
        succeeded: 1,
        failed: 0,
        failures: [],
        coordHashes: ['coord-hash-1'],
      },
      mastodon: {
        succeeded: 1,
        failed: 0,
        failures: [],
        coordHashes: ['coord-hash-2'],
      },
    })
  })
})
