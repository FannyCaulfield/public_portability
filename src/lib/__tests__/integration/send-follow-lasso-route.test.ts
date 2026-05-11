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
const mockResumeSession = vi.fn()
const mockMastodonBatchFollow = vi.fn()
const mockCheckRateLimit = vi.fn()
const mockConsumeRateLimit = vi.fn()
const mockGetNodesByHashes = vi.fn()
const mockCreateFollowRequestsBatch = vi.fn()
const mockUpdateFollowRequestStatusBatch = vi.fn()

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

vi.mock('@/lib/repositories/public/pg-lasso-repository', () => ({
  pgLassoRepository: {
    createFollowRequestsBatch: mockCreateFollowRequestsBatch,
    updateFollowRequestStatusBatch: mockUpdateFollowRequestStatusBatch,
  },
}))

vi.mock('@/lib/services/graphNodesService', () => ({
  GraphNodesService: class {
    getNodesByHashes = mockGetNodesByHashes
  },
}))

vi.mock('@/lib/services/rateLimitService', () => ({
  checkRateLimit: mockCheckRateLimit,
  consumeRateLimit: mockConsumeRateLimit,
}))

vi.mock('@/lib/log_utils', () => ({
  default: {
    logError: vi.fn(),
    logWarning: vi.fn(),
    logInfo: vi.fn(),
    logDebug: vi.fn(),
  },
}))

describe('POST /api/migrate/send_follow_lasso', () => {
  beforeEach(() => {
    vi.clearAllMocks()

    mockContext.data = {
      hashes: ['hash-1'],
    }

    mockContext.session = {
      user: {
        id: 'user-123',
        twitter_id: '555666777',
        has_onboarded: true,
        mastodon_instance: null,
        social_accounts: [],
      },
    }

    mockGetNodesByHashes.mockResolvedValue([
      {
        twitter_id: '12345',
        bluesky_handle: 'target.bsky.social',
        mastodon_username: null,
        mastodon_instance: null,
      },
    ])

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
  })

  it('follows Bluesky targets successfully through lasso', async () => {
    const { POST } = await import('@/app/api/migrate/send_follow_lasso/route')

    const response = await POST({} as any) as unknown as MockJsonResponse

    expect(mockGetNodesByHashes).toHaveBeenCalledWith(['hash-1'])
    expect(mockCheckRateLimit).toHaveBeenCalledWith('user-123', 1)
    expect(mockVerifyAndRefreshBlueskyToken).toHaveBeenCalledWith('user-123')
    expect(mockCreateFollowRequestsBatch).toHaveBeenCalledWith([
      { user_id: 'user-123', target_twitter_id: '12345', platform: 'bluesky' },
    ])
    expect(mockBatchFollowOAuth).toHaveBeenCalledWith('did:plc:alice', ['target.bsky.social'])
    expect(mockConsumeRateLimit).toHaveBeenCalledWith('user-123', 1)
    expect(mockUpdateFollowRequestStatusBatch).toHaveBeenCalledWith('user-123', ['12345'], 'bluesky', 'completed')
    expect(response.data).toEqual({
      bluesky: {
        succeeded: 1,
        failed: 0,
        failures: [],
      },
      mastodon: null,
      total: {
        hashesRequested: 1,
        resolved: 1,
        blueskyRequested: 1,
        mastodonRequested: 0,
      },
    })
  })

  it('follows Mastodon targets successfully through lasso using social_accounts session data', async () => {
    mockContext.data = { hashes: ['hash-2'] }
    mockContext.session = {
      user: {
        id: 'user-123',
        twitter_id: '555666777',
        has_onboarded: true,
        mastodon_instance: null,
        social_accounts: [
          { provider: 'mastodon', username: 'home_user', instance: 'https://home.social' },
        ],
      },
    }
    mockGetNodesByHashes.mockResolvedValue([
      {
        twitter_id: '67890',
        bluesky_handle: null,
        mastodon_username: 'target_user',
        mastodon_instance: 'https://target.social',
      },
    ])
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

    const { POST } = await import('@/app/api/migrate/send_follow_lasso/route')
    const response = await POST({} as any) as unknown as MockJsonResponse

    expect(mockVerifyAndRefreshMastodonToken).toHaveBeenCalledWith('user-123')
    expect(mockCreateFollowRequestsBatch).toHaveBeenCalledWith([
      { user_id: 'user-123', target_twitter_id: '67890', platform: 'mastodon' },
    ])
    expect(mockMastodonBatchFollow).toHaveBeenCalledWith(
      'mastodon-access-token',
      'https://home.social',
      [{ username: 'target_user', instance: 'https://target.social', id: undefined }]
    )
    expect(mockUpdateFollowRequestStatusBatch).toHaveBeenCalledWith('user-123', ['67890'], 'mastodon', 'completed')
    expect(response.data).toEqual({
      bluesky: null,
      mastodon: {
        succeeded: 1,
        failed: 0,
        failures: [],
      },
      total: {
        hashesRequested: 1,
        resolved: 1,
        blueskyRequested: 0,
        mastodonRequested: 1,
      },
    })
  })

  it('works for non-onboarded users through lasso', async () => {
    mockContext.session = {
      user: {
        id: 'user-123',
        twitter_id: '555666777',
        has_onboarded: false,
        mastodon_instance: null,
        social_accounts: [],
      },
    }

    const { POST } = await import('@/app/api/migrate/send_follow_lasso/route')
    const response = await POST({} as any) as unknown as MockJsonResponse

    expect(mockCheckRateLimit).toHaveBeenCalledWith('user-123', 1)
    expect(mockCreateFollowRequestsBatch).toHaveBeenCalledWith([
      { user_id: 'user-123', target_twitter_id: '12345', platform: 'bluesky' },
    ])
    expect(mockUpdateFollowRequestStatusBatch).toHaveBeenCalledWith('user-123', ['12345'], 'bluesky', 'completed')
    expect(response.data.bluesky?.succeeded).toBe(1)
  })

  it('returns null results when the user has no connected Bluesky or Mastodon account', async () => {
    mockGetAccountByProviderAndUserId.mockResolvedValue(null)

    const { POST } = await import('@/app/api/migrate/send_follow_lasso/route')
    const response = await POST({} as any) as unknown as MockJsonResponse

    expect(mockVerifyAndRefreshBlueskyToken).not.toHaveBeenCalled()
    expect(mockVerifyAndRefreshMastodonToken).not.toHaveBeenCalled()
    expect(mockBatchFollowOAuth).not.toHaveBeenCalled()
    expect(mockMastodonBatchFollow).not.toHaveBeenCalled()
    expect(response.data).toEqual({
      bluesky: null,
      mastodon: null,
      total: {
        hashesRequested: 1,
        resolved: 1,
        blueskyRequested: 1,
        mastodonRequested: 0,
      },
    })
  })

  it('returns 429 when Bluesky rate limit is exceeded through lasso', async () => {
    mockCheckRateLimit.mockResolvedValue({
      allowed: false,
      reason: 'Hourly rate limit exceeded. 0 follows remaining this hour.',
      maxFollowsAllowed: 0,
      retryAfterSeconds: 120,
    })

    const { POST } = await import('@/app/api/migrate/send_follow_lasso/route')
    const response = await POST({} as any) as unknown as MockJsonResponse

    expect(mockVerifyAndRefreshBlueskyToken).not.toHaveBeenCalled()
    expect(mockBatchFollowOAuth).not.toHaveBeenCalled()
    expect(response.status).toBe(429)
    expect(response.data).toEqual({
      error: 'Rate limit exceeded',
      rateLimited: true,
      reason: 'Hourly rate limit exceeded. 0 follows remaining this hour.',
      maxFollowsAllowed: 0,
      retryAfterSeconds: 120,
    })
  })

  it('returns reauth response when Bluesky token verification fails through lasso', async () => {
    mockVerifyAndRefreshBlueskyToken.mockResolvedValue({ success: false, requiresReauth: true })

    const { POST } = await import('@/app/api/migrate/send_follow_lasso/route')
    const response = await POST({} as any) as unknown as MockJsonResponse

    expect(mockBatchFollowOAuth).not.toHaveBeenCalled()
    expect(response.status).toBe(401)
    expect(response.data).toEqual({
      error: 'Bluesky authentication required',
      requiresReauth: true,
      providers: ['bluesky'],
    })
  })

  it('handles partial Bluesky failures through lasso', async () => {
    mockContext.data = { hashes: ['hash-1', 'hash-2'] }
    mockGetNodesByHashes.mockResolvedValue([
      {
        twitter_id: '12345',
        bluesky_handle: 'success.bsky.social',
        mastodon_username: null,
        mastodon_instance: null,
      },
      {
        twitter_id: '67890',
        bluesky_handle: 'failed.bsky.social',
        mastodon_username: null,
        mastodon_instance: null,
      },
    ])
    mockBatchFollowOAuth.mockResolvedValue({
      succeeded: 1,
      failures: [
        { handle: 'failed.bsky.social', error: 'Already blocked' },
      ],
    })

    const { POST } = await import('@/app/api/migrate/send_follow_lasso/route')
    const response = await POST({} as any) as unknown as MockJsonResponse

    expect(mockConsumeRateLimit).toHaveBeenCalledWith('user-123', 1)
    expect(mockUpdateFollowRequestStatusBatch).toHaveBeenNthCalledWith(1, 'user-123', ['12345'], 'bluesky', 'completed')
    expect(mockUpdateFollowRequestStatusBatch).toHaveBeenNthCalledWith(2, 'user-123', ['67890'], 'bluesky', 'failed', 'Already blocked')
    expect(response.data).toEqual({
      bluesky: {
        succeeded: 1,
        failed: 1,
        failures: [
          { handle: 'failed.bsky.social', error: 'Already blocked' },
        ],
      },
      mastodon: null,
      total: {
        hashesRequested: 2,
        resolved: 2,
        blueskyRequested: 2,
        mastodonRequested: 0,
      },
    })
  })
})
