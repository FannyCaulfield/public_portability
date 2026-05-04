import { NextRequest, NextResponse } from 'next/server'
import { z } from 'zod'
import logger from '@/lib/log_utils'
import { withPublicValidation } from '@/lib/validation/middleware'
import { pgFediverseInstanceRepository } from '@/lib/repositories/auth/pg-fediverse-instance-repository'
import { pgUserRepository } from '@/lib/repositories/auth/pg-user-repository'
import { pgAccountRepository } from '@/lib/repositories/auth/pg-account-repository'
import { pgSocialAccountRepository } from '@/lib/repositories/auth/pg-social-account-repository'
import { auth, signIn } from '@/app/auth'

export const runtime = 'nodejs'

const CallbackQuerySchema = z.object({
  code: z.string().max(2048).optional(),
  state: z.string().max(512).optional(),
  error: z.string().max(256).optional(),
}).passthrough()

interface FediverseCookiePayload {
  host: string
  state: string
  codeVerifier: string
  userId: string | null
  redirectTo: string | null
  scopes: string[]
}

function parseCookiePayload(request: NextRequest): FediverseCookiePayload | null {
  const cookie = request.cookies.get('fediverse_oauth')?.value
  if (!cookie) return null

  try {
    return JSON.parse(cookie) as FediverseCookiePayload
  } catch {
    return null
  }
}

function resolveRedirect(baseUrl: string, redirectTo?: string | null): string {
  if (!redirectTo) return baseUrl
  if (redirectTo.startsWith('http')) return redirectTo
  return redirectTo.startsWith('/') ? `${baseUrl}${redirectTo}` : baseUrl
}

function safeOrigin(url: string, fallbackHost: string): string {
  try {
    return new URL(url).origin
  } catch {
    return `https://${fallbackHost}`
  }
}

async function exchangeToken(options: {
  tokenEndpoint: string
  code: string
  clientId: string
  clientSecret?: string | null
  redirectUri: string
  codeVerifier: string
  isMisskey: boolean
}) {
  const payload = {
    grant_type: 'authorization_code',
    code: options.code,
    client_id: options.clientId,
    redirect_uri: options.redirectUri,
    code_verifier: options.codeVerifier,
    ...(options.clientSecret ? { client_secret: options.clientSecret } : {}),
  }

  const response = await fetch(options.tokenEndpoint, {
    method: 'POST',
    headers: {
      'Content-Type': options.isMisskey ? 'application/json' : 'application/x-www-form-urlencoded',
      Accept: 'application/json',
    },
    body: options.isMisskey ? JSON.stringify(payload) : new URLSearchParams(payload).toString(),
    signal: AbortSignal.timeout(10000),
  })

  if (!response.ok) {
    const text = await response.text().catch(() => 'Unknown error')
    throw new Error(`Token exchange failed (${response.status}): ${text}`)
  }

  return response.json() as Promise<{
    access_token?: string
    refresh_token?: string
    token_type?: string
    scope?: string
    expires_in?: number
    token?: string
  }>
}

async function fetchProfile(options: {
  instanceType: string
  userinfoEndpoint: string
  accessToken: string
  host: string
}) {
  if (options.instanceType === 'misskey') {
    const response = await fetch(options.userinfoEndpoint, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ i: options.accessToken }),
      signal: AbortSignal.timeout(10000),
    })

    if (!response.ok) {
      const text = await response.text().catch(() => 'Unknown error')
      throw new Error(`Misskey userinfo failed (${response.status}): ${text}`)
    }

    const data = await response.json() as any
    return {
      id: String(data.id),
      username: data.username ? String(data.username) : String(data.id),
      displayName: data.name ? String(data.name) : (data.username ? String(data.username) : String(data.id)),
      avatar: data.avatarUrl ? String(data.avatarUrl) : null,
      url: `https://${options.host}/@${data.username}`,
    }
  }

  const response = await fetch(options.userinfoEndpoint, {
    headers: { Authorization: `Bearer ${options.accessToken}` },
    signal: AbortSignal.timeout(10000),
  })

  if (!response.ok) {
    const text = await response.text().catch(() => 'Unknown error')
    throw new Error(`Fediverse userinfo failed (${response.status}): ${text}`)
  }

  const data = await response.json() as any
  return {
    id: String(data.id),
    username: data.username ? String(data.username) : String(data.id),
    displayName: data.display_name ? String(data.display_name) : (data.username ? String(data.username) : String(data.id)),
    avatar: data.avatar ? String(data.avatar) : null,
    url: data.url ? String(data.url) : `https://${options.host}/@${data.username}`,
  }
}

async function callbackHandler(request: NextRequest) {
  const code = request.nextUrl.searchParams.get('code')
  const state = request.nextUrl.searchParams.get('state')
  const error = request.nextUrl.searchParams.get('error')

  if (error) {
    return NextResponse.json({ error }, { status: 400 })
  }

  if (!code || !state) {
    return NextResponse.json({ error: 'Missing OAuth callback parameters' }, { status: 400 })
  }

  const cookiePayload = parseCookiePayload(request)
  if (!cookiePayload || cookiePayload.state !== state) {
    return NextResponse.json({ error: 'Invalid OAuth state' }, { status: 400 })
  }

  const baseUrl = process.env.NEXTAUTH_URL || request.nextUrl.origin
  const redirectUri = `${baseUrl}/api/auth/fediverse/callback`

  try {
    const instance = await pgFediverseInstanceRepository.getInstance(cookiePayload.host)
    if (!instance) {
      return NextResponse.json({ error: 'Unknown Fediverse instance' }, { status: 400 })
    }

    if (!instance.token_endpoint) {
      return NextResponse.json({ error: 'Missing token endpoint' }, { status: 400 })
    }

    const tokenResponse = await exchangeToken({
      tokenEndpoint: instance.token_endpoint,
      code,
      clientId: instance.client_id,
      clientSecret: instance.client_secret,
      redirectUri,
      codeVerifier: cookiePayload.codeVerifier,
      isMisskey: instance.instance_type === 'misskey',
    })

    const accessToken = tokenResponse.access_token ?? tokenResponse.token
    if (!accessToken) {
      return NextResponse.json({ error: 'Missing access token' }, { status: 400 })
    }

    const userinfoEndpoint = instance.userinfo_endpoint || `https://${cookiePayload.host}/api/v1/accounts/verify_credentials`
    const profile = await fetchProfile({
      instanceType: instance.instance_type,
      userinfoEndpoint,
      accessToken,
      host: cookiePayload.host,
    })

    const currentSession = await auth()
    const socialInstance = safeOrigin(profile.url, cookiePayload.host)
    let userId = cookiePayload.userId || currentSession?.user?.id || undefined

    const existingUser = await pgUserRepository.getUserByProviderId('mastodon', profile.id, socialInstance)

    if (existingUser && userId && existingUser.id !== userId) {
      const errorUrl = new URL('/auth/error', baseUrl)
      errorUrl.searchParams.set('error', 'FediverseAccountAlreadyLinked')
      return NextResponse.redirect(errorUrl)
    }

    if (existingUser) {
      userId = existingUser.id
      await pgUserRepository.updateUser(userId, {
        name: profile.displayName,
        image: profile.avatar,
      })
    } else if (userId) {
      await pgUserRepository.updateUser(userId, {
        name: profile.displayName,
        image: profile.avatar,
      })
    } else {
      const newUser = await pgUserRepository.createUser({
        name: profile.displayName,
        email: null,
        image: profile.avatar,
        has_onboarded: false,
        hqx_newsletter: false,
        oep_accepted: false,
        have_seen_newsletter: false,
        research_accepted: false,
        automatic_reconnect: false,
      })
      userId = newUser.id
    }

    await pgSocialAccountRepository.upsertSocialAccount({
      user_id: userId!,
      provider: 'mastodon',
      provider_account_id: profile.id,
      username: profile.username,
      instance: socialInstance,
      email: null,
      is_primary: true,
      last_seen_at: new Date(),
    })

    await pgAccountRepository.upsertAccount({
      user_id: userId!,
      type: 'oauth',
      provider: 'mastodon',
      provider_account_id: profile.id,
      access_token: accessToken,
      refresh_token: tokenResponse.refresh_token ?? null,
      scope: tokenResponse.scope ?? cookiePayload.scopes.join(' '),
      token_type: tokenResponse.token_type ?? null,
    })

    const redirectTo = resolveRedirect(baseUrl, cookiePayload.redirectTo)

    return await signIn('fediverse', {
      id: userId!,
      provider: 'fediverse',
      mastodon_id: profile.id,
      mastodon_username: profile.username,
      mastodon_image: profile.avatar,
      mastodon_instance: socialInstance,
      has_onboarded: false,
      hqx_newsletter: false,
      oep_accepted: false,
      research_accepted: false,
      have_seen_newsletter: false,
      automatic_reconnect: false,
      name: profile.displayName,
      email: null,
      redirectTo,
    })
  } catch (error) {
    const err = error instanceof Error ? error.message : String(error)
    const isNextRedirect = typeof error === 'object' && error !== null && 'digest' in error && String((error as any).digest).startsWith('NEXT_REDIRECT')

    if (isNextRedirect) {
      throw error
    }

    logger.logError('Auth', 'fediverse.callback', err)
    return NextResponse.json({ error: 'Failed to complete Fediverse OAuth' }, { status: 500 })
  }
}

export const GET = withPublicValidation(
  z.object({}).passthrough(),
  async (request: NextRequest) => callbackHandler(request),
  {
    validateQueryParams: true,
    queryParamsSchema: CallbackQuerySchema,
    applySecurityChecks: true,
    excludeQueryParamsFromSecurity: ['state'],
    customRateLimit: { identifier: 'ip', windowMs: 60_000, maxRequests: 120 },
  }
)
