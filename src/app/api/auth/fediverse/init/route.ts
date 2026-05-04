import crypto from 'crypto'
import { NextRequest, NextResponse } from 'next/server'
import { z } from 'zod'
import logger from '@/lib/log_utils'
import { withPublicValidation } from '@/lib/validation/middleware'
import { pgFediverseInstanceRepository } from '@/lib/repositories/auth/pg-fediverse-instance-repository'

export const runtime = 'nodejs'

const BodySchema = z.object({
  handle: z.string().min(1).max(255),
  redirect: z.boolean().optional(),
  userId: z.string().uuid().optional(),
  redirectTo: z.string().max(512).optional(),
}).passthrough()

function base64url(input: Buffer): string {
  return input.toString('base64').replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/g, '')
}

function generatePkce(): { codeVerifier: string; codeChallenge: string } {
  const codeVerifier = base64url(crypto.randomBytes(64))
  const hash = crypto.createHash('sha256').update(codeVerifier).digest()
  return {
    codeVerifier,
    codeChallenge: base64url(hash),
  }
}

function normalizeHost(raw: string): string {
  const trimmed = raw.trim()
  const candidate = trimmed.startsWith('@') ? trimmed.split('@').filter(Boolean).pop() ?? '' : trimmed
  const withoutProtocol = candidate.replace(/^https?:\/\//i, '')
  return withoutProtocol.replace(/\/.*$/, '').toLowerCase()
}

function getDefaultScopes(instanceType: 'mastodon' | 'misskey' | 'pixelfed'): string[] {
  return instanceType === 'misskey' ? ['read:account'] : ['read']
}

async function detectInstanceType(host: string): Promise<'mastodon' | 'misskey' | 'pixelfed'> {
  try {
    const nodeInfoResp = await fetch(`https://${host}/.well-known/nodeinfo`, {
      headers: { Accept: 'application/json' },
      signal: AbortSignal.timeout(8000),
    })

    if (!nodeInfoResp.ok) {
      return 'mastodon'
    }

    const nodeInfoData = await nodeInfoResp.json().catch(() => null) as any
    const href = Array.isArray(nodeInfoData?.links)
      ? nodeInfoData.links.find((link: any) => typeof link?.href === 'string')?.href
      : null

    if (!href) {
      return 'mastodon'
    }

    const softwareResp = await fetch(href, {
      headers: { Accept: 'application/json' },
      signal: AbortSignal.timeout(8000),
    })

    if (!softwareResp.ok) {
      return 'mastodon'
    }

    const softwareData = await softwareResp.json().catch(() => null) as any
    const softwareName = String(softwareData?.software?.name ?? '').toLowerCase()

    if (softwareName.includes('misskey')) {
      return 'misskey'
    }

    if (softwareName.includes('pixelfed')) {
      return 'pixelfed'
    }

    return 'mastodon'
  } catch {
    return 'mastodon'
  }
}

async function createOAuthApp(host: string, instanceType: 'mastodon' | 'misskey' | 'pixelfed', redirectUri: string) {
  const isMisskey = instanceType === 'misskey'
  const endpoint = isMisskey ? `https://${host}/api/app/create` : `https://${host}/api/v1/apps`
  const scopes = getDefaultScopes(instanceType)

  const payload = isMisskey
    ? {
        name: 'OpenPortability',
        description: 'OpenPortability',
        permission: scopes,
        callbackUrl: redirectUri,
      }
    : {
        client_name: 'OpenPortability',
        redirect_uris: redirectUri,
        scopes: scopes.join(' '),
        website: 'https://app.beta.v2.helloquitx.com/',
      }

  const response = await fetch(endpoint, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Accept: 'application/json',
    },
    body: JSON.stringify(payload),
    signal: AbortSignal.timeout(10000),
  })

  if (!response.ok) {
    const text = await response.text().catch(() => 'Unknown error')
    throw new Error(`OAuth app creation failed (${response.status}): ${text}`)
  }

  const json = await response.json() as any

  const clientId = json.client_id ?? json.id
  const clientSecret = json.client_secret ?? json.secret ?? null

  if (!clientId) {
    throw new Error('Missing client_id from Fediverse instance')
  }

  return {
    host,
    instance_type: instanceType,
    client_id: String(clientId),
    client_secret: clientSecret ? String(clientSecret) : null,
    authorization_endpoint: isMisskey ? `https://${host}/oauth/authorize` : `https://${host}/oauth/authorize`,
    token_endpoint: isMisskey ? `https://${host}/api/auth/session/userkey` : `https://${host}/oauth/token`,
    userinfo_endpoint: isMisskey ? `https://${host}/api/i` : `https://${host}/api/v1/accounts/verify_credentials`,
    scopes,
  }
}

export const POST = withPublicValidation(
  BodySchema,
  async (request: NextRequest, data: z.infer<typeof BodySchema>) => {
    try {
      const host = normalizeHost(data.handle)
      if (!host || !host.includes('.')) {
        return NextResponse.json({ error: 'Invalid handle or instance' }, { status: 400 })
      }

      const callbackBase = process.env.NEXTAUTH_URL || request.nextUrl.origin
      const redirectUri = `${callbackBase}/api/auth/fediverse/callback`
      const instanceType = await detectInstanceType(host)
      const existing = await pgFediverseInstanceRepository.getInstance(host)
      const instance = existing ?? await pgFediverseInstanceRepository.upsertInstance(
        await createOAuthApp(host, instanceType, redirectUri)
      )

      const { codeVerifier, codeChallenge } = generatePkce()
      const state = crypto.randomUUID()
      const scopes = getDefaultScopes(instance.instance_type as 'mastodon' | 'misskey' | 'pixelfed')

      const authParams = new URLSearchParams({
        client_id: instance.client_id,
        response_type: 'code',
        redirect_uri: redirectUri,
        scope: instance.instance_type === 'misskey' ? scopes.join(' ') : scopes.join(' '),
        code_challenge: codeChallenge,
        code_challenge_method: 'S256',
        state,
      })

      const authorizationEndpoint = instance.authorization_endpoint || `https://${host}/oauth/authorize`
      const authorizationUrl = `${authorizationEndpoint}?${authParams.toString()}`

      const response = data.redirect
        ? NextResponse.redirect(authorizationUrl)
        : NextResponse.json({ authorizationUrl, host, type: instance.instance_type, scopes })

      response.cookies.set('fediverse_oauth', JSON.stringify({
        host,
        state,
        codeVerifier,
        userId: data.userId ?? null,
        redirectTo: data.redirectTo ?? null,
        scopes,
      }), {
        httpOnly: true,
        sameSite: 'lax',
        secure: process.env.NODE_ENV === 'production',
        path: '/',
        maxAge: 60 * 15,
      })

      return response
    } catch (error) {
      const err = error instanceof Error ? error.message : String(error)
      logger.logError('Auth', 'fediverse.init', err)
      return NextResponse.json({ error: 'Failed to initiate Fediverse OAuth' }, { status: 500 })
    }
  },
  {
    applySecurityChecks: true,
    customRateLimit: { identifier: 'ip', windowMs: 60_000, maxRequests: 60 },
  }
)
