 import { NextResponse } from "next/server"
import logger from '@/lib/log_utils'
import { withValidation } from "@/lib/validation/middleware"
import { z } from "zod"
import { pgAccountRepository } from '@/lib/repositories/auth/pg-account-repository'
import { pgSocialAccountRepository } from '@/lib/repositories/auth/pg-social-account-repository'
import {
  cancelQueuedNetworkSyncJobs,
  deleteNetworkEdgesByUserId,
} from '@/lib/repositories/jobs/networkCleanupRepository'

// Classe d'erreur pour la déliaison de compte (interne au module)
class UnlinkError extends Error {
  constructor(
    message: string,
    public code: 'LAST_ACCOUNT' | 'NOT_FOUND' | 'NOT_LINKED' | 'DATABASE_ERROR',
    public status: number = 400
  ) {
    super(message)
    this.name = 'UnlinkError'
  }
}

// Schéma de validation pour la requête de déliaison de compte
const UnlinkSchema = z.object({
  provider: z.string().refine(
    (val) => ['twitter', 'bluesky', 'mastodon'].includes(val),
    { message: "Provider must be one of: twitter, bluesky, mastodon" }
  )
}).strict()

// Type pour les données validées
type UnlinkRequest = z.infer<typeof UnlinkSchema>

type UnlinkProvider = 'twitter' | 'bluesky' | 'mastodon'

async function unlinkHandler(_req: Request, data: UnlinkRequest, session: any) {
  try {
    if (!session?.user?.id) {
      logger.logWarning('API', 'POST /api/auth/unlink', 'Unauthorized access attempt')
      return NextResponse.json({ error: 'Unauthorized' }, { status: 401 })
    }
    
    const userId = session.user.id
    const provider = data.provider as UnlinkProvider

    const socialAccounts = await pgSocialAccountRepository.getSocialAccountsByUserId(userId)
    const matchingSocialAccount = socialAccounts.find((account) => account.provider === provider)
    
    // Vérifier si le compte est lié (type-safe)
    if (!matchingSocialAccount?.provider_account_id) {
      logger.logWarning('API', 'POST /api/auth/unlink', 'Account not found for provider', userId, { provider })
      return NextResponse.json({ 
        error: 'Account not found', 
        code: 'NOT_LINKED' 
      }, { status: 400 })
    }
    
    // Vérifier si c'est le dernier compte lié
    const linkedProviders = socialAccounts.length
    
    if (linkedProviders <= 1) {
      logger.logWarning('API', 'POST /api/auth/unlink', 'Cannot unlink last account', userId, { provider })
      throw new UnlinkError("Cannot unlink last account", "LAST_ACCOUNT", 400)
    }

    try {
      if (provider === 'bluesky') {
        await cancelQueuedNetworkSyncJobs(userId, 'bluesky')
        await deleteNetworkEdgesByUserId(userId, 'bluesky')
      }

      if (provider === 'mastodon') {
        await cancelQueuedNetworkSyncJobs(userId, 'mastodon')
        await deleteNetworkEdgesByUserId(userId, 'mastodon')
      }
    } catch (cleanupErr) {
      logger.logError('API', 'POST /api/auth/unlink', 'Error cleaning network sync data during unlink', userId, {
        provider,
        error: cleanupErr,
      })
      throw new UnlinkError("Database error", "DATABASE_ERROR", 500)
    }
    
    // Vérifier si c'est une instance Piaille pour Mastodon
    const isPiaille = provider === 'mastodon' && matchingSocialAccount.instance === 'piaille.fr'
    
    // Supprimer le compte de la table accounts (via repo)
    try {
      const providerForAccounts = isPiaille ? 'piaille' : provider
      const account = await pgAccountRepository.getProviderAccount(providerForAccounts, userId)
      if (account) {
        await pgAccountRepository.deleteAccount(providerForAccounts, account.provider_account_id)
      }
      await pgSocialAccountRepository.deleteSocialAccount(
        userId,
        provider,
        matchingSocialAccount.provider_account_id,
        matchingSocialAccount.instance
      )
    } catch (deleteErr) {
      logger.logError('API', 'POST /api/auth/unlink', 'Error deleting account', userId, { provider, error: deleteErr })
      // Continuer même en cas d'erreur car le compte peut ne pas exister dans accounts
    }
    
    return NextResponse.json({ success: true })
  } catch (error) {
    const userId = session?.user?.id || 'unknown'
    const err = error instanceof Error ? error : new Error(String(error))
    logger.logError('API', 'POST /api/auth/unlink', err, userId, {
      name: err.name,
      message: err.message
    })
    if (error instanceof UnlinkError) {
      return NextResponse.json(
        { error: error.message, code: error.code },
        { status: error.status }
      )
    }
    
    return NextResponse.json(
      { error: 'Failed to unlink account' },
      { status: 500 }
    )
  }
}

// Configuration du middleware de validation
export const POST = withValidation(
  UnlinkSchema,
  unlinkHandler,
  {
    requireAuth: true,
    applySecurityChecks: true,
    skipRateLimit: false
  }
)