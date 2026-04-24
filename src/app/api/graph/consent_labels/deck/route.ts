import { NextResponse } from 'next/server';
import { auth } from '@/app/auth';
import { pgGraphNodesRepository } from '@/lib/repositories/graph/pg-graph-nodes-repository';
import logger from '@/lib/log_utils';
import { withPublicValidation } from '@/lib/validation/middleware';
import { z } from 'zod';

export const dynamic = 'force-dynamic';
export const revalidate = 60;

const EmptySchema = z.object({}).strict();

type RawDeckLabelRow = {
  node_id: string;
  display_label: string;
  consent_id: string | null;
  consent_level: string | null;
  visibility_reason: string | null;
  x: number;
  y: number;
  follower_level: number | null;
  is_public_account: boolean | null;
};

type DeckPriorityTier = 'consent_record' | 'public_consent' | 'network_visible' | 'secondary';

function coordHash(x: number, y: number): string {
  return `${x.toFixed(6)}_${y.toFixed(6)}`;
}

function computePriority(row: RawDeckLabelRow): number {
  let priority = 0;

  if (row.consent_id) priority += 1000;
  if (row.consent_level === 'all_consent') priority += 200;
  if (row.follower_level === 1) priority += 120;
  else if (row.follower_level === 2) priority += 60;
  if (row.is_public_account) priority += 40;

  return priority;
}

function computePriorityTier(row: RawDeckLabelRow): DeckPriorityTier {
  if (row.consent_id) return 'consent_record';
  if (row.consent_level === 'all_consent') return 'public_consent';
  if ((row.follower_level ?? 0) > 0) return 'network_visible';
  return 'secondary';
}

async function getDeckConsentLabelsHandler() {
  try {
    const session = await auth();
    const userId = session?.user?.id;
    const twitterId = session?.user?.twitter_id;

    const rows: RawDeckLabelRow[] = userId
      ? await pgGraphNodesRepository.getVisibleDeckLabelsForUser(userId, twitterId)
      : await pgGraphNodesRepository.getPublicDeckConsentLabels();

    if (rows.length === 0) {
      return NextResponse.json({
        success: true,
        authenticated: !!userId,
        count: 0,
        labels: [],
      });
    }

    const deduped = new Map<string, {
      coord_hash: string;
      x: number;
      y: number;
      text: string;
      priority: number;
      priority_tier: DeckPriorityTier;
      has_consent_record: boolean;
      consent_id: string | null;
      consent_level: string | null;
      visibility_reason: string | null;
      follower_level: number | null;
      is_public_account: boolean | null;
    }>();

    for (const row of rows) {
      const candidate = {
        coord_hash: coordHash(row.x, row.y),
        x: row.x,
        y: row.y,
        text: row.display_label,
        priority: computePriority(row),
        priority_tier: computePriorityTier(row),
        has_consent_record: row.consent_id !== null,
        consent_id: row.consent_id,
        consent_level: row.consent_level,
        visibility_reason: row.visibility_reason,
        follower_level: row.follower_level,
        is_public_account: row.is_public_account,
      };

      const existing = deduped.get(candidate.coord_hash);
      if (!existing || candidate.priority > existing.priority) {
        deduped.set(candidate.coord_hash, candidate);
      }
    }

    const labels = [...deduped.values()].sort((a, b) => {
      if (b.priority !== a.priority) return b.priority - a.priority;
      return a.text.localeCompare(b.text);
    });

    logger.logDebug(
      'API',
      'GET /api/graph/consent_labels/deck',
      `Returning ${labels.length} prioritized deck labels (auth: ${!!userId})`,
      'system'
    );

    return NextResponse.json({
      success: true,
      authenticated: !!userId,
      count: labels.length,
      labels,
    });
  } catch (error) {
    const err = error instanceof Error ? error : new Error(String(error));
    logger.logError('API', 'GET /api/graph/consent_labels/deck', err, 'system', {
      context: 'Error fetching deck consent labels',
    });
    return NextResponse.json({ error: 'Internal server error' }, { status: 500 });
  }
}

export const GET = withPublicValidation(
  EmptySchema,
  getDeckConsentLabelsHandler,
  {
    applySecurityChecks: false,
    skipRateLimit: false,
  }
);
