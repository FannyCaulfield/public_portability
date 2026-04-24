import { NextRequest, NextResponse } from 'next/server';
import { z } from 'zod';
import { redis } from '@/lib/redis';
import { withInternalValidation } from '@/lib/validation/internal-middleware';
import { pgPythonTasksRepository } from '@/lib/repositories/jobs/pg-python-tasks-repository'

// Schéma de validation pour le payload du trigger
const ActivateWaitingTasksSchema = z.object({
  user_id: z.string().uuid(),
  platform: z.enum(['bluesky', 'mastodon']),
  handle: z.string().min(1),
  activated_tasks: z.number().min(1),
  metadata: z.object({
    trigger_operation: z.string(),
    timestamp: z.string(),
    source: z.string().optional()
  }).optional()
});

/**
 * Handler pour ajouter les tâches activées à Redis
 * Appelé par les triggers PostgreSQL via secure_webhook_call
 */
async function handleActivateWaitingTasks(
  request: NextRequest,
  validatedData: z.infer<typeof ActivateWaitingTasksSchema>
): Promise<NextResponse> {
  try {
    const { user_id, platform, handle, activated_tasks, metadata } = validatedData;

    console.log(`🔄 [activate-waiting-tasks] Processing ${activated_tasks} tasks for user ${user_id} on ${platform}`);

    // 1. Récupérer les tâches pending récemment activées pour ce user/platform
    let tasks
    try {
      const sinceIso = new Date(Date.now() - 5 * 60 * 1000).toISOString() // Dernières 5 minutes
      tasks = await pgPythonTasksRepository.getRecentlyActivatedPendingTasks(
        user_id,
        platform,
        sinceIso,
        'test-dm'
      )
    } catch (error) {
      console.error('❌ [activate-waiting-tasks] DB error:', error);
      return NextResponse.json({ error: 'Database error' }, { status: 500 });
    }

    if (!tasks || tasks.length === 0) {
      console.log('⚠️ [activate-waiting-tasks] No pending tasks found to add to Redis');
      return NextResponse.json({ 
        success: true, 
        message: 'No pending tasks found',
        added_to_redis: 0 
      });
    }

    // 2. Ajouter chaque tâche à Redis
    let addedCount = 0;
    const today = new Date().toISOString().split('T')[0]; // YYYY-MM-DD
    const queueKey = `consent_tasks:${today}`;

    for (const task of tasks) {
      try {
        // Vérifier déduplication
        const dedupeKey = `task_dedup:${task.user_id}:${task.platform}:${task.task_type}`;
        const existingTask = await redis.get(dedupeKey);
        
        if (existingTask) {
          console.log(`⏭️ [activate-waiting-tasks] Task ${task.id} already in Redis, skipping`);
          continue;
        }

        const taskData = {
          id: task.id,
          user_id: task.user_id,
          task_type: task.task_type,
          platform: task.platform,
          handle: handle,
          created_at: task.created_at,
          status: 'pending',
          activated_from_waiting: true,
          activation_metadata: metadata
        };

        // Ajouter à la queue Redis
        await redis.lpush(queueKey, JSON.stringify(taskData));
        
        // Marquer comme traité pour déduplication (expire après 1 heure)
        await redis.setex(dedupeKey, 3600, JSON.stringify(taskData));
        
        addedCount++;
        console.log(`✅ [activate-waiting-tasks] Added task ${task.id} to Redis queue`);

      } catch (redisError) {
        console.error(`❌ [activate-waiting-tasks] Redis error for task ${task.id}:`, redisError);
        // Continue avec les autres tâches
      }
    }

    console.log(`🎉 [activate-waiting-tasks] Successfully added ${addedCount}/${tasks.length} tasks to Redis`);

    return NextResponse.json({ 
      success: true,
      message: `Added ${addedCount} tasks to Redis`,
      added_to_redis: addedCount,
      total_tasks: tasks.length,
      platform: platform,
      user_id: user_id
    });

  } catch (error) {
    console.error('❌ [activate-waiting-tasks] Error:', error);
    return NextResponse.json(
      { error: 'Failed to activate waiting tasks' },
      { status: 500 }
    );
  }
}

// Export du POST wrappé avec le middleware de validation interne
export const POST = withInternalValidation(
  ActivateWaitingTasksSchema,
  handleActivateWaitingTasks,
  {
    disableInDev: true,        // Désactivé en dev pour faciliter les tests
    requireSignature: true,    // Signature HMAC requise
    logSecurityEvents: true,   // Logs de sécurité activés
    allowEmptyBody: false      // Body requis pour cet endpoint
  }
);
