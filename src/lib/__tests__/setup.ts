import { beforeAll, afterAll, beforeEach, afterEach, vi } from 'vitest'
import dotenv from 'dotenv'
dotenv.config({ path: '.env.local' })

// Mock next/server avant tout import
vi.mock('next/server', () => ({
  NextRequest: class {},
  NextResponse: class {
    static json(data: any, init?: any) {
      return { data, ...init }
    }
  },
}))

// Mock @/app/auth avant tout import
vi.mock('@/app/auth', () => ({
  auth: vi.fn(async () => null),
  signIn: vi.fn(async () => {}),
  signOut: vi.fn(async () => {}),
  handlers: {},
}))

import { nextAuthPool, publicPool, closePools } from '../database'

/**
 * Configuration globale des tests
 */

beforeAll(async () => {
  // Test de connexion à la base de données
  console.log('🔍 Testing database connection...')
  console.log('Config:', {
    host: process.env.POSTGRES_HOST || 'localhost',
    port: process.env.POSTGRES_PORT || '5432',
    database: process.env.POSTGRES_DB || 'nexus',
    user: process.env.POSTGRES_USER || 'postgres'
  })
  
  try {
    // Test de connexion simple
    const result = await nextAuthPool.query('SELECT current_user, current_database()')
    console.log('✅ Database connection successful!')
    console.log('Connected as:', result.rows[0].current_user)
    console.log('Database:', result.rows[0].current_database)
  } catch (error) {
    console.error('❌ Database connection failed:', error)
    throw new Error(`Cannot connect to database: ${error}`)
  }
})

beforeEach(async () => {
  // Démarrer une transaction pour isoler chaque test
  await nextAuthPool.query('BEGIN')
  await publicPool.query('BEGIN')
})

afterEach(async () => {
  // Rollback de la transaction pour annuler les changements
  await nextAuthPool.query('ROLLBACK')
  await publicPool.query('ROLLBACK')
  
  // Nettoyer les mocks après chaque test
  vi.clearAllMocks()
})

afterAll(async () => {
  // Fermer les connexions à la base de données
  try {
    const { closePools } = await import('../database')
    await closePools()
  } catch (error) {
    // Ignorer les erreurs si les pools n'ont pas été initialisés
  }
})
