import { Pool, PoolClient, QueryResult, QueryResultRow } from 'pg'
import logger from './log_utils'

// ============================================================================
// SINGLETON PATTERN POUR NEXT.JS
// ============================================================================
// En Next.js, les modules peuvent être rechargés (hot reload en dev, ou 
// différentes invocations serverless en prod). Sans globalThis, chaque reload
// crée un NOUVEAU pool sans fermer l'ancien → accumulation de connexions mortes
// → saturation → timeouts.
//
// globalThis persiste entre les reloads du module.
// ============================================================================

// Déclaration du type global pour TypeScript
declare global {
  // eslint-disable-next-line no-var
  var __nextAuthPool: Pool | undefined
  // eslint-disable-next-line no-var
  var __publicPool: Pool | undefined
  // eslint-disable-next-line no-var
  var __consentPool: Pool | undefined
  // eslint-disable-next-line no-var
  var __jobsPool: Pool | undefined
  // eslint-disable-next-line no-var
  var __networkPool: Pool | undefined
  // eslint-disable-next-line no-var
  var __instancesPool: Pool | undefined
  // eslint-disable-next-line no-var
  var __graphPool: Pool | undefined
  // eslint-disable-next-line no-var
  var __cachePool: Pool | undefined
}

function getPoolConfig(max: number) {
  const host = process.env.PGBOUNCER_HOST || process.env.POSTGRES_HOST || 'pgbouncer'
  const port = parseInt(process.env.PGBOUNCER_PORT || process.env.POSTGRES_PORT || '6432')

  return {
    host,
    port,
    database: process.env.POSTGRES_DB || 'nexus',
    user: process.env.POSTGRES_USER || 'postgres',
    password: process.env.POSTGRES_PASSWORD || 'mysecretpassword',
    max,
    idleTimeoutMillis: 10000,
    connectionTimeoutMillis: 30000,
    allowExitOnIdle: true,
    maxLifetimeSeconds: 300,
  }
}

function getNextAuthPoolConfig() {
  return getPoolConfig(10)
}

function getPublicPoolConfig() {
  return getPoolConfig(8)
}

function getConsentPoolConfig() {
  return getPoolConfig(8)
}

function getJobsPoolConfig() {
  return getPoolConfig(8)
}

function getNetworkPoolConfig() {
  return getPoolConfig(8)
}

function getInstancesPoolConfig() {
  return getPoolConfig(6)
}

function getGraphPoolConfig() {
  return getPoolConfig(8)
}

function getCachePoolConfig() {
  return getPoolConfig(6)
}

function attachPoolEvents(pool: Pool, poolName: string) {
  pool.on('error', (err: Error) => {
    console.log('Database', poolName, 'Unexpected error on idle client', undefined, { error: err.message })
  })
  pool.on('connect', () => {
  })
  pool.on('remove', () => {
  })
}

// Création des pools avec singleton via globalThis
function getNextAuthPool(): Pool {
  if (!globalThis.__nextAuthPool) {
    console.log('Database', 'getNextAuthPool', 'Creating new nextAuthPool singleton')
    globalThis.__nextAuthPool = new Pool(getNextAuthPoolConfig())
    attachPoolEvents(globalThis.__nextAuthPool, 'nextAuthPool')
  }
  return globalThis.__nextAuthPool
}

function getPublicPool(): Pool {
  if (!globalThis.__publicPool) {
    console.log('Database', 'getPublicPool', 'Creating new publicPool singleton')
    globalThis.__publicPool = new Pool(getPublicPoolConfig())
    attachPoolEvents(globalThis.__publicPool, 'publicPool')
  }
  return globalThis.__publicPool
}

function getConsentPool(): Pool {
  if (!globalThis.__consentPool) {
    console.log('Database', 'getConsentPool', 'Creating new consentPool singleton')
    globalThis.__consentPool = new Pool(getConsentPoolConfig())
    attachPoolEvents(globalThis.__consentPool, 'consentPool')
  }
  return globalThis.__consentPool
}

function getJobsPool(): Pool {
  if (!globalThis.__jobsPool) {
    console.log('Database', 'getJobsPool', 'Creating new jobsPool singleton')
    globalThis.__jobsPool = new Pool(getJobsPoolConfig())
    attachPoolEvents(globalThis.__jobsPool, 'jobsPool')
  }
  return globalThis.__jobsPool
}

function getNetworkPool(): Pool {
  if (!globalThis.__networkPool) {
    console.log('Database', 'getNetworkPool', 'Creating new networkPool singleton')
    globalThis.__networkPool = new Pool(getNetworkPoolConfig())
    attachPoolEvents(globalThis.__networkPool, 'networkPool')
  }
  return globalThis.__networkPool
}

function getInstancesPool(): Pool {
  if (!globalThis.__instancesPool) {
    console.log('Database', 'getInstancesPool', 'Creating new instancesPool singleton')
    globalThis.__instancesPool = new Pool(getInstancesPoolConfig())
    attachPoolEvents(globalThis.__instancesPool, 'instancesPool')
  }
  return globalThis.__instancesPool
}

function getGraphPool(): Pool {
  if (!globalThis.__graphPool) {
    console.log('Database', 'getGraphPool', 'Creating new graphPool singleton')
    globalThis.__graphPool = new Pool(getGraphPoolConfig())
    attachPoolEvents(globalThis.__graphPool, 'graphPool')
  }
  return globalThis.__graphPool
}

function getCachePool(): Pool {
  if (!globalThis.__cachePool) {
    console.log('Database', 'getCachePool', 'Creating new cachePool singleton')
    globalThis.__cachePool = new Pool(getCachePoolConfig())
    attachPoolEvents(globalThis.__cachePool, 'cachePool')
  }
  return globalThis.__cachePool
}

// Export des pools via getters (pour compatibilité avec le code existant)
export const nextAuthPool = new Proxy({} as Pool, {
  get(_, prop) {
    const pool = getNextAuthPool()
    const value = (pool as any)[prop]
    // Bind les méthodes au pool pour conserver le contexte
    return typeof value === 'function' ? value.bind(pool) : value
  }
})

export const publicPool = new Proxy({} as Pool, {
  get(_, prop) {
    const pool = getPublicPool()
    const value = (pool as any)[prop]
    return typeof value === 'function' ? value.bind(pool) : value
  }
})

export const consentPool = new Proxy({} as Pool, {
  get(_, prop) {
    const pool = getConsentPool()
    const value = (pool as any)[prop]
    return typeof value === 'function' ? value.bind(pool) : value
  }
})

export const jobsPool = new Proxy({} as Pool, {
  get(_, prop) {
    const pool = getJobsPool()
    const value = (pool as any)[prop]
    return typeof value === 'function' ? value.bind(pool) : value
  }
})

export const networkPool = new Proxy({} as Pool, {
  get(_, prop) {
    const pool = getNetworkPool()
    const value = (pool as any)[prop]
    return typeof value === 'function' ? value.bind(pool) : value
  }
})

export const instancesPool = new Proxy({} as Pool, {
  get(_, prop) {
    const pool = getInstancesPool()
    const value = (pool as any)[prop]
    return typeof value === 'function' ? value.bind(pool) : value
  }
})

export const graphPool = new Proxy({} as Pool, {
  get(_, prop) {
    const pool = getGraphPool()
    const value = (pool as any)[prop]
    return typeof value === 'function' ? value.bind(pool) : value
  }
})

export const cachePool = new Proxy({} as Pool, {
  get(_, prop) {
    const pool = getCachePool()
    const value = (pool as any)[prop]
    return typeof value === 'function' ? value.bind(pool) : value
  }
})

async function executeQuery<T extends QueryResultRow = any>(
  pool: Pool,
  searchPath: string,
  logName: string,
  text: string,
  params?: any[]
): Promise<QueryResult<T>> {
  const client = await pool.connect()
  try {
    await client.query(`SET search_path TO ${searchPath}`)
    return await client.query<T>(text, params)
  } catch (error) {
    console.log('Database', logName, 'Query failed', undefined, {
      text,
      params,
      error,
    })
    throw error
  } finally {
    client.release()
  }
}

async function executeTransaction<T>(
  pool: Pool,
  searchPath: string,
  logName: string,
  callback: (client: PoolClient) => Promise<T>
): Promise<T> {
  const client = await pool.connect()
  try {
    await client.query(`SET search_path TO ${searchPath}`)
    await client.query('BEGIN')
    const result = await callback(client)
    await client.query('COMMIT')
    return result
  } catch (error) {
    await client.query('ROLLBACK')
    console.log('Database', logName, 'Transaction failed and rolled back', undefined, { error })
    throw error
  } finally {
    client.release()
  }
}

// Helper pour exécuter une query sur le pool next-auth
export async function queryNextAuth<T extends QueryResultRow = any>(
  text: string,
  params?: any[]
): Promise<QueryResult<T>> {
  return executeQuery<T>(nextAuthPool, '"next-auth", public', 'queryNextAuth', text, params)
}

// Helper pour exécuter une query sur le pool public
export async function queryPublic<T extends QueryResultRow = any>(
  text: string,
  params?: any[]
): Promise<QueryResult<T>> {
  return executeQuery<T>(publicPool, 'public', 'queryPublic', text, params)
}

export async function queryConsent<T extends QueryResultRow = any>(
  text: string,
  params?: any[]
): Promise<QueryResult<T>> {
  return executeQuery<T>(consentPool, 'consent, public', 'queryConsent', text, params)
}

export async function queryJobs<T extends QueryResultRow = any>(
  text: string,
  params?: any[]
): Promise<QueryResult<T>> {
  return executeQuery<T>(jobsPool, 'jobs, public', 'queryJobs', text, params)
}

export async function queryNetwork<T extends QueryResultRow = any>(
  text: string,
  params?: any[]
): Promise<QueryResult<T>> {
  return executeQuery<T>(networkPool, 'network, graph, public, "next-auth"', 'queryNetwork', text, params)
}

export async function queryInstances<T extends QueryResultRow = any>(
  text: string,
  params?: any[]
): Promise<QueryResult<T>> {
  return executeQuery<T>(instancesPool, 'instances, public', 'queryInstances', text, params)
}

export async function queryGraph<T extends QueryResultRow = any>(
  text: string,
  params?: any[]
): Promise<QueryResult<T>> {
  return executeQuery<T>(graphPool, 'graph, consent, network, public, "next-auth"', 'queryGraph', text, params)
}

export async function queryCache<T extends QueryResultRow = any>(
  text: string,
  params?: any[]
): Promise<QueryResult<T>> {
  return executeQuery<T>(cachePool, 'cache, public', 'queryCache', text, params)
}

// Helper pour les transactions sur next-auth
export async function transactionNextAuth<T>(
  callback: (client: PoolClient) => Promise<T>
): Promise<T> {
  return executeTransaction(nextAuthPool, '"next-auth", public', 'transactionNextAuth', callback)
}

// Helper pour les transactions sur public
export async function transactionPublic<T>(
  callback: (client: PoolClient) => Promise<T>
): Promise<T> {
  return executeTransaction(publicPool, 'public', 'transactionPublic', callback)
}

export async function transactionConsent<T>(
  callback: (client: PoolClient) => Promise<T>
): Promise<T> {
  return executeTransaction(consentPool, 'consent, public', 'transactionConsent', callback)
}

export async function transactionJobs<T>(
  callback: (client: PoolClient) => Promise<T>
): Promise<T> {
  return executeTransaction(jobsPool, 'jobs, public', 'transactionJobs', callback)
}

export async function transactionNetwork<T>(
  callback: (client: PoolClient) => Promise<T>
): Promise<T> {
  return executeTransaction(networkPool, 'network, graph, public, "next-auth"', 'transactionNetwork', callback)
}

export async function transactionInstances<T>(
  callback: (client: PoolClient) => Promise<T>
): Promise<T> {
  return executeTransaction(instancesPool, 'instances, public', 'transactionInstances', callback)
}

export async function transactionGraph<T>(
  callback: (client: PoolClient) => Promise<T>
): Promise<T> {
  return executeTransaction(graphPool, 'graph, consent, network, public, "next-auth"', 'transactionGraph', callback)
}

export async function transactionCache<T>(
  callback: (client: PoolClient) => Promise<T>
): Promise<T> {
  return executeTransaction(cachePool, 'cache, public', 'transactionCache', callback)
}

// Fonction pour fermer les pools (utile pour les tests et le shutdown)
export async function closePools(): Promise<void> {
  if (globalThis.__nextAuthPool) {
    await globalThis.__nextAuthPool.end()
    globalThis.__nextAuthPool = undefined
  }
  if (globalThis.__publicPool) {
    await globalThis.__publicPool.end()
    globalThis.__publicPool = undefined
  }
  if (globalThis.__consentPool) {
    await globalThis.__consentPool.end()
    globalThis.__consentPool = undefined
  }
  if (globalThis.__jobsPool) {
    await globalThis.__jobsPool.end()
    globalThis.__jobsPool = undefined
  }
  if (globalThis.__networkPool) {
    await globalThis.__networkPool.end()
    globalThis.__networkPool = undefined
  }
  if (globalThis.__instancesPool) {
    await globalThis.__instancesPool.end()
    globalThis.__instancesPool = undefined
  }
  if (globalThis.__graphPool) {
    await globalThis.__graphPool.end()
    globalThis.__graphPool = undefined
  }
  if (globalThis.__cachePool) {
    await globalThis.__cachePool.end()
    globalThis.__cachePool = undefined
  }
  console.log('Database', 'closePools', 'All database pools closed')
}

// Fonction pour vérifier la connexion
export async function checkConnection(): Promise<boolean> {
  try {
    await queryNextAuth('SELECT 1')
    await queryPublic('SELECT 1')
    await queryConsent('SELECT 1')
    await queryJobs('SELECT 1')
    await queryNetwork('SELECT 1')
    await queryInstances('SELECT 1')
    await queryGraph('SELECT 1')
    await queryCache('SELECT 1')
    console.log('Database', 'checkConnection', 'Database connection successful')
    return true
  } catch (error) {
    console.log('Database', 'checkConnection', 'Database connection failed', undefined, { error })
    return false
  }
}
