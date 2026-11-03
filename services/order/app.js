const fs = require('fs');
const http = require('http');
const { randomUUID } = require('crypto');
const { Pool } = require('pg');
const Redis = require('ioredis');
const { GetQueueAttributesCommand, GetQueueUrlCommand, SendMessageCommand, SQSClient } = require('@aws-sdk/client-sqs');
const { ORDER_CACHE_KEY, createOrderWithOutbox, dispatchOutboxBatch, mapOrderRow } = require('./core');

const config = {
  port: Number(process.env.PORT || 8080),
  region: process.env.AWS_REGION || 'ap-southeast-1',
  queueName: process.env.SQS_QUEUE_NAME || '',
  queueUrl: process.env.SQS_QUEUE_URL || '',
  dbHost: process.env.DB_HOST || '',
  dbPort: Number(process.env.DB_PORT || 5432),
  dbName: process.env.DB_NAME || '',
  dbUser: process.env.DB_USER || '',
  dbPassword: process.env.DB_PASSWORD || '',
  redisHost: process.env.REDIS_HOST || '',
  redisPort: Number(process.env.REDIS_PORT || 6379),
  redisAuth: process.env.REDIS_AUTH || '',
  outboxPollMs: Number(process.env.OUTBOX_POLL_MS || 1000)
};

function optionalCa(name) {
  if (process.env[name]) return process.env[name].replace(/\\n/g, '\n');
  return process.env[`${name}_FILE`] ? fs.readFileSync(process.env[`${name}_FILE`], 'utf8') : undefined;
}

function tlsOptions(enabledName, rejectName, caName) {
  if (process.env[enabledName] === 'false') return false;
  const options = { rejectUnauthorized: process.env[rejectName] !== 'false' };
  const ca = optionalCa(caName);
  if (ca) options.ca = ca;
  return options;
}

const pool = config.dbHost && config.dbName && config.dbUser && config.dbPassword
  ? new Pool({
      host: config.dbHost,
      port: config.dbPort,
      database: config.dbName,
      user: config.dbUser,
      password: config.dbPassword,
      ssl: tlsOptions('DB_SSL', 'DB_SSL_REJECT_UNAUTHORIZED', 'DB_SSL_CA'),
      connectionTimeoutMillis: 5000,
      idleTimeoutMillis: 10000,
      max: Number(process.env.DB_POOL_MAX || 10)
    })
  : null;

const redis = config.redisHost && config.redisAuth
  ? new Redis({
      host: config.redisHost,
      port: config.redisPort,
      password: config.redisAuth,
      tls: tlsOptions('REDIS_TLS', 'REDIS_TLS_REJECT_UNAUTHORIZED', 'REDIS_TLS_CA'),
      connectTimeout: 5000,
      maxRetriesPerRequest: 1,
      retryStrategy: times => Math.min(times * 200, 3000)
    })
  : null;
const sqs = new SQSClient({ region: config.region });

const state = {
  databaseReady: false,
  databaseInitializing: false,
  databaseError: pool ? null : 'database configuration missing',
  redisConnected: false,
  redisError: redis ? null : 'redis disabled',
  resolvedQueueUrl: config.queueUrl,
  queueReady: Boolean(config.queueUrl),
  dispatcherRunning: false,
  lastDispatch: null
};

redis?.on('ready', () => { state.redisConnected = true; state.redisError = null; });
redis?.on('error', error => { state.redisConnected = false; state.redisError = error.message; });
pool?.on('error', error => { state.databaseReady = false; state.databaseError = error.message; });

async function resolveQueueUrl() {
  if (state.resolvedQueueUrl) return state.resolvedQueueUrl;
  if (!config.queueName) throw new Error('SQS_QUEUE_URL or SQS_QUEUE_NAME is required');
  const result = await sqs.send(new GetQueueUrlCommand({ QueueName: config.queueName }));
  state.resolvedQueueUrl = result.QueueUrl;
  state.queueReady = true;
  return state.resolvedQueueUrl;
}

async function initializeDatabase() {
  if (!pool || state.databaseInitializing) return;
  state.databaseInitializing = true;
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS orders (
        id VARCHAR(64) PRIMARY KEY,
        item VARCHAR(255) NOT NULL,
        quantity INT NOT NULL,
        amount DECIMAL(10,2) NOT NULL,
        customer VARCHAR(255) NOT NULL,
        status VARCHAR(64) NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
      ALTER TABLE orders ADD COLUMN IF NOT EXISTS idempotency_key VARCHAR(255);
      ALTER TABLE orders ADD COLUMN IF NOT EXISTS payment_processed_at TIMESTAMPTZ;
      ALTER TABLE orders ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();
      CREATE UNIQUE INDEX IF NOT EXISTS orders_idempotency_key_unique
        ON orders (idempotency_key) WHERE idempotency_key IS NOT NULL;
      CREATE TABLE IF NOT EXISTS order_outbox (
        event_id UUID PRIMARY KEY,
        aggregate_id VARCHAR(64) NOT NULL REFERENCES orders(id),
        event_type VARCHAR(64) NOT NULL,
        schema_version INT NOT NULL,
        payload JSONB NOT NULL,
        status VARCHAR(32) NOT NULL DEFAULT 'PENDING',
        attempts INT NOT NULL DEFAULT 0,
        next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        published_at TIMESTAMPTZ,
        message_id VARCHAR(255),
        last_error TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
      CREATE INDEX IF NOT EXISTS order_outbox_pending_idx
        ON order_outbox (next_attempt_at, created_at) WHERE status = 'PENDING';
    `);
    state.databaseReady = true;
    state.databaseError = null;
  } catch (error) {
    state.databaseReady = false;
    state.databaseError = error.message;
    console.error('[order-service] database initialization failed:', error.message);
  } finally {
    state.databaseInitializing = false;
  }
}

async function sendEvent(payload) {
  const event = typeof payload === 'string' ? JSON.parse(payload) : payload;
  const result = await sqs.send(new SendMessageCommand({
    QueueUrl: await resolveQueueUrl(),
    MessageBody: JSON.stringify(event),
    MessageAttributes: {
      EventType: { DataType: 'String', StringValue: event.eventType },
      SchemaVersion: { DataType: 'Number', StringValue: String(event.schemaVersion) },
      TraceId: { DataType: 'String', StringValue: event.traceId }
    }
  }));
  return { messageId: result.MessageId };
}

async function runDispatcher() {
  if (state.dispatcherRunning || !state.databaseReady) return;
  state.dispatcherRunning = true;
  try {
    state.lastDispatch = { ...(await dispatchOutboxBatch({ pool, sendEvent })), at: new Date().toISOString() };
  } catch (error) {
    state.lastDispatch = { error: error.message, at: new Date().toISOString() };
    console.warn('[order-service] outbox dispatch failed:', error.message);
  } finally {
    state.dispatcherRunning = false;
  }
}

function json(res, statusCode, body) {
  res.statusCode = statusCode;
  res.setHeader('Content-Type', 'application/json');
  res.end(JSON.stringify(body));
}

function traceIdFrom(req) {
  const match = typeof req.headers.traceparent === 'string' && req.headers.traceparent.match(/^\d{2}-([a-f0-9]{32})-/i);
  return match ? match[1] : String(req.headers['x-correlation-id'] || randomUUID());
}

function readJsonBody(req, maxBytes = 65536) {
  return new Promise((resolve, reject) => {
    let body = '';
    req.on('data', chunk => {
      body += chunk;
      if (Buffer.byteLength(body) > maxBytes) reject(new TypeError('request body too large'));
    });
    req.on('end', () => {
      try { resolve(JSON.parse(body || '{}')); } catch { reject(new TypeError('invalid JSON payload')); }
    });
    req.on('error', reject);
  });
}

async function listOrders() {
  if (!pool || !state.databaseReady) throw new Error('database unavailable');
  if (redis && state.redisConnected) {
    try {
      const cached = await redis.get(ORDER_CACHE_KEY);
      if (cached) return { orders: JSON.parse(cached), cached: true, cacheStatus: 'HIT' };
    } catch (error) { state.redisError = error.message; }
  }
  const result = await pool.query(`
    SELECT o.*, x.event_id, COALESCE(x.status, 'UNKNOWN') AS event_status,
           x.published_at AS event_published_at
    FROM orders o
    LEFT JOIN LATERAL (
      SELECT event_id, status, published_at FROM order_outbox
      WHERE aggregate_id = o.id ORDER BY created_at DESC LIMIT 1
    ) x ON TRUE
    ORDER BY o.created_at DESC LIMIT 20
  `);
  const orders = result.rows.map(mapOrderRow);
  let cacheStatus = redis ? (state.redisConnected ? 'MISS' : 'ERROR') : 'BYPASS';
  if (redis && state.redisConnected) {
    try { await redis.set(ORDER_CACHE_KEY, JSON.stringify(orders), 'EX', 15); }
    catch (error) { cacheStatus = 'ERROR'; state.redisError = error.message; }
  }
  return { orders, cached: false, cacheStatus };
}

async function statusSnapshot() {
  let pending = null;
  if (pool && state.databaseReady) {
    const result = await pool.query("SELECT COUNT(*)::int AS pending FROM order_outbox WHERE status = 'PENDING'");
    pending = result.rows[0].pending;
  }
  return {
    db: { connected: state.databaseReady },
    redis: { connected: state.redisConnected, optional: true },
    sqs: { configured: Boolean(config.queueUrl || config.queueName), connected: state.queueReady },
    outbox: {
      pending,
      lastDispatch: state.lastDispatch ? {
        at: state.lastDispatch.at,
        published: state.lastDispatch.published || 0,
        failed: state.lastDispatch.failed || 0,
        healthy: !state.lastDispatch.error
      } : null
    }
  };
}

const server = http.createServer(async (req, res) => {
  const pathname = new URL(req.url, 'http://localhost').pathname;
  if (process.env.CORS_ORIGIN) res.setHeader('Access-Control-Allow-Origin', process.env.CORS_ORIGIN);
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization, Idempotency-Key, Traceparent, X-Correlation-Id');
  if (req.method === 'OPTIONS') return json(res, 204, {});
  if (pathname === '/health') return json(res, 200, { status: 'UP', service: 'order-service', uptime: process.uptime() });
  if (pathname === '/ready') {
    const ready = state.databaseReady && state.queueReady;
    return json(res, ready ? 200 : 503, { status: ready ? 'READY' : 'NOT_READY', service: 'order-service' });
  }

  try {
    if (pathname === '/api/orders/status' && req.method === 'GET') return json(res, 200, await statusSnapshot());
    if (pathname === '/api/orders/db-test' && req.method === 'GET') {
      if (!pool || !state.databaseReady) return json(res, 503, { dbConnected: false });
      const result = await pool.query('SELECT NOW() AS server_time, COUNT(*)::int AS order_count FROM orders');
      return json(res, 200, { dbConnected: true, serverTime: result.rows[0].server_time, orderCount: result.rows[0].order_count });
    }
    if (pathname === '/api/orders/redis-test' && req.method === 'GET') {
      if (!redis || !state.redisConnected) return json(res, 503, { redisConnected: false });
      const started = Date.now();
      await redis.ping();
      return json(res, 200, { redisConnected: true, latencyMs: Date.now() - started });
    }
    if (pathname === '/api/orders/sqs-test' && req.method === 'GET') {
      const result = await sqs.send(new GetQueueAttributesCommand({
        QueueUrl: await resolveQueueUrl(),
        AttributeNames: ['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible']
      }));
      return json(res, 200, { sqsConfigured: true, attributes: result.Attributes });
    }
    if (pathname === '/api/orders/secrets-info' && req.method === 'GET') {
      return json(res, 200, { provider: 'External Secrets Operator', configuration: {
        databaseCredentialsLoaded: Boolean(config.dbPassword),
        redisCredentialsLoaded: Boolean(config.redisAuth),
        queueConfigured: Boolean(config.queueUrl || config.queueName)
      } });
    }
    if (pathname === '/api/orders' && req.method === 'GET') {
      const result = await listOrders();
      return json(res, 200, { success: true, source: result.cached ? 'ELASTICACHE_REDIS' : 'POSTGRESQL_RDS', ...result });
    }
    if (pathname === '/api/orders' && req.method === 'POST') {
      if (!state.databaseReady) return json(res, 503, { success: false, error: 'database unavailable' });
      const idempotencyKey = req.headers['idempotency-key'];
      if (typeof idempotencyKey !== 'string' || idempotencyKey.length < 8 || idempotencyKey.length > 255) {
        throw new TypeError('Idempotency-Key header must contain 8 to 255 characters');
      }
      const result = await createOrderWithOutbox({
        pool,
        payload: await readJsonBody(req),
        idempotencyKey,
        traceId: traceIdFrom(req)
      });
      if (redis && state.redisConnected) await redis.del(ORDER_CACHE_KEY).catch(error => { state.redisError = error.message; });
      setImmediate(runDispatcher);
      return json(res, result.duplicate ? 200 : 201, {
        success: true,
        duplicate: result.duplicate,
        order: result.order,
        storage: 'POSTGRESQL_RDS',
        outbox: { eventId: result.order.eventId, status: result.order.eventStatus },
        sqs: { dispatched: false, published: false, status: 'PENDING_OUTBOX' }
      });
    }
    return json(res, 404, { error: 'Not Found', service: 'order-service' });
  } catch (error) {
    const clientError = error instanceof TypeError;
    console.warn('[order-service] request failed:', error.message);
    return json(res, clientError ? 400 : 503, { success: false, error: clientError ? error.message : 'dependency unavailable' });
  }
});

async function start() {
  await initializeDatabase();
  resolveQueueUrl().catch(error => { state.queueReady = false; console.warn('[order-service] queue resolution pending:', error.message); });
  setInterval(() => {
    if (!state.databaseReady) initializeDatabase();
    if (!state.queueReady) resolveQueueUrl().catch(() => { state.queueReady = false; });
  }, 5000).unref();
  setInterval(runDispatcher, config.outboxPollMs).unref();
  server.listen(config.port, '0.0.0.0', () => console.log(`[order-service] listening on port ${config.port}`));
}

async function shutdown() {
  server.close();
  await pool?.end().catch(() => {});
  redis?.disconnect();
}

module.exports = { config, initializeDatabase, listOrders, resolveQueueUrl, runDispatcher, server, shutdown, start, state, statusSnapshot };
