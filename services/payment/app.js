const fs = require('fs');
const http = require('http');
const { Pool } = require('pg');
const Redis = require('ioredis');
const { DeleteMessageCommand, GetQueueUrlCommand, ReceiveMessageCommand, SQSClient } = require('@aws-sdk/client-sqs');
const { processOrderEvent } = require('./core');

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
  redisAuth: process.env.REDIS_AUTH || ''
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
  workerStarting: false,
  isPolling: false,
  processedCount: 0,
  duplicateCount: 0,
  failureCount: 0,
  lastProcessed: null,
  redisConnected: false,
  resolvedQueueUrl: config.queueUrl,
  recentLogs: []
};

redis?.on('ready', () => { state.redisConnected = true; });
redis?.on('error', () => { state.redisConnected = false; });
pool?.on('error', error => { state.databaseReady = false; addLog(`database pool error: ${error.message}`); });

function addLog(message) {
  const entry = `[${new Date().toISOString()}] ${message}`;
  console.log(entry);
  state.recentLogs.unshift(entry);
  if (state.recentLogs.length > 20) state.recentLogs.pop();
}

async function resolveQueueUrl() {
  if (state.resolvedQueueUrl) return state.resolvedQueueUrl;
  if (!config.queueName) throw new Error('SQS_QUEUE_URL or SQS_QUEUE_NAME is required');
  const result = await sqs.send(new GetQueueUrlCommand({ QueueName: config.queueName }));
  state.resolvedQueueUrl = result.QueueUrl;
  return state.resolvedQueueUrl;
}

async function initializeDatabase() {
  if (!pool || state.databaseInitializing) return;
  state.databaseInitializing = true;
  try {
    await pool.query(`
      ALTER TABLE orders ADD COLUMN IF NOT EXISTS payment_processed_at TIMESTAMPTZ;
      ALTER TABLE orders ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();
      CREATE TABLE IF NOT EXISTS processed_events (
        event_id VARCHAR(128) PRIMARY KEY,
        order_id VARCHAR(64) NOT NULL,
        processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
    `);
    state.databaseReady = true;
  } catch (error) {
    state.databaseReady = false;
    addLog(`database initialization failed: ${error.message}`);
  } finally {
    state.databaseInitializing = false;
  }
}

async function handleMessage(message) {
  const result = await processOrderEvent({ pool, body: message.Body, messageId: message.MessageId });
  if (redis && state.redisConnected) {
    await redis.del('reliabilityshop:orders:list').catch(error => addLog(`cache invalidation failed: ${error.message}`));
  }
  await sqs.send(new DeleteMessageCommand({
    QueueUrl: await resolveQueueUrl(),
    ReceiptHandle: message.ReceiptHandle
  }));
  if (result.duplicate) state.duplicateCount += 1;
  else state.processedCount += 1;
  state.lastProcessed = {
    eventId: result.event.eventId,
    orderId: result.event.orderId,
    duplicate: result.duplicate,
    processedAt: new Date().toISOString()
  };
  addLog(`${result.duplicate ? 'duplicate acknowledged' : 'payment committed'} for order ${result.event.orderId}`);
}

async function pollOnce() {
  const response = await sqs.send(new ReceiveMessageCommand({
    QueueUrl: await resolveQueueUrl(),
    MaxNumberOfMessages: 5,
    WaitTimeSeconds: 20,
    AttributeNames: ['All'],
    MessageAttributeNames: ['All']
  }));
  for (const message of response.Messages || []) {
    try {
      await handleMessage(message);
    } catch (error) {
      state.failureCount += 1;
      addLog(`message retained for retry: ${error.message}`);
    }
  }
}

async function pollLoop() {
  if (!state.isPolling) return;
  try { await pollOnce(); }
  catch (error) { addLog(`SQS poll failed: ${error.message}`); }
  setImmediate(pollLoop);
}

async function ensureWorkerStarted() {
  if (state.workerStarting || state.isPolling) return;
  state.workerStarting = true;
  try {
    if (!state.databaseReady) await initializeDatabase();
    await resolveQueueUrl();
    if (state.databaseReady) {
      state.isPolling = true;
      pollLoop();
    }
  } catch (error) {
    addLog(`worker dependency unavailable: ${error.message}`);
  } finally {
    state.workerStarting = false;
  }
}

function json(res, statusCode, body) {
  res.statusCode = statusCode;
  res.setHeader('Content-Type', 'application/json');
  res.end(JSON.stringify(body));
}

const server = http.createServer((req, res) => {
  const pathname = new URL(req.url, 'http://localhost').pathname;
  if (process.env.CORS_ORIGIN) res.setHeader('Access-Control-Allow-Origin', process.env.CORS_ORIGIN);
  if (pathname === '/health') return json(res, 200, { status: 'UP', service: 'payment-worker', uptime: process.uptime() });
  if (pathname === '/ready') {
    const ready = state.databaseReady && state.isPolling && Boolean(config.queueUrl || config.queueName);
    return json(res, ready ? 200 : 503, { status: ready ? 'READY' : 'NOT_READY', service: 'payment-worker' });
  }
  if (pathname === '/api/worker/stats') {
    return json(res, 200, {
      worker: 'payment-worker',
      isPolling: state.isPolling,
      messagesProcessed: state.processedCount,
      duplicateMessages: state.duplicateCount,
      failedMessages: state.failureCount,
      lastProcessed: state.lastProcessed,
      timestamp: new Date().toISOString()
    });
  }
  return json(res, 404, { error: 'Endpoint not found' });
});

async function start() {
  await ensureWorkerStarted();
  setInterval(ensureWorkerStarted, 5000).unref();
  server.listen(config.port, '0.0.0.0', () => addLog(`listening on port ${config.port}`));
}

async function shutdown() {
  state.isPolling = false;
  server.close();
  await pool?.end().catch(() => {});
  redis?.disconnect();
}

module.exports = { config, ensureWorkerStarted, handleMessage, initializeDatabase, pollOnce, resolveQueueUrl, server, shutdown, start, state };
