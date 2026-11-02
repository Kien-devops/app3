const http = require('http');
const url = require('url');

const PORT = process.env.PORT || 8080;
const SERVICE_NAME = 'payment-worker';

// SQS Configuration
const SQS_QUEUE_URL = process.env.SQS_QUEUE_URL || 'https://sqs.ap-southeast-1.amazonaws.com/620306387033/reliability-shop-dev-order-queue';
const AWS_REGION = process.env.AWS_REGION || 'ap-southeast-1';

// Database & Redis Configuration
const DB_HOST = process.env.DB_HOST || 'sre-platform-dev-pg.c1ky6i8iomcn.ap-southeast-1.rds.amazonaws.com';
const DB_PORT = parseInt(process.env.DB_PORT || '5432', 10);
const DB_NAME = process.env.DB_NAME || 'reliability_shop';
const DB_USER = process.env.DB_USER || 'sre_admin';
const DB_PASSWORD = process.env.DB_PASSWORD || '';

const REDIS_HOST = process.env.REDIS_HOST || 'master.sre-platform-dev-redis.qspmd1.apse1.cache.amazonaws.com';
const REDIS_PORT = parseInt(process.env.REDIS_PORT || '6379', 10);
const REDIS_AUTH = process.env.REDIS_AUTH || '';

let pgPool = null;
if (DB_PASSWORD) {
  try {
    const { Pool } = require('pg');
    pgPool = new Pool({
      host: DB_HOST,
      port: DB_PORT,
      database: DB_NAME,
      user: DB_USER,
      password: DB_PASSWORD,
      ssl: process.env.DB_SSL === 'false' ? false : { rejectUnauthorized: false },
      connectionTimeoutMillis: 5000
    });
  } catch (e) {
    console.warn(`[${SERVICE_NAME}] pg init error:`, e.message);
  }
}

let redisClient = null;
if (REDIS_AUTH) {
  try {
    const Redis = require('ioredis');
    redisClient = new Redis({
      host: REDIS_HOST,
      port: REDIS_PORT,
      password: REDIS_AUTH,
      tls: { rejectUnauthorized: false },
      connectTimeout: 5000,
      retryStrategy: () => 5000
    });
  } catch (e) {
    console.warn(`[${SERVICE_NAME}] redis init error:`, e.message);
  }
}

let sqsClient = null;
try {
  const { SQSClient } = require('@aws-sdk/client-sqs');
  sqsClient = new SQSClient({ region: AWS_REGION });
} catch (e) {
  console.warn(`[${SERVICE_NAME}] sqs init error:`, e.message);
}

let processedCount = 0;
let lastProcessed = null;
let isPolling = true;
let pollLogs = [];

function addLog(msg) {
  const logEntry = `[${new Date().toISOString()}] ${msg}`;
  console.log(logEntry);
  pollLogs.unshift(logEntry);
  if (pollLogs.length > 50) pollLogs.pop();
}

// REAL SQS CONSUMER LOOP
async function pollSqsMessages() {
  if (!isPolling || !sqsClient || !SQS_QUEUE_URL) {
    setTimeout(pollSqsMessages, 5000);
    return;
  }

  try {
    const { ReceiveMessageCommand, DeleteMessageCommand } = require('@aws-sdk/client-sqs');
    const receiveCommand = new ReceiveMessageCommand({
      QueueUrl: SQS_QUEUE_URL,
      MaxNumberOfMessages: 5,
      WaitTimeSeconds: 5, // Long polling
      AttributeNames: ['All'],
      MessageAttributeNames: ['All']
    });

    const response = await sqsClient.send(receiveCommand);

    if (response.Messages && response.Messages.length > 0) {
      for (const msg of response.Messages) {
        let order = {};
        try {
          order = JSON.parse(msg.Body || '{}');
        } catch (e) {
          order = { raw: msg.Body };
        }

        const orderId = order.id || order.orderId;
        addLog(`📬 Consumed message from SQS (ID: ${msg.MessageId}, Order: ${orderId || 'UNKNOWN'})`);

        // Update database order status to PAID_VIA_SQS_WORKER
        if (orderId && pgPool) {
          try {
            await pgPool.query(
              `UPDATE orders SET status = 'PAID_VIA_SQS_WORKER' WHERE id = $1`,
              [orderId]
            );
            addLog(`✔ Updated order ${orderId} in PostgreSQL to status=PAID_VIA_SQS_WORKER`);
          } catch (dbErr) {
            addLog(`⚠️ DB update error for ${orderId}: ${dbErr.message}`);
          }
        }

        // Invalidate Redis list cache
        if (redisClient) {
          redisClient.del('reliabilityshop:orders:list').catch(() => {});
        }

        // Delete processed message from SQS
        try {
          const deleteCommand = new DeleteMessageCommand({
            QueueUrl: SQS_QUEUE_URL,
            ReceiptHandle: msg.ReceiptHandle
          });
          await sqsClient.send(deleteCommand);
          addLog(`🗑️ Deleted message ${msg.MessageId} from SQS queue`);
        } catch (delErr) {
          addLog(`⚠️ SQS delete error: ${delErr.message}`);
        }

        processedCount += 1;
        lastProcessed = {
          messageId: msg.MessageId,
          orderId,
          processedAt: new Date().toISOString()
        };
      }
    }
  } catch (err) {
    // Only log if not a standard timeout
    if (!err.message.includes('socket hang up')) {
      addLog(`⚠️ SQS Poll error: ${err.message}`);
    }
  }

  // Next poll
  setTimeout(pollSqsMessages, 1000);
}

// Start consumer loop
addLog(`[payment-worker] Starting real SQS consumer loop for queue: ${SQS_QUEUE_URL}`);
pollSqsMessages();

const server = http.createServer((req, res) => {
  const parsed = url.parse(req.url, true);
  const { pathname } = parsed;

  res.setHeader('Content-Type', 'application/json');
  res.setHeader('Access-Control-Allow-Origin', '*');

  if (pathname === '/health' || pathname === '/ready') {
    res.statusCode = 200;
    return res.end(JSON.stringify({
      status: 'UP',
      service: SERVICE_NAME,
      workerState: isPolling ? 'ACTIVE_LONG_POLLING' : 'PAUSED',
      sqsQueueUrl: SQS_QUEUE_URL,
      messagesProcessed: processedCount,
      lastProcessed,
      dbConnected: Boolean(pgPool),
      redisConnected: Boolean(redisClient),
      recentLogs: pollLogs.slice(0, 10),
      timestamp: new Date().toISOString()
    }));
  }

  if (pathname === '/api/worker/stats') {
    res.statusCode = 200;
    return res.end(JSON.stringify({
      worker: SERVICE_NAME,
      isPolling,
      queueUrl: SQS_QUEUE_URL,
      messagesProcessed: processedCount,
      lastProcessed,
      recentLogs: pollLogs.slice(0, 20),
      timestamp: new Date().toISOString()
    }, null, 2));
  }

  res.statusCode = 404;
  res.end(JSON.stringify({ error: 'Endpoint not found' }));
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`[${SERVICE_NAME}] listening on port ${PORT}`);
});

process.on('SIGTERM', () => {
  console.log(`[${SERVICE_NAME}] SIGTERM received, draining worker...`);
  isPolling = false;
  server.close(() => process.exit(0));
});
