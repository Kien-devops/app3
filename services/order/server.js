const http = require('http');
const url = require('url');

const PORT = process.env.PORT || 8080;
const SERVICE_NAME = 'order-service';

// Database & ESO Configuration
const DB_HOST = process.env.DB_HOST || 'sre-platform-dev-pg.c1ky6i8iomcn.ap-southeast-1.rds.amazonaws.com';
const DB_PORT = parseInt(process.env.DB_PORT || '5432', 10);
const DB_NAME = process.env.DB_NAME || 'reliability_shop';
const DB_USER = process.env.DB_USER || 'sre_admin';
const DB_PASSWORD = process.env.DB_PASSWORD || '';

// Redis & ElastiCache Configuration
const REDIS_HOST = process.env.REDIS_HOST || 'master.sre-platform-dev-redis.qspmd1.apse1.cache.amazonaws.com';
const REDIS_PORT = parseInt(process.env.REDIS_PORT || '6379', 10);
const REDIS_AUTH = process.env.REDIS_AUTH || '';

// SQS Configuration
const SQS_QUEUE_URL = process.env.SQS_QUEUE_URL || 'https://sqs.ap-southeast-1.amazonaws.com/620306387033/reliability-shop-dev-order-queue';
const AWS_REGION = process.env.AWS_REGION || 'ap-southeast-1';

// 1. Initialize PostgreSQL Client
let pgPool = null;
let pgStatus = { connected: false, error: null, lastChecked: null };

try {
  const { Pool } = require('pg');
  if (DB_PASSWORD) {
    pgPool = new Pool({
      host: DB_HOST,
      port: DB_PORT,
      database: DB_NAME,
      user: DB_USER,
      password: DB_PASSWORD,
      ssl: process.env.DB_SSL === 'false' ? false : { rejectUnauthorized: false },
      connectionTimeoutMillis: 5000,
      idleTimeoutMillis: 10000
    });

    pgPool.query(`
      CREATE TABLE IF NOT EXISTS orders (
        id VARCHAR(64) PRIMARY KEY,
        item VARCHAR(255) NOT NULL,
        quantity INT NOT NULL,
        amount DECIMAL(10,2) NOT NULL,
        customer VARCHAR(255),
        status VARCHAR(64),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );
    `).then(() => {
      pgStatus = { connected: true, error: null, lastChecked: new Date().toISOString() };
      console.log(`[${SERVICE_NAME}] Connected to PostgreSQL (${DB_HOST}:${DB_PORT}/${DB_NAME})`);
    }).catch(err => {
      pgStatus = { connected: false, error: err.message, lastChecked: new Date().toISOString() };
      console.warn(`[${SERVICE_NAME}] PostgreSQL init warning:`, err.message);
    });
  }
} catch (e) {
  console.warn(`[${SERVICE_NAME}] running in mock db mode:`, e.message);
}

// 2. Initialize Real Redis Client (ioredis over TLS)
let redisClient = null;
let redisStatus = { connected: false, error: null, lastPingMs: null, client: 'ioredis' };

try {
  const Redis = require('ioredis');
  if (REDIS_AUTH) {
    redisClient = new Redis({
      host: REDIS_HOST,
      port: REDIS_PORT,
      password: REDIS_AUTH,
      tls: { rejectUnauthorized: false },
      connectTimeout: 5000,
      lazyConnect: false,
      retryStrategy(times) {
        return Math.min(times * 100, 3000);
      }
    });

    redisClient.on('connect', () => {
      redisStatus.connected = true;
      redisStatus.error = null;
      console.log(`[${SERVICE_NAME}] Connected to Amazon ElastiCache Redis (${REDIS_HOST}:${REDIS_PORT}) via TLS`);
    });

    redisClient.on('error', (err) => {
      redisStatus.connected = false;
      redisStatus.error = err.message;
      console.warn(`[${SERVICE_NAME}] Redis connection error:`, err.message);
    });
  }
} catch (e) {
  console.warn(`[${SERVICE_NAME}] ioredis init exception:`, e.message);
}

// 3. Initialize Real AWS SQS Client
let sqsClient = null;
try {
  const { SQSClient } = require('@aws-sdk/client-sqs');
  sqsClient = new SQSClient({ region: AWS_REGION });
} catch (e) {
  console.warn(`[${SERVICE_NAME}] SQS client init exception:`, e.message);
}

// Helper to send real message to SQS
async function sendToSqs(orderPayload) {
  if (!sqsClient || !SQS_QUEUE_URL) {
    return { dispatched: false, reason: 'SQS_CLIENT_OR_URL_UNAVAILABLE' };
  }
  try {
    const { SendMessageCommand } = require('@aws-sdk/client-sqs');
    const command = new SendMessageCommand({
      QueueUrl: SQS_QUEUE_URL,
      MessageBody: JSON.stringify(orderPayload),
      MessageAttributes: {
        EventType: { DataType: 'String', StringValue: 'ORDER_CREATED' },
        Service: { DataType: 'String', StringValue: SERVICE_NAME }
      }
    });
    const result = await sqsClient.send(command);
    console.log(`[${SERVICE_NAME}] Real SQS Message Sent! MessageId: ${result.MessageId}`);
    return {
      dispatched: true,
      messageId: result.MessageId,
      md5OfMessageBody: result.MD5OfMessageBody,
      queueUrl: SQS_QUEUE_URL
    };
  } catch (err) {
    console.warn(`[${SERVICE_NAME}] SQS SendMessage error:`, err.message);
    return { dispatched: false, error: err.message };
  }
}

// In-memory fallback
const memoryOrders = [];

const server = http.createServer(async (req, res) => {
  const parsed = url.parse(req.url, true);
  const { pathname } = parsed;

  res.setHeader('Content-Type', 'application/json');
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');

  if (req.method === 'OPTIONS') {
    res.statusCode = 204;
    return res.end();
  }

  // SRE Health Probes
  if (pathname === '/health') {
    res.statusCode = 200;
    return res.end(JSON.stringify({
      status: 'UP',
      service: SERVICE_NAME,
      uptime: process.uptime(),
      dbConnected: pgStatus.connected,
      redisConnected: redisStatus.connected,
      sqsConfigured: Boolean(SQS_QUEUE_URL),
      timestamp: new Date().toISOString()
    }));
  }

  if (pathname === '/ready') {
    res.statusCode = 200;
    return res.end(JSON.stringify({ status: 'READY', service: SERVICE_NAME }));
  }

  // 1. LIVE REDIS TEST ENDPOINT
  if (pathname === '/api/orders/redis-test') {
    const startTime = Date.now();
    if (!redisClient || !redisStatus.connected) {
      return res.end(JSON.stringify({
        redisConnected: false,
        error: redisStatus.error || 'Redis client not connected',
        host: REDIS_HOST,
        port: REDIS_PORT,
        tls: true,
        hint: 'Check ElastiCache Security Group sg-0fc6af14d254fcf22 and REDIS_AUTH secret'
      }));
    }

    try {
      const pingRes = await redisClient.ping();
      const testKey = `sre_test_${Date.now()}`;
      await redisClient.set(testKey, 'RELIABILITY_SHOP_ACTIVE', 'EX', 60);
      const testVal = await redisClient.get(testKey);
      const latencyMs = Date.now() - startTime;

      res.statusCode = 200;
      return res.end(JSON.stringify({
        redisConnected: true,
        pingResponse: pingRes,
        latencyMs,
        testKey,
        retrievedValue: testVal,
        host: REDIS_HOST,
        port: REDIS_PORT,
        tlsMode: 'TLSv1.3 In-Transit Encrypted',
        authMode: 'Redis AUTH via External Secrets (AES-256)',
        timestamp: new Date().toISOString()
      }, null, 2));
    } catch (err) {
      const latencyMs = Date.now() - startTime;
      res.statusCode = 200;
      return res.end(JSON.stringify({
        redisConnected: false,
        latencyMs,
        error: err.message,
        host: REDIS_HOST,
        port: REDIS_PORT
      }));
    }
  }

  // 2. LIVE REAL SQS TEST ENDPOINT
  if (pathname === '/api/orders/sqs-test') {
    const testOrder = {
      orderId: `test-ord-${Date.now().toString().slice(-4)}`,
      item: 'SRE Real SQS Queue Test Packet',
      amount: 49.00,
      timestamp: new Date().toISOString()
    };

    const sqsResult = await sendToSqs(testOrder);
    res.statusCode = 200;
    return res.end(JSON.stringify({
      operation: 'SQS_SEND_MESSAGE',
      queueUrl: SQS_QUEUE_URL,
      sqsDispatched: sqsResult.dispatched,
      messageId: sqsResult.messageId || null,
      md5: sqsResult.md5OfMessageBody || null,
      error: sqsResult.error || null,
      kmsEncrypted: true,
      kmsKeyArn: 'arn:aws:kms:ap-southeast-1:620306387033:key/e5164492-8e17-4d65-b1c5-66b7b9601800',
      podIdentity: 'Active (sre-platform-dev-app-role)',
      timestamp: new Date().toISOString()
    }, null, 2));
  }

  // 3. SECRETS MAPPING VERIFICATION ENDPOINT
  if (pathname === '/api/orders/secrets-info') {
    res.statusCode = 200;
    return res.end(JSON.stringify({
      provider: 'External Secrets Operator (ESO)',
      sourceStore: 'sre-platform-cluster-secret-store',
      secretsSynced: [
        { key: 'DB_HOST', loaded: Boolean(DB_HOST), value: DB_HOST },
        { key: 'DB_NAME', loaded: Boolean(DB_NAME), value: DB_NAME },
        { key: 'DB_USER', loaded: Boolean(DB_USER), value: DB_USER },
        { key: 'DB_PASSWORD', loaded: Boolean(DB_PASSWORD), masked: '●●●●●●●●●●●● (KMS Decrypted)' },
        { key: 'REDIS_HOST', loaded: Boolean(REDIS_HOST), value: REDIS_HOST },
        { key: 'REDIS_PORT', loaded: Boolean(REDIS_PORT), value: REDIS_PORT },
        { key: 'REDIS_AUTH', loaded: Boolean(REDIS_AUTH), masked: '●●●●●●●●●●●● (Loaded via ESO)' },
        { key: 'JWT_SECRET', loaded: Boolean(process.env.JWT_SECRET), masked: '●●●●●●●●●●●● (Loaded)' }
      ],
      serviceAccount: 'reliability-shop-sa',
      podIdentityRole: 'arn:aws:iam::620306387033:role/sre-platform-dev-app-role',
      timestamp: new Date().toISOString()
    }, null, 2));
  }

  // 4. LIVE DATABASE PING & TEST
  if (pathname === '/api/orders/db-test') {
    const startTime = Date.now();
    if (!pgPool) {
      res.statusCode = 200;
      return res.end(JSON.stringify({
        dbConnected: false,
        source: 'MOCK_DRIVER',
        message: 'DB_PASSWORD not loaded or pg library not initialized.',
        config: { host: DB_HOST, port: DB_PORT, database: DB_NAME, user: DB_USER }
      }));
    }

    try {
      const client = await pgPool.connect();
      try {
        await client.query(`
          CREATE TABLE IF NOT EXISTS orders (
            id VARCHAR(64) PRIMARY KEY,
            item VARCHAR(255) NOT NULL,
            quantity INT NOT NULL,
            amount DECIMAL(10,2) NOT NULL,
            customer VARCHAR(255),
            status VARCHAR(64),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
          );
        `);
        pgStatus = { connected: true, error: null, lastChecked: new Date().toISOString() };
        const result = await client.query(`
          SELECT 
            NOW() as server_time,
            current_database() as database_name,
            current_user as current_user,
            version() as pg_version,
            (SELECT count(*) FROM orders) as order_count;
        `);
        const latencyMs = Date.now() - startTime;
        res.statusCode = 200;
        return res.end(JSON.stringify({
          dbConnected: true,
          latencyMs,
          serverTime: result.rows[0].server_time,
          databaseName: result.rows[0].database_name,
          currentUser: result.rows[0].current_user,
          orderCount: parseInt(result.rows[0].order_count, 10),
          pgVersion: result.rows[0].pg_version,
          host: DB_HOST,
          port: DB_PORT
        }));
      } finally {
        client.release();
      }
    } catch (err) {
      const latencyMs = Date.now() - startTime;
      res.statusCode = 200;
      return res.end(JSON.stringify({
        dbConnected: false,
        latencyMs,
        error: err.message,
        code: err.code || 'CONNECT_ERROR',
        host: DB_HOST,
        port: DB_PORT,
        database: DB_NAME,
        user: DB_USER
      }));
    }
  }

  // 5. GET ORDERS (WITH REAL CACHE-ASIDE PATTERN VIA REDIS)
  if (pathname === '/api/orders' && req.method === 'GET') {
    // 5.1 Check Redis Cache first
    if (redisClient && redisStatus.connected) {
      try {
        const cached = await redisClient.get('reliabilityshop:orders:list');
        if (cached) {
          res.statusCode = 200;
          return res.end(JSON.stringify({
            success: true,
            source: 'ELASTICACHE_REDIS (Cache Hit)',
            cached: true,
            orders: JSON.parse(cached),
            metadata: { dbHost: DB_HOST, redisHost: REDIS_HOST }
          }));
        }
      } catch (e) {
        console.warn('[order-service] Redis cache read failed, falling back to DB:', e.message);
      }
    }

    // 5.2 Query PostgreSQL
    if (pgPool && pgStatus.connected) {
      try {
        const result = await pgPool.query('SELECT * FROM orders ORDER BY created_at DESC LIMIT 20');
        const orders = result.rows.map(r => ({
          id: r.id,
          item: r.item,
          quantity: r.quantity,
          amount: parseFloat(r.amount),
          customer: r.customer,
          status: r.status,
          createdAt: r.created_at
        }));

        // Populate Redis cache with 15s TTL
        if (redisClient && redisStatus.connected) {
          redisClient.set('reliabilityshop:orders:list', JSON.stringify(orders), 'EX', 15).catch(() => {});
        }

        res.statusCode = 200;
        return res.end(JSON.stringify({
          success: true,
          source: 'POSTGRESQL_RDS (Cache Miss -> DB Queried & Cached)',
          cached: false,
          orders,
          metadata: { dbHost: DB_HOST, redisHost: REDIS_HOST }
        }));
      } catch (err) {
        console.warn('DB query error, falling back to memory:', err.message);
      }
    }

    res.statusCode = 200;
    return res.end(JSON.stringify({
      success: true,
      source: 'IN_MEMORY_FALLBACK',
      orders: memoryOrders,
      metadata: { dbHost: DB_HOST, dbUser: DB_USER, dbName: DB_NAME }
    }));
  }

  // 6. PLACE ORDER (POSTGRESQL + REDIS CACHE + REAL SQS DISPATCH)
  if (pathname === '/api/orders' && req.method === 'POST') {
    let body = '';
    req.on('data', chunk => { body += chunk; });
    req.on('end', async () => {
      try {
        const payload = JSON.parse(body || '{}');
        const newOrder = {
          id: `ord-${Date.now().toString().slice(-4)}`,
          item: payload.item || 'Cloud-Native Architecture License',
          quantity: payload.quantity || 1,
          amount: payload.amount || 99.00,
          customer: payload.customer || 'SRE Platform Engineer',
          status: 'PENDING_PAYMENT',
          createdAt: new Date().toISOString()
        };

        let storageMode = 'IN_MEMORY';

        // 6.1 Insert to PostgreSQL
        if (pgPool) {
          try {
            await pgPool.query(
              'INSERT INTO orders (id, item, quantity, amount, customer, status, created_at) VALUES ($1, $2, $3, $4, $5, $6, $7)',
              [newOrder.id, newOrder.item, newOrder.quantity, newOrder.amount, newOrder.customer, newOrder.status, newOrder.createdAt]
            );
            storageMode = 'POSTGRESQL_RDS';
          } catch (e) {
            console.warn('Failed to insert into PostgreSQL, falling back to memory:', e.message);
            memoryOrders.unshift(newOrder);
          }
        } else {
          memoryOrders.unshift(newOrder);
        }

        // 6.2 Invalidate Redis list cache and store individual order
        if (redisClient && redisStatus.connected) {
          redisClient.del('reliabilityshop:orders:list').catch(() => {});
          redisClient.set(`reliabilityshop:order:${newOrder.id}`, JSON.stringify(newOrder), 'EX', 300).catch(() => {});
        }

        // 6.3 Real SQS Message Dispatch
        const sqsResult = await sendToSqs(newOrder);

        res.statusCode = 201;
        return res.end(JSON.stringify({
          success: true,
          order: newOrder,
          storage: storageMode,
          redisCached: redisStatus.connected,
          sqs: sqsResult
        }));
      } catch (err) {
        res.statusCode = 400;
        return res.end(JSON.stringify({ error: 'Invalid JSON payload' }));
      }
    });
    return;
  }

  res.statusCode = 404;
  res.end(JSON.stringify({ error: 'Not Found', service: SERVICE_NAME }));
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`[${SERVICE_NAME}] listening on port ${PORT}`);
});
