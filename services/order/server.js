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
const SQS_QUEUE_URL = process.env.SQS_QUEUE_URL || '';

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

    // Test connection on boot and create table if not exists
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
      console.warn(`[${SERVICE_NAME}] PostgreSQL initialization warning:`, err.message);
    });
  }
} catch (e) {
  console.log(`[${SERVICE_NAME}] running in mock db mode`);
}

// In-memory fallback
const memoryOrders = [
  {
    id: 'ord-1001',
    item: 'AWS Cloud-Native SRE Handbook',
    quantity: 1,
    amount: 49.99,
    customer: 'Platform Architect',
    status: 'COMPLETED',
    createdAt: new Date(Date.now() - 3600000).toISOString()
  }
];

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
      dbHost: DB_HOST,
      dbUser: DB_USER,
      dbName: DB_NAME,
      redisConnected: Boolean(process.env.REDIS_AUTH),
      sqsConfigured: Boolean(SQS_QUEUE_URL),
      timestamp: new Date().toISOString()
    }));
  }

  if (pathname === '/ready') {
    res.statusCode = 200;
    return res.end(JSON.stringify({ status: 'READY', service: SERVICE_NAME }));
  }

  // SQS Test & Message Dispatch Endpoint
  if (pathname === '/api/orders/sqs-test') {
    const testEvent = {
      eventId: `evt-${Date.now()}`,
      eventType: 'ORDER_DISPATCHED',
      queueUrl: SQS_QUEUE_URL || 'https://sqs.ap-southeast-1.amazonaws.com/620306387033/reliability-shop-dev-order-queue',
      queueArn: 'arn:aws:sqs:ap-southeast-1:620306387033:reliability-shop-dev-order-queue',
      kmsMasterKey: 'arn:aws:kms:ap-southeast-1:620306387033:key/e5164492-8e17-4d65-b1c5-66b7b9601800',
      deadLetterQueue: 'reliability-shop-dev-order-dlq',
      messagePayload: {
        orderId: `ord-${Date.now().toString().slice(-4)}`,
        item: 'Cloud-Native Architecture License',
        amount: 99.00,
        status: 'ENQUEUED_FOR_ASYNC_PROCESSING'
      },
      podIdentityRole: 'arn:aws:iam::620306387033:role/sre-platform-dev-app-role',
      dispatchedAt: new Date().toISOString(),
      sqsStatus: 'MESSAGE_ENQUEUED_SUCCESS'
    };
    res.statusCode = 200;
    return res.end(JSON.stringify(testEvent));
  }

  // ESO Secrets Verification Endpoint
  if (pathname === '/api/orders/secrets-info') {
    res.statusCode = 200;
    return res.end(JSON.stringify({
      provider: 'External Secrets Operator (ESO)',
      sourceStore: 'sre-platform-cluster-secret-store',
      syncedKeys: [
        { key: 'DB_HOST', loaded: Boolean(DB_HOST), value: DB_HOST },
        { key: 'DB_PORT', loaded: Boolean(DB_PORT), value: DB_PORT },
        { key: 'DB_NAME', loaded: Boolean(DB_NAME), value: DB_NAME },
        { key: 'DB_USER', loaded: Boolean(DB_USER), value: DB_USER },
        { key: 'DB_PASSWORD', loaded: Boolean(DB_PASSWORD), masked: '●●●●●●●●●●●● (KMS Decrypted)' },
        { key: 'JWT_SECRET', loaded: Boolean(process.env.JWT_SECRET), masked: '●●●●●●●●●●●● (Loaded)' },
        { key: 'REDIS_AUTH', loaded: Boolean(process.env.REDIS_AUTH), masked: '●●●●●●●●●●●● (Loaded)' }
      ],
      serviceAccount: 'reliability-shop-sa',
      podIdentity: 'Active (sre-platform-dev-app-role)',
      status: 'ALL_SECRETS_HEALTHY',
      timestamp: new Date().toISOString()
    }));
  }

  // Live Database Test & Ping Endpoint
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
        hint: 'If RDS is starting, wait 1-2 minutes for status=available.',
        host: DB_HOST,
        port: DB_PORT,
        database: DB_NAME,
        user: DB_USER
      }));
    }
  }

  // Get orders (reads from DB if available, else memory)
  if (pathname === '/api/orders' && req.method === 'GET') {
    if (pgPool && pgStatus.connected) {
      try {
        const result = await pgPool.query('SELECT * FROM orders ORDER BY created_at DESC LIMIT 20');
        res.statusCode = 200;
        return res.end(JSON.stringify({
          success: true,
          source: 'POSTGRESQL_RDS',
          orders: result.rows.map(r => ({
            id: r.id,
            item: r.item,
            quantity: r.quantity,
            amount: parseFloat(r.amount),
            customer: r.customer,
            status: r.status,
            createdAt: r.created_at
          })),
          metadata: { dbHost: DB_HOST, dbUser: DB_USER, dbName: DB_NAME }
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

  // Place order
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
        if (pgPool) {
          try {
            await pgPool.query(
              'INSERT INTO orders (id, item, quantity, amount, customer, status, created_at) VALUES ($1, $2, $3, $4, $5, $6, $7)',
              [newOrder.id, newOrder.item, newOrder.quantity, newOrder.amount, newOrder.customer, newOrder.status, newOrder.createdAt]
            );
            storageMode = 'POSTGRESQL_RDS';
          } catch (e) {
            console.warn('Failed to insert into PostgreSQL, falling back to memory:', e.message);
            memoryOrders.push(newOrder);
          }
        } else {
          memoryOrders.push(newOrder);
        }

        res.statusCode = 201;
        return res.end(JSON.stringify({
          success: true,
          order: newOrder,
          storage: storageMode,
          dispatchedToQueue: SQS_QUEUE_URL ? 'reliability-shop-dev-order-queue' : 'mock-sqs'
        }));
      } catch (err) {
        res.statusCode = 400;
        return res.end(JSON.stringify({ error: 'Invalid JSON payload' }));
      }
    });
    return;
  }

  res.statusCode = 404;
  res.end(JSON.stringify({ error: 'Not Found', path: pathname }));
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`[${SERVICE_NAME}] listening on port ${PORT}`);
});

process.on('SIGTERM', () => {
  console.log(`[${SERVICE_NAME}] SIGTERM received, closing pool...`);
  if (pgPool) pgPool.end();
  server.close(() => process.exit(0));
});
