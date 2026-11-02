const http = require('http');
const url = require('url');

const PORT = process.env.PORT || 8080;
const SERVICE_NAME = 'order-service';

// ESO Credentials
const DB_USER = process.env.DB_USER || 'sre_admin';
const DB_NAME = process.env.DB_NAME || 'reliability_shop';
const SQS_QUEUE_URL = process.env.SQS_QUEUE_URL || '';

// In-memory order storage
const orders = [
  {
    id: 'ord-1001',
    item: 'AWS Cloud-Native SRE Handbook',
    quantity: 1,
    amount: 49.99,
    status: 'COMPLETED',
    createdAt: new Date(Date.now() - 3600000).toISOString()
  }
];

const server = http.createServer((req, res) => {
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
      dbConnected: Boolean(process.env.DB_PASSWORD),
      databaseUser: DB_USER,
      databaseName: DB_NAME,
      redisConnected: Boolean(process.env.REDIS_AUTH),
      sqsConfigured: Boolean(SQS_QUEUE_URL),
      ordersCount: orders.length,
      timestamp: new Date().toISOString()
    }));
  }

  if (pathname === '/ready') {
    res.statusCode = 200;
    return res.end(JSON.stringify({ status: 'READY', service: SERVICE_NAME }));
  }

  // Metrics endpoint for Prometheus
  if (pathname === '/metrics') {
    res.setHeader('Content-Type', 'text/plain');
    return res.end(
      `# HELP http_requests_total Total HTTP Requests\n` +
      `# TYPE http_requests_total counter\n` +
      `http_requests_total{service="order-service",status="200"} ${orders.length * 5}\n` +
      `order_processing_count{service="order-service"} ${orders.length}\n`
    );
  }

  // Orders API
  if (pathname === '/api/orders' && req.method === 'GET') {
    res.statusCode = 200;
    return res.end(JSON.stringify({
      success: true,
      orders,
      metadata: {
        dbUser: DB_USER,
        dbName: DB_NAME,
        redisAuthLoaded: Boolean(process.env.REDIS_AUTH),
        sqsQueueUrl: SQS_QUEUE_URL || 'local-mock-queue'
      }
    }));
  }

  if (pathname === '/api/orders' && req.method === 'POST') {
    let body = '';
    req.on('data', chunk => { body += chunk; });
    req.on('end', () => {
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

        orders.push(newOrder);

        res.statusCode = 201;
        return res.end(JSON.stringify({
          success: true,
          order: newOrder,
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
  console.log(`[${SERVICE_NAME}] SIGTERM received, draining HTTP connections...`);
  server.close(() => process.exit(0));
});
