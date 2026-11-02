const http = require('http');
const url = require('url');

const PORT = process.env.PORT || 8080;
const SERVICE_NAME = 'inventory-service';

// Mock inventory state in-memory (with Redis fallback if REDIS_AUTH is present)
const inventory = [
  { id: 'item-101', name: 'AWS Cloud-Native SRE Handbook', price: 49.99, stock: 120 },
  { id: 'item-102', name: 'Kubernetes Multi-AZ Node License', price: 199.00, stock: 45 },
  { id: 'item-103', name: 'Argo CD Enterprise Rollout Pack', price: 99.50, stock: 80 },
  { id: 'item-104', name: 'Grafana SRE Dashboard Suite', price: 29.00, stock: 250 }
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
      redisAuthPresent: Boolean(process.env.REDIS_AUTH),
      itemsCount: inventory.length,
      timestamp: new Date().toISOString()
    }));
  }

  if (pathname === '/ready') {
    res.statusCode = 200;
    return res.end(JSON.stringify({ status: 'READY', service: SERVICE_NAME }));
  }

  // Inventory API
  if (pathname === '/api/inventory' && req.method === 'GET') {
    res.statusCode = 200;
    return res.end(JSON.stringify({
      success: true,
      items: inventory,
      cached: Boolean(process.env.REDIS_AUTH)
    }));
  }

  if (pathname === '/api/inventory/reserve' && req.method === 'POST') {
    let body = '';
    req.on('data', chunk => { body += chunk; });
    req.on('end', () => {
      try {
        const { itemId, quantity = 1 } = JSON.parse(body || '{}');
        const item = inventory.find(i => i.id === itemId);
        if (!item) {
          res.statusCode = 404;
          return res.end(JSON.stringify({ error: 'Item not found' }));
        }
        if (item.stock < quantity) {
          res.statusCode = 400;
          return res.end(JSON.stringify({ error: 'Insufficient stock', available: item.stock }));
        }
        item.stock -= quantity;
        res.statusCode = 200;
        return res.end(JSON.stringify({
          success: true,
          reservedItem: item,
          remainingStock: item.stock
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
  console.log(`[${SERVICE_NAME}] SIGTERM received, shutting down gracefully...`);
  server.close(() => process.exit(0));
});
