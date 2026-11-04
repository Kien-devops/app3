const http = require('http');
const fs = require('fs');
const path = require('path');
const url = require('url');

const PORT = process.env.PORT || 8080;
const ORDER_SERVICE_URL = process.env.ORDER_SERVICE_URL || 'http://order-service:8080';
const INVENTORY_SERVICE_URL = process.env.INVENTORY_SERVICE_URL || 'http://inventory-service:8080';
const AUTH_SERVICE_URL = process.env.AUTH_SERVICE_URL || 'http://auth-service:8080';
const WORKER_SERVICE_URL = process.env.WORKER_SERVICE_URL || 'http://payment-worker:8080';

function getJson(targetBaseUrl, pathname) {
  return new Promise(resolve => {
    const targetUrl = new URL(pathname, targetBaseUrl);
    const request = http.get(targetUrl, { timeout: 3000 }, response => {
      let body = '';
      response.setEncoding('utf8');
      response.on('data', chunk => { body += chunk; });
      response.on('end', () => {
        try {
          resolve({ ok: response.statusCode >= 200 && response.statusCode < 300, data: JSON.parse(body) });
        } catch (_error) {
          resolve({ ok: false, error: 'Invalid JSON response' });
        }
      });
    });

    request.on('timeout', () => request.destroy(new Error('Request timeout')));
    request.on('error', error => resolve({ ok: false, error: error.message }));
  });
}

function proxyRequest(targetBaseUrl, req, res) {
  const targetUrl = new URL(req.url, targetBaseUrl);
  const options = {
    hostname: targetUrl.hostname,
    port: targetUrl.port || 80,
    path: targetUrl.pathname + targetUrl.search,
    method: req.method,
    headers: { ...req.headers, host: targetUrl.host }
  };

  const proxyReq = http.request(options, proxyRes => {
    res.writeHead(proxyRes.statusCode, proxyRes.headers);
    proxyRes.pipe(res, { end: true });
  });

  proxyReq.on('error', err => {
    console.error(`Proxy error to ${targetBaseUrl}:`, err.message);
    res.statusCode = 502;
    res.setHeader('Content-Type', 'application/json');
    res.end(JSON.stringify({ error: 'Bad Gateway', details: err.message }));
  });

  req.pipe(proxyReq, { end: true });
}

const server = http.createServer(async (req, res) => {
  const parsed = url.parse(req.url, true);
  const { pathname } = parsed;

  if (pathname === '/health') {
    res.statusCode = 200;
    res.setHeader('Content-Type', 'application/json');
    return res.end(JSON.stringify({
      status: 'UP',
      service: 'frontend',
      version: 'v1.1.0',
      uptime: process.uptime(),
      timestamp: new Date().toISOString()
    }));
  }

  if (pathname === '/ready') {
    res.statusCode = 200;
    res.setHeader('Content-Type', 'application/json');
    return res.end(JSON.stringify({ status: 'READY', service: 'frontend' }));
  }

  if (pathname === '/api/system/health' && req.method === 'GET') {
    const [order, inventory, auth, worker] = await Promise.all([
      getJson(ORDER_SERVICE_URL, '/api/orders/status'),
      getJson(INVENTORY_SERVICE_URL, '/health'),
      getJson(AUTH_SERVICE_URL, '/health'),
      getJson(WORKER_SERVICE_URL, '/api/worker/stats')
    ]);

    res.statusCode = 200;
    res.setHeader('Content-Type', 'application/json');
    return res.end(JSON.stringify({
      checkedAt: new Date().toISOString(),
      services: {
        frontend: { ok: true, status: 'UP' },
        order: {
          ok: order.ok,
          status: order.ok ? 'UP' : 'UNKNOWN',
          dbConnected: order.data?.db?.connected === true,
          redisConnected: order.data?.redis?.connected === true,
          sqsConfigured: order.data?.sqs?.configured === true,
          sqsConnected: order.data?.sqs?.connected === true,
          outboxPending: order.data?.outbox?.pending ?? null,
          error: order.error
        },
        inventory: { ok: inventory.ok, status: inventory.data?.status || 'UNKNOWN', error: inventory.error },
        auth: { ok: auth.ok, status: auth.data?.status || 'UNKNOWN', error: auth.error },
        worker: {
          ok: worker.ok && worker.data?.isPolling === true,
          status: worker.ok && worker.data?.isPolling === true ? 'UP' : 'UNKNOWN',
          workerState: worker.data?.isPolling ? 'POLLING' : 'NOT_POLLING',
          messagesProcessed: worker.data?.messagesProcessed ?? null,
          lastProcessed: worker.data?.lastProcessed || null,
          error: worker.error
        }
      }
    }));
  }

  // Reverse proxy routing to internal microservices
  if (pathname.startsWith('/api/orders')) {
    return proxyRequest(ORDER_SERVICE_URL, req, res);
  }

  if (pathname.startsWith('/api/inventory')) {
    return proxyRequest(INVENTORY_SERVICE_URL, req, res);
  }

  if (pathname.startsWith('/api/auth')) {
    return proxyRequest(AUTH_SERVICE_URL, req, res);
  }

  if (pathname.startsWith('/api/worker')) {
    return proxyRequest(WORKER_SERVICE_URL, req, res);
  }

  // Serve static UI
  const filePath = path.join(__dirname, 'public', pathname === '/' ? 'index.html' : pathname);
  fs.readFile(filePath, (err, content) => {
    if (err) {
      // Fallback to index.html for SPA
      fs.readFile(path.join(__dirname, 'public', 'index.html'), (err2, fallback) => {
        if (err2) {
          res.statusCode = 404;
          return res.end('Not Found');
        }
        res.setHeader('Content-Type', 'text/html');
        res.end(fallback);
      });
      return;
    }
    res.setHeader('Content-Type', 'text/html');
    res.end(content);
  });
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`[frontend] SRE Storefront listening on port ${PORT}`);
});

process.on('SIGTERM', () => {
  console.log('[frontend] SIGTERM received, draining connections...');
  server.close(() => process.exit(0));
});

