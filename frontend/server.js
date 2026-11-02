const http = require('http');
const fs = require('fs');
const path = require('path');
const url = require('url');

const PORT = process.env.PORT || 8080;
const ORDER_SERVICE_URL = process.env.ORDER_SERVICE_URL || 'http://order-service:8080';
const INVENTORY_SERVICE_URL = process.env.INVENTORY_SERVICE_URL || 'http://inventory-service:8080';
const AUTH_SERVICE_URL = process.env.AUTH_SERVICE_URL || 'http://auth-service:8080';

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

const server = http.createServer((req, res) => {
  const parsed = url.parse(req.url, true);
  const { pathname } = parsed;

  if (pathname === '/health') {
    res.statusCode = 200;
    res.setHeader('Content-Type', 'application/json');
    return res.end(JSON.stringify({
      status: 'UP',
      service: 'frontend',
      uptime: process.uptime(),
      timestamp: new Date().toISOString()
    }));
  }

  if (pathname === '/ready') {
    res.statusCode = 200;
    res.setHeader('Content-Type', 'application/json');
    return res.end(JSON.stringify({ status: 'READY', service: 'frontend' }));
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
