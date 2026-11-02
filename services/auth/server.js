const http = require('http');
const url = require('url');

const PORT = process.env.PORT || 8080;
const JWT_SECRET = process.env.JWT_SECRET || 'dev-fallback-secret-2026';
const SERVICE_NAME = 'auth-service';

function createToken(payload) {
  const header = Buffer.from(JSON.stringify({ alg: 'HS256', typ: 'JWT' })).toString('base64url');
  const body = Buffer.from(JSON.stringify(payload)).toString('base64url');
  const crypto = require('crypto');
  const signature = crypto.createHmac('sha256', JWT_SECRET).update(`${header}.${body}`).digest('base64url');
  return `${header}.${body}.${signature}`;
}

function verifyToken(token) {
  try {
    const [header, body, signature] = token.split('.');
    const crypto = require('crypto');
    const expected = crypto.createHmac('sha256', JWT_SECRET).update(`${header}.${body}`).digest('base64url');
    if (signature !== expected) return null;
    return JSON.parse(Buffer.from(body, 'base64url').toString('utf8'));
  } catch {
    return null;
  }
}

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
      secretConfigured: Boolean(process.env.JWT_SECRET),
      timestamp: new Date().toISOString()
    }));
  }

  if (pathname === '/ready') {
    res.statusCode = 200;
    return res.end(JSON.stringify({ status: 'READY', service: SERVICE_NAME }));
  }

  // Auth Endpoints
  if (pathname === '/api/auth/login' && req.method === 'POST') {
    let body = '';
    req.on('data', chunk => { body += chunk; });
    req.on('end', () => {
      try {
        const data = body ? JSON.parse(body) : {};
        const username = data.username || 'sre_user';
        const role = data.role || 'viewer';

        const token = createToken({
          sub: username,
          role,
          iat: Math.floor(Date.now() / 1000),
          exp: Math.floor(Date.now() / 1000) + 3600
        });

        res.statusCode = 200;
        return res.end(JSON.stringify({
          success: true,
          token,
          user: { username, role },
          secretLoaded: process.env.JWT_SECRET ? 'FROM_ESO_SECRET' : 'DEFAULT'
        }));
      } catch (err) {
        res.statusCode = 400;
        return res.end(JSON.stringify({ error: 'Invalid JSON payload' }));
      }
    });
    return;
  }

  if (pathname === '/api/auth/verify' && req.method === 'GET') {
    const authHeader = req.headers.authorization;
    if (!authHeader || !authHeader.startsWith('Bearer ')) {
      res.statusCode = 401;
      return res.end(JSON.stringify({ error: 'Missing or invalid token' }));
    }
    const token = authHeader.split(' ')[1];
    const user = verifyToken(token);
    if (!user) {
      res.statusCode = 403;
      return res.end(JSON.stringify({ error: 'Invalid or expired signature' }));
    }
    res.statusCode = 200;
    return res.end(JSON.stringify({ valid: true, user }));
  }

  res.statusCode = 404;
  res.end(JSON.stringify({ error: 'Not Found', path: pathname }));
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`[${SERVICE_NAME}] listening on port ${PORT}`);
});

// Graceful Shutdown
process.on('SIGTERM', () => {
  console.log(`[${SERVICE_NAME}] SIGTERM received, draining connections...`);
  server.close(() => {
    console.log(`[${SERVICE_NAME}] process terminated cleanly.`);
    process.exit(0);
  });
});

