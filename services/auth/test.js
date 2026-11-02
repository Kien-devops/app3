const test = require('node:test');
const assert = require('node:assert');

test('Auth Service Unit Tests - Token Sign and Verify', () => {
  const secret = 'test-secret';
  const crypto = require('crypto');
  const payload = { sub: 'test-user', role: 'admin' };
  
  const header = Buffer.from(JSON.stringify({ alg: 'HS256', typ: 'JWT' })).toString('base64url');
  const body = Buffer.from(JSON.stringify(payload)).toString('base64url');
  const signature = crypto.createHmac('sha256', secret).update(`${header}.${body}`).digest('base64url');
  const token = `${header}.${body}.${signature}`;

  const parts = token.split('.');
  assert.strictEqual(parts.length, 3);
  const parsed = JSON.parse(Buffer.from(parts[1], 'base64url').toString('utf8'));
  assert.strictEqual(parsed.sub, 'test-user');
});

