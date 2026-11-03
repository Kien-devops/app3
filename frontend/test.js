const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const html = fs.readFileSync(path.join(__dirname, 'public', 'index.html'), 'utf8');
const server = fs.readFileSync(path.join(__dirname, 'server.js'), 'utf8');

test('dashboard uses read-only diagnostics and safe DOM rendering', () => {
  assert.doesNotMatch(html, /innerHTML/);
  assert.doesNotMatch(html, /\/api\/orders\/(redis-test|sqs-test|secrets-info)/);
  assert.match(html, /Luồng Order thực tế/);
  assert.match(html, />UNKNOWN</);
});

test('frontend exposes a read-only service health aggregator', () => {
  assert.match(server, /\/api\/system\/health/);
  assert.match(server, /\/api\/orders\/status/);
  assert.match(server, /\/api\/worker\/stats/);
  assert.match(server, /Promise\.all/);
  assert.match(server, /worker\.ok && worker\.data\?\.isPolling === true/);
});

test('order requests include a generated idempotency key', () => {
  assert.match(html, /function createIdempotencyKey\(\)/);
  assert.match(html, /'Idempotency-Key': idempotencyKey/);
});
