const assert = require('node:assert/strict');
const test = require('node:test');
const { buildOrderEvent, dispatchOutboxBatch, validateOrderInput } = require('./core');

test('validateOrderInput accepts the API contract', () => {
  assert.deepEqual(validateOrderInput({ item: 'Redis', quantity: 2, amount: 49, customer: 'SRE' }), {
    item: 'Redis', quantity: 2, amount: 49, customer: 'SRE'
  });
});

test('validateOrderInput rejects invalid amounts', () => {
  assert.throws(() => validateOrderInput({ item: 'Redis', quantity: 1, amount: -1, customer: 'SRE' }), /amount/);
});

test('buildOrderEvent produces a versioned traceable event', () => {
  const event = buildOrderEvent(
    { id: 'ord-1', item: 'SQS', quantity: 1, amount: 10, customer: 'SRE' },
    { eventId: 'event-1', traceId: 'trace-1', now: new Date('2026-11-03T00:00:00Z') }
  );
  assert.equal(event.eventId, 'event-1');
  assert.equal(event.orderId, 'ord-1');
  assert.equal(event.schemaVersion, 1);
  assert.equal(event.traceId, 'trace-1');
});

test('dispatchOutboxBatch leaves failed events pending', async () => {
  const statements = [];
  const client = {
    async query(sql) {
      statements.push(sql);
      if (sql.includes('SELECT event_id')) return { rows: [{ event_id: 'event-1', payload: { eventId: 'event-1' } }] };
      return { rows: [], rowCount: 1 };
    },
    release() {}
  };
  const result = await dispatchOutboxBatch({
    pool: { async connect() { return client; } },
    async sendEvent() { throw new Error('SQS unavailable'); }
  });
  assert.deepEqual(result, { published: 0, failed: 1 });
  assert.ok(statements.some(sql => sql.includes('next_attempt_at')));
  assert.ok(statements.includes('COMMIT'));
});
