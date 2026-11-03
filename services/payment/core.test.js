const assert = require('node:assert/strict');
const test = require('node:test');
const { parseOrderEvent, processOrderEvent } = require('./core');

test('parseOrderEvent validates the event contract', () => {
  const event = parseOrderEvent(JSON.stringify({
    eventId: 'event-1', eventType: 'ORDER_CREATED', schemaVersion: 1, orderId: 'ord-1'
  }), 'message-1');
  assert.equal(event.eventId, 'event-1');
  assert.equal(event.orderId, 'ord-1');
});

test('parseOrderEvent rejects poison messages', () => {
  assert.throws(() => parseOrderEvent('{broken', 'message-1'), /valid JSON/);
  assert.throws(() => parseOrderEvent('{}', 'message-1'), /orderId/);
});

test('processOrderEvent rolls back when the order does not exist', async () => {
  const statements = [];
  const client = {
    async query(sql) {
      statements.push(sql);
      if (sql.includes('INSERT INTO processed_events')) return { rowCount: 1, rows: [{ event_id: 'event-1' }] };
      if (sql.includes('UPDATE orders')) return { rowCount: 0, rows: [] };
      return { rowCount: 0, rows: [] };
    },
    release() {}
  };
  await assert.rejects(
    processOrderEvent({
      pool: { async connect() { return client; } },
      body: JSON.stringify({ eventId: 'event-1', orderId: 'ord-missing' }),
      messageId: 'message-1'
    }),
    /order not payable/
  );
  assert.ok(statements.includes('ROLLBACK'));
  assert.ok(!statements.includes('COMMIT'));
});

test('processOrderEvent acknowledges duplicates without updating twice', async () => {
  const statements = [];
  const client = {
    async query(sql) {
      statements.push(sql);
      if (sql.includes('INSERT INTO processed_events')) return { rowCount: 0, rows: [] };
      if (sql.includes('SELECT order_id')) return { rowCount: 1, rows: [{ order_id: 'ord-1' }] };
      return { rowCount: 0, rows: [] };
    },
    release() {}
  };
  const result = await processOrderEvent({
    pool: { async connect() { return client; } },
    body: JSON.stringify({ eventId: 'event-1', orderId: 'ord-1' }),
    messageId: 'message-1'
  });
  assert.equal(result.duplicate, true);
  assert.ok(!statements.some(sql => sql.includes('UPDATE orders')));
  assert.ok(statements.includes('COMMIT'));
});

test('processOrderEvent tolerates the legacy paid status during rollout overlap', async () => {
  const statements = [];
  const client = {
    async query(sql) {
      statements.push(sql);
      if (sql === 'BEGIN' || sql === 'COMMIT') return { rowCount: 0, rows: [] };
      if (sql.includes('INSERT INTO processed_events')) return { rowCount: 1, rows: [{ event_id: 'event-legacy' }] };
      if (sql.includes('UPDATE orders')) return { rowCount: 0, rows: [] };
      if (sql.includes('SELECT status FROM orders')) return { rowCount: 1, rows: [{ status: 'PAID_VIA_SQS_WORKER' }] };
      throw new Error(`unexpected query: ${sql}`);
    },
    release() {}
  };
  const pool = { async connect() { return client; } };
  const result = await processOrderEvent({
    pool,
    body: JSON.stringify({ eventId: 'event-legacy', orderId: 'ord-1', eventType: 'ORDER_CREATED', schemaVersion: 1 })
  });
  assert.equal(result.duplicate, true);
  assert.ok(statements.includes('COMMIT'));
});
