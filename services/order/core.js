const { randomUUID } = require('crypto');

const ORDER_CACHE_KEY = 'reliabilityshop:orders:list';

function requireString(value, name, maxLength) {
  if (typeof value !== 'string' || value.trim().length === 0) {
    throw new TypeError(`${name} must be a non-empty string`);
  }
  const normalized = value.trim();
  if (normalized.length > maxLength) {
    throw new TypeError(`${name} must be at most ${maxLength} characters`);
  }
  return normalized;
}

function validateOrderInput(payload = {}) {
  const quantity = Number(payload.quantity ?? 1);
  const amount = Number(payload.amount);

  if (!Number.isInteger(quantity) || quantity < 1 || quantity > 1000) {
    throw new TypeError('quantity must be an integer between 1 and 1000');
  }
  if (!Number.isFinite(amount) || amount <= 0 || amount > 1000000) {
    throw new TypeError('amount must be a number between 0 and 1000000');
  }

  return {
    item: requireString(payload.item, 'item', 255),
    quantity,
    amount,
    customer: requireString(payload.customer, 'customer', 255)
  };
}

function buildOrderEvent(order, { eventId = randomUUID(), traceId = randomUUID(), now = new Date() } = {}) {
  return {
    eventId,
    eventType: 'ORDER_CREATED',
    schemaVersion: 1,
    orderId: order.id,
    occurredAt: now.toISOString(),
    traceId,
    data: {
      item: order.item,
      quantity: order.quantity,
      amount: order.amount,
      customer: order.customer
    }
  };
}

function mapOrderRow(row) {
  return {
    id: row.id,
    item: row.item,
    quantity: Number(row.quantity),
    amount: Number(row.amount),
    customer: row.customer,
    status: row.status,
    createdAt: row.created_at,
    eventId: row.event_id || null,
    eventStatus: row.event_status || 'UNKNOWN',
    eventPublishedAt: row.event_published_at || null,
    paymentProcessedAt: row.payment_processed_at || null
  };
}

async function createOrderWithOutbox({
  pool,
  payload,
  idempotencyKey,
  traceId,
  uuid = randomUUID,
  now = () => new Date()
}) {
  if (!pool) throw new Error('database unavailable');
  const input = validateOrderInput(payload);
  const orderId = `ord-${uuid()}`;
  const eventId = uuid();
  const createdAt = now();
  const order = {
    id: orderId,
    ...input,
    status: 'PENDING_PAYMENT',
    createdAt: createdAt.toISOString()
  };
  const event = buildOrderEvent(order, { eventId, traceId, now: createdAt });
  const client = await pool.connect();

  try {
    await client.query('BEGIN');
    const inserted = await client.query(
      `INSERT INTO orders
        (id, item, quantity, amount, customer, status, created_at, idempotency_key)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
       ON CONFLICT DO NOTHING
       RETURNING *`,
      [order.id, order.item, order.quantity, order.amount, order.customer, order.status, order.createdAt, idempotencyKey]
    );

    if (inserted.rowCount === 0) {
      const existing = await client.query(
        `SELECT o.*, x.event_id, x.status AS event_status, x.published_at AS event_published_at
         FROM orders o
         LEFT JOIN LATERAL (
           SELECT event_id, status, published_at
           FROM order_outbox
           WHERE aggregate_id = o.id
           ORDER BY created_at DESC
           LIMIT 1
         ) x ON TRUE
         WHERE o.idempotency_key = $1`,
        [idempotencyKey]
      );
      if (existing.rowCount !== 1) throw new Error('idempotent order lookup failed');
      await client.query('COMMIT');
      return { order: mapOrderRow(existing.rows[0]), duplicate: true };
    }

    await client.query(
      `INSERT INTO order_outbox
        (event_id, aggregate_id, event_type, schema_version, payload, status, created_at, next_attempt_at)
       VALUES ($1, $2, $3, $4, $5::jsonb, 'PENDING', $6, $6)`,
      [event.eventId, order.id, event.eventType, event.schemaVersion, JSON.stringify(event), order.createdAt]
    );
    await client.query('COMMIT');
    return {
      order: {
        ...order,
        eventId,
        eventStatus: 'PENDING',
        eventPublishedAt: null,
        paymentProcessedAt: null
      },
      duplicate: false,
      event
    };
  } catch (error) {
    await client.query('ROLLBACK').catch(() => {});
    throw error;
  } finally {
    client.release();
  }
}

async function dispatchOutboxBatch({ pool, sendEvent, batchSize = 5 }) {
  if (!pool || !sendEvent) return { published: 0, failed: 0 };
  const client = await pool.connect();
  let published = 0;
  let failed = 0;

  try {
    await client.query('BEGIN');
    const pending = await client.query(
      `SELECT event_id, payload
       FROM order_outbox
       WHERE status = 'PENDING' AND next_attempt_at <= NOW()
       ORDER BY created_at
       FOR UPDATE SKIP LOCKED
       LIMIT $1`,
      [batchSize]
    );

    for (const row of pending.rows) {
      try {
        const result = await sendEvent(row.payload);
        await client.query(
          `UPDATE order_outbox
           SET status = 'PUBLISHED', published_at = NOW(), message_id = $2,
               attempts = attempts + 1, last_error = NULL
           WHERE event_id = $1`,
          [row.event_id, result.messageId || null]
        );
        published += 1;
      } catch (error) {
        await client.query(
          `UPDATE order_outbox
           SET attempts = attempts + 1, last_error = LEFT($2, 1000),
               next_attempt_at = NOW() + make_interval(secs => LEAST(300, POWER(2, LEAST(attempts + 1, 8))::int))
           WHERE event_id = $1`,
          [row.event_id, error.message]
        );
        failed += 1;
      }
    }

    await client.query('COMMIT');
    return { published, failed };
  } catch (error) {
    await client.query('ROLLBACK').catch(() => {});
    throw error;
  } finally {
    client.release();
  }
}

module.exports = {
  ORDER_CACHE_KEY,
  buildOrderEvent,
  createOrderWithOutbox,
  dispatchOutboxBatch,
  mapOrderRow,
  validateOrderInput
};
