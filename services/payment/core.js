function parseOrderEvent(body, messageId) {
  let event;
  try {
    event = JSON.parse(body || '');
  } catch {
    throw new TypeError('message body must be valid JSON');
  }
  if (!event || typeof event !== 'object') throw new TypeError('message body must be an object');

  const orderId = event.orderId || event.id;
  if (typeof orderId !== 'string' || orderId.length === 0 || orderId.length > 64) {
    throw new TypeError('message requires a valid orderId');
  }
  const eventId = event.eventId || (messageId ? `legacy-${messageId}` : null);
  if (!eventId || eventId.length > 128) throw new TypeError('message requires a valid eventId');
  if (event.eventType && event.eventType !== 'ORDER_CREATED') throw new TypeError('unsupported eventType');
  if (event.schemaVersion && Number(event.schemaVersion) !== 1) throw new TypeError('unsupported schemaVersion');

  return {
    eventId,
    eventType: event.eventType || 'ORDER_CREATED',
    schemaVersion: Number(event.schemaVersion || 1),
    orderId,
    traceId: event.traceId || null
  };
}

async function processOrderEvent({ pool, body, messageId }) {
  if (!pool) throw new Error('database unavailable');
  const event = parseOrderEvent(body, messageId);
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const inserted = await client.query(
      `INSERT INTO processed_events (event_id, order_id, processed_at)
       VALUES ($1, $2, NOW())
       ON CONFLICT (event_id) DO NOTHING
       RETURNING event_id`,
      [event.eventId, event.orderId]
    );
    if (inserted.rowCount === 0) {
      const existing = await client.query(
        'SELECT order_id FROM processed_events WHERE event_id = $1',
        [event.eventId]
      );
      if (existing.rowCount !== 1 || existing.rows[0].order_id !== event.orderId) {
        throw new Error('eventId already belongs to another order');
      }
      await client.query('COMMIT');
      return { duplicate: true, event };
    }

    const updated = await client.query(
      `UPDATE orders
       SET status = 'PAID', payment_processed_at = NOW(), updated_at = NOW()
       WHERE id = $1 AND status = 'PENDING_PAYMENT'
       RETURNING id, status, payment_processed_at`,
      [event.orderId]
    );
    if (updated.rowCount !== 1) {
      const current = await client.query('SELECT status FROM orders WHERE id = $1', [event.orderId]);
      if (current.rowCount === 1 && ['PAID', 'PAID_VIA_SQS_WORKER'].includes(current.rows[0].status)) {
        await client.query('COMMIT');
        return { duplicate: true, event };
      }
      throw new Error(`order not payable: ${event.orderId}`);
    }
    await client.query('COMMIT');
    return { duplicate: false, event, order: updated.rows[0] };
  } catch (error) {
    await client.query('ROLLBACK').catch(() => {});
    throw error;
  } finally {
    client.release();
  }
}

module.exports = { parseOrderEvent, processOrderEvent };
