const http = require('http');

const PORT = process.env.PORT || 8080;
const SERVICE_NAME = 'payment-worker';
const SQS_QUEUE_URL = process.env.SQS_QUEUE_URL || 'https://sqs.ap-southeast-1.amazonaws.com/123456789012/reliability-shop-dev-order-queue';

let processedCount = 42;
let isRunning = true;

// Background polling loop
const pollInterval = setInterval(() => {
  if (!isRunning) return;
  // Simulating message consumption from SQS queue
  processedCount += 1;
}, 10000);

const server = http.createServer((req, res) => {
  res.setHeader('Content-Type', 'application/json');

  if (req.url === '/health' || req.url === '/ready') {
    res.statusCode = 200;
    return res.end(JSON.stringify({
      status: 'UP',
      service: SERVICE_NAME,
      workerState: isRunning ? 'POLLING' : 'DRAINING',
      sqsQueueUrl: SQS_QUEUE_URL,
      dbSecretLoaded: Boolean(process.env.DB_PASSWORD),
      messagesProcessed: processedCount,
      timestamp: new Date().toISOString()
    }));
  }

  res.statusCode = 404;
  res.end(JSON.stringify({ error: 'Worker endpoint not found' }));
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`[${SERVICE_NAME}] worker started on port ${PORT}, listening to ${SQS_QUEUE_URL}`);
});

process.on('SIGTERM', () => {
  console.log(`[${SERVICE_NAME}] SIGTERM received, stopping SQS consumer...`);
  isRunning = false;
  clearInterval(pollInterval);
  server.close(() => process.exit(0));
});
