import Fastify from 'fastify';
import * as dotenv from 'dotenv';
import { Queue } from 'bullmq';
import IORedis from 'ioredis';

// Load environment configuration variables
dotenv.config();

const fastify = Fastify({
  logger: true
});

// Configure a dedicated connection manager to link cleanly with our Docker Redis service
const redisConnection = new IORedis({
  host: 'localhost',
  port: 6379,
  maxRetriesPerRequest: null // This flag is strictly required by BullMQ framework architectural standards
});

// Initialize the persistent background queue structure
const transcriptQueue = new Queue('transcript-processing', {
  connection: redisConnection
});

// Core verification route
fastify.get('/', async (request, reply) => {
  return { status: 'GTM API Gateway Active' };
});

// Inbound Webhook Endpoint integrated directly with the Redis queue
fastify.post('/api/v1/webhooks/gong', async (request, reply) => {
  const payload = request.body;

  try {
    // Safely drop the raw webhook payload straight into the persistent Redis Queue data block
    const job = await transcriptQueue.add('process-gong-raw', {
      callId: payload.callId || 'unknown_id',
      transcriptData: payload.transcript || ''
    });

    // Log the successful background delegation
    fastify.log.info({ jobId: job.id }, 'Webhook parsed and handed off to background worker queue');

    reply.code(202);
    return { 
      status: 'success', 
      message: 'Transcript payload safely enqueued',
      jobId: job.id
    };
  } catch (error) {
    fastify.log.error(error, 'Failed to inject webhook event payload into Redis storage layer');
    reply.code(500);
    return { status: 'error', message: 'Internal Queue Storage Handoff Failure' };
  }
});

const start = async () => {
  try {
    await fastify.listen({ port: 3000, host: '0.0.0.0' });
    console.log('Server is running seamlessly on http://localhost:3000');
  } catch (err) {
    fastify.log.error(err);
    process.exit(1);
  }
};

start();