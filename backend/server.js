// 1. Core environment configuration MUST load before anything else
import * as dotenv from 'dotenv';
dotenv.config();

// 2. Import core server and database dependencies
import Fastify from 'fastify';
import { Queue } from 'bullmq';
import IORedis from 'ioredis';
import fastifyCors from '@fastify/cors';
import pg from 'pg';

// Import Prisma 7 Client and the native Postgres Adapter
import { PrismaClient } from '@prisma/client';
import { PrismaPg } from '@prisma/adapter-pg';

// Setup the database connection pool connection array
const pool = new pg.Pool({ connectionString: process.env.DATABASE_URL });
const adapter = new PrismaPg(pool);

// Initialize Prisma 7 with the direct local adapter
const prisma = new PrismaClient({ adapter });

const fastify = Fastify({
  logger: true
});

// Register CORS plugin cleanly for Fastify
await fastify.register(fastifyCors, {
  origin: 'http://localhost:3001'
});

// Configure a dedicated connection manager to link cleanly with our Docker Redis service
const redisConnection = new IORedis({
  host: 'localhost',
  port: 6379,
  maxRetriesPerRequest: null
});

// Initialize the persistent background queue structure
const transcriptQueue = new Queue('transcript-processing', {
  connection: redisConnection
});

// Core verification route
fastify.get('/', async (request, reply) => {
  return { status: 'GTM API Gateway Active' };
});

// GET Endpoint to fetch live database syncs for our frontend matrix
fastify.get('/api/v1/jobs', async (request, reply) => {
  try {
    const records = await prisma.callSummary.findMany({
      orderBy: {
        createdAt: 'desc'
      },
      take: 20
    });
    return records;
  } catch (error) {
    fastify.log.error(error, 'Failed to pull pipeline database records');
    reply.code(500);
    return { error: 'Internal pipeline database fetch failure.' };
  }
});

// Inbound Webhook Endpoint integrated directly with the Redis queue
fastify.post('/api/v1/webhooks/gong', async (request, reply) => {
  const payload = request.body;

  try {
    const job = await transcriptQueue.add('process-gong-raw', {
      callId: payload.callId || 'unknown_id',
      transcriptData: payload.transcript || ''
    });

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