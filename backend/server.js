import Fastify from 'fastify';
import fastifyCors from '@fastify/cors';
import { PrismaClient } from '@prisma/client';
import pg from 'pg';
import { PrismaPg } from '@prisma/adapter-pg';
import { Queue } from 'bullmq';
import IORedis from 'ioredis';

// Initialize Fastify framework instance
const fastify = Fastify({
  logger: true
});

// 1. Initialize your raw PostgreSQL connection pool driver instance
const pool = new pg.Pool({ connectionString: process.env.DATABASE_URL });
const adapter = new PrismaPg(pool);

// 2. Pass the adapter driver wrapper into PrismaClient
const prisma = new PrismaClient({
  adapter: adapter,
  log: ['query', 'info', 'warn', 'error'],
});

// 3. Initialize BullMQ Queue for background workers
const redisConnection = new IORedis({
  host: '127.0.0.1',
  port: 6379,
  maxRetriesPerRequest: null
});

const transcriptQueue = new Queue('transcript-processing', {
  connection: redisConnection
});

// SSE clients registry
const clients = new Set();

// Dedicated PostgreSQL client for LISTEN/NOTIFY
const listenClient = new pg.Client({ connectionString: process.env.DATABASE_URL });

// Register CORS plugin cleanly to accept Next.js connections from port 3001
await fastify.register(fastifyCors, {
  origin: 'http://localhost:3001',
  methods: ['GET', 'POST', 'OPTIONS'],
  allowedHeaders: ['Content-Type']
});

/**
 * -----------------------------------------------------------------------------
 * CORE ROUTING TABLE MATRIX
 * -----------------------------------------------------------------------------
 */

// 1. Root Verification Endpoint
fastify.get('/', async (request, reply) => {
  return { status: 'GTM API Gateway Active' };
});

// Remove status: true from this block
fastify.get('/api/v1/jobs', async (request, reply) => {
  try {
    const jobs = await prisma.callSummary.findMany({
      orderBy: {
        createdAt: 'desc'
      },
      select: {
        id: true,
        callId: true,
        status: true,
        outboundTriggered: true,
        createdAt: true,
        rawTranscript: true,
        aiAnalysisPass: true
      }
    });
    return jobs;
  } catch (error) {
    fastify.log.error(error);
    return reply.status(500).send({
      status: 'FAULTED',
      message: 'DATABASE REFUSAL: Failed to fetch pipeline rows.'
    });
  }
});

// Remove status: true from this block too
fastify.get('/api/v1/jobs/:id', async (request, reply) => {
  const { id } = request.params;
  
  try {
    const job = await prisma.callSummary.findUnique({
      where: { id: id },
      select: {
        id: true,
        callId: true,
        status: true,
        aiAnalysisPass: true, 
        createdAt: true
      }
    });

    if (!job) {
      return reply.status(404).send({
        status: 'ERROR',
        message: `RECORD NOT FOUND: UUID #${id} could not be resolved.`
      });
    }

    return job;
  } catch (error) {
    fastify.log.error(error);
    return reply.status(500).send({
      status: 'FAULTED',
      message: 'DATABASE ERROR: Failed to fetch log summary.'
    });
  }
});

// 3. SSE Stream Endpoint for Real-time Updates
fastify.get('/api/v1/jobs/stream', (request, reply) => {
  reply.raw.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
    'Access-Control-Allow-Origin': 'http://localhost:3001',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Access-Control-Allow-Credentials': 'true'
  });

  reply.raw.write('retry: 10000\n\n');
  clients.add(reply.raw);
  
  console.log(`SSE Client connected. Total clients: ${clients.size}`);

  const keepAlive = setInterval(() => {
    reply.raw.write(': keep-alive\n\n');
  }, 15000);

  request.raw.on('close', () => {
    clearInterval(keepAlive);
    clients.delete(reply.raw);
    console.log(`SSE Client disconnected. Total clients: ${clients.size}`);
  });
});

// 4. POST Webhook Data Ingestion Route (Receives data stream & triggers BullMQ background handoff)
fastify.post('/api/v1/webhooks/gong', async (request, reply) => {
  const { callId, transcript } = request.body;

  if (!callId || !transcript) {
    return reply.status(400).send({
      status: 'REJECTED',
      message: 'MALFORMED PAYLOAD: Both callId and transcript parameters are mandatory.'
    });
  }

  try {
    // 1. Instantly stage basic execution frame to PostgreSQL via Prisma using upsert
    const newRecord = await prisma.callSummary.upsert({
      where: { callId: callId },
      update: {
        rawTranscript: transcript,
        status: 'PROCESSING',
        outboundTriggered: false,
        aiAnalysisPass: ''
      },
      create: {
        callId: callId,
        rawTranscript: transcript,
        status: 'PROCESSING',
        outboundTriggered: false,
        aiAnalysisPass: ''
      }
    });

    // 2. Handoff transaction to our background background queue worker node via Redis
    await transcriptQueue.add('process-gong-raw', {
      callId: callId,
      transcript: transcript
    });

    return reply.status(202).send({
      status: 'success',
      message: 'Transcript payload safely enqueued',
      jobId: newRecord.id
    });
  } catch (error) {
    fastify.log.error(error);
    return reply.status(500).send({
      status: 'FAULTED',
      message: 'INGESTION LAYER FAILURE: Internal server exception during background handoff sequence.'
    });
  }
});

// 5. POST Retry Ingestion Job Route (Resets state in PostgreSQL and re-enqueues back to BullMQ)
fastify.post('/api/v1/jobs/:id/retry', async (request, reply) => {
  const { id } = request.params;

  try {
    const job = await prisma.callSummary.findUnique({
      where: { id: id }
    });

    if (!job) {
      return reply.status(404).send({
        status: 'ERROR',
        message: `RECORD NOT FOUND: UUID #${id} could not be resolved.`
      });
    }

    // Reset status to PROCESSING, reset outboundTriggered and aiAnalysisPass
    const updatedRecord = await prisma.callSummary.update({
      where: { id: id },
      data: {
        status: 'PROCESSING',
        outboundTriggered: false,
        aiAnalysisPass: ''
      }
    });

    // Re-enqueue transaction to BullMQ background worker queue via Redis
    await transcriptQueue.add('process-gong-raw', {
      callId: job.callId,
      transcript: job.rawTranscript
    });

    return reply.status(200).send({
      status: 'success',
      message: 'Job re-enqueued successfully for analysis retry.',
      jobId: updatedRecord.id
    });
  } catch (error) {
    fastify.log.error(error);
    return reply.status(500).send({
      status: 'FAULTED',
      message: 'RETRY LAYER FAILURE: Internal server exception during retry dispatch sequence.'
    });
  }
});

/**
 * -----------------------------------------------------------------------------
 * ENGINE STARTUP PROCEDURES
 * -----------------------------------------------------------------------------
 */
async function setupPostgresListener() {
  try {
    await listenClient.connect();
    console.log('🔌 Connected dedicated pg client for LISTEN/NOTIFY');
    
    // Setup database triggers dynamically
    await listenClient.query(`
      CREATE OR REPLACE FUNCTION notify_job_change()
      RETURNS trigger AS $$
      DECLARE
        payload text;
      BEGIN
        payload := json_build_object(
          'id', NEW."id"
        )::text;
        PERFORM pg_notify('jobs_channel', payload);
        RETURN NEW;
      END;
      $$ LANGUAGE plpgsql;
    `);
    
    await listenClient.query(`
      DROP TRIGGER IF EXISTS job_change_trigger ON "CallSummary";
    `);
    
    await listenClient.query(`
      CREATE TRIGGER job_change_trigger
      AFTER INSERT OR UPDATE ON "CallSummary"
      FOR EACH ROW
      EXECUTE FUNCTION notify_job_change();
    `);
    
    console.log('✅ PostgreSQL trigger and trigger function configured.');

    await listenClient.query('LISTEN jobs_channel');
    console.log('👂 Listening to jobs_channel...');

    listenClient.on('notification', async (msg) => {
      if (msg.channel === 'jobs_channel' && msg.payload) {
        console.log('🔔 Received PG notify:', msg.payload);
        try {
          const minimalData = JSON.parse(msg.payload);
          const fullJob = await prisma.callSummary.findUnique({
            where: { id: minimalData.id },
            select: {
              id: true,
              callId: true,
              status: true,
              outboundTriggered: true,
              createdAt: true,
              rawTranscript: true,
              aiAnalysisPass: true
            }
          });
          if (fullJob) {
            const broadcastPayload = JSON.stringify(fullJob);
            for (const client of clients) {
              client.write(`data: ${broadcastPayload}\n\n`);
            }
          }
        } catch (err) {
          console.error('Error fetching full job details for broadcast:', err);
        }
      }
    });
    
    listenClient.on('error', async (err) => {
      console.error('❌ Postgres listener client error:', err);
      try {
        await listenClient.end();
      } catch {}
      setTimeout(setupPostgresListener, 5000);
    });
  } catch (err) {
    console.error('❌ Failed to setup Postgres listener:', err);
    setTimeout(setupPostgresListener, 5000);
  }
}

const startServer = async () => {
  try {
    await fastify.listen({ port: 3000, host: '0.0.0.0' });
    console.log('Server is running seamlessly on http://localhost:3000');
    setupPostgresListener();
  } catch (err) {
    fastify.log.error(err);
    process.exit(1);
  }
};

startServer();