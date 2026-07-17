import Fastify from 'fastify';
import fastifyCors from '@fastify/cors';
import { PrismaClient } from '@prisma/client';
import pg from 'pg';
import { PrismaPg } from '@prisma/adapter-pg';
import { Queue } from 'bullmq';
import IORedis from 'ioredis';
import fastifyRateLimit from '@fastify/rate-limit';

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
  host: process.env.REDIS_HOST || '127.0.0.1',
  port: parseInt(process.env.REDIS_PORT || '6379', 10),
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
  origin: true,
  methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS']
});

// Register Rate Limit plugin globally
await fastify.register(fastifyRateLimit);

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
  
  fastify.log.info({ clientsCount: clients.size }, 'SSE Client connected.');

  const keepAlive = setInterval(() => {
    reply.raw.write(': ping\n\n');
  }, 15000);

  request.raw.on('close', () => {
    clearInterval(keepAlive);
    clients.delete(reply.raw);
    fastify.log.info({ clientsCount: clients.size }, 'SSE Client disconnected.');
  });
});

const gongSchema = {
  body: {
    type: 'object',
    required: ['callId', 'rawTranscript'],
    properties: {
      callId: { type: 'string', minLength: 1 },
      rawTranscript: { type: 'string', minLength: 1 }
    }
  }
};

// 4. POST Webhook Data Ingestion Route (Receives data stream & triggers BullMQ background handoff)
fastify.post('/api/v1/webhooks/gong', {
  config: {
    rateLimit: {
      max: 10,
      timeWindow: '1 minute'
    }
  },
  schema: gongSchema
}, async (request, reply) => {
  const { callId, rawTranscript } = request.body;
  const geminiKey = request.headers['x-gemini-key'] || request.headers['x-api-key'];
  const provider = request.headers['x-ai-provider'];
  const modelName = request.headers['x-model-name'];

  try {
    // 1. Instantly stage basic execution frame to PostgreSQL via Prisma using upsert
    const newRecord = await prisma.callSummary.upsert({
      where: { callId: callId },
      update: {
        rawTranscript: rawTranscript,
        status: 'PROCESSING',
        outboundTriggered: false,
        aiAnalysisPass: ''
      },
      create: {
        callId: callId,
        rawTranscript: rawTranscript,
        status: 'PROCESSING',
        outboundTriggered: false,
        aiAnalysisPass: ''
      }
    });

    // 2. Handoff transaction to our background background queue worker node via Redis with retry configuration
    await transcriptQueue.add('process-gong-raw', {
      callId: callId,
      transcript: rawTranscript,
      geminiKey: geminiKey,
      provider: provider,
      modelName: modelName
    }, {
      attempts: 3,
      backoff: {
        type: 'exponential',
        delay: 2000
      }
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

    const geminiKey = request.headers['x-gemini-key'] || request.headers['x-api-key'];
    const provider = request.headers['x-ai-provider'];
    const modelName = request.headers['x-model-name'];

    // Reset status to PROCESSING, reset outboundTriggered and aiAnalysisPass
    const updatedRecord = await prisma.callSummary.update({
      where: { id: id },
      data: {
        status: 'PROCESSING',
        outboundTriggered: false,
        aiAnalysisPass: ''
      }
    });

    // Re-enqueue transaction to BullMQ background worker queue via Redis with retry configuration
    await transcriptQueue.add('process-gong-raw', {
      callId: job.callId,
      transcript: job.rawTranscript,
      geminiKey: geminiKey,
      provider: provider,
      modelName: modelName
    }, {
      attempts: 3,
      backoff: {
        type: 'exponential',
        delay: 2000
      }
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
    fastify.log.info('🔌 Connected dedicated pg client for LISTEN/NOTIFY');
    
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
    
    fastify.log.info('✅ PostgreSQL trigger and trigger function configured.');

    await listenClient.query('LISTEN jobs_channel');
    fastify.log.info('👂 Listening to jobs_channel...');

    listenClient.on('notification', async (msg) => {
      if (msg.channel === 'jobs_channel' && msg.payload) {
        fastify.log.info({ payload: msg.payload }, '🔔 Received PG notify');
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
          fastify.log.error({ err }, 'Error fetching full job details for broadcast');
        }
      }
    });
    
    listenClient.on('error', async (err) => {
      fastify.log.error({ err }, '❌ Postgres listener client error');
      try {
        await listenClient.end();
      } catch {}
      setTimeout(setupPostgresListener, 5000);
    });
  } catch (err) {
    fastify.log.error({ err }, '❌ Failed to setup Postgres listener');
    setTimeout(setupPostgresListener, 5000);
  }
}

// 6. GET Health Check Route (Checks connections and reports BullMQ job status metrics)
fastify.get('/api/v1/health', async (request, reply) => {
  let dbStatus = 'UP';
  let redisStatus = 'UP';
  let status = 'UP';

  try {
    await prisma.$queryRaw`SELECT 1`;
  } catch (err) {
    dbStatus = 'DOWN';
    status = 'DOWN';
    fastify.log.error({ err }, 'Healthcheck DB connection failure');
  }

  try {
    const pong = await redisConnection.ping();
    if (pong !== 'PONG') {
      redisStatus = 'DOWN';
      status = 'DOWN';
    }
  } catch (err) {
    redisStatus = 'DOWN';
    status = 'DOWN';
    fastify.log.error({ err }, 'Healthcheck Redis connection failure');
  }

  let queueMetrics = {};
  if (redisStatus === 'UP') {
    try {
      queueMetrics = await transcriptQueue.getJobCounts();
    } catch (err) {
      fastify.log.error({ err }, 'Failed to fetch queue metrics');
    }
  }

  return reply.status(200).send({
    status,
    uptime: process.uptime(),
    timestamp: new Date().toISOString(),
    services: {
      database: dbStatus,
      redis: redisStatus
    },
    queueMetrics
  });
});

const startServer = async () => {
  try {
    await fastify.listen({ port: 3000, host: '0.0.0.0' });
    fastify.log.info('Server is running seamlessly on http://localhost:3000');
    setupPostgresListener();
  } catch (err) {
    fastify.log.error(err);
    process.exit(1);
  }
};

// Graceful Shutdown Logic
async function gracefulShutdown(signal) {
  fastify.log.info({ signal }, 'Starting graceful shutdown of server...');
  
  // 1. Close all active SSE connections
  fastify.log.info({ clientsCount: clients.size }, 'Closing active SSE connections...');
  for (const client of clients) {
    try {
      client.write('event: shutdown\ndata: Server is shutting down\n\n');
      client.end();
    } catch (e) {}
  }
  clients.clear();

  // 2. Close Fastify server
  try {
    await fastify.close();
    fastify.log.info('⚡ Fastify server closed.');
  } catch (err) {
    fastify.log.error({ err }, 'Error closing Fastify server');
  }

  // 3. Close Dedicated Postgres listener
  try {
    await listenClient.end();
    fastify.log.info('🔌 Dedicated PG listener connection ended.');
  } catch (err) {
    fastify.log.error({ err }, 'Error closing PG listener');
  }

  // 4. Disconnect Prisma
  try {
    await prisma.$disconnect();
    fastify.log.info('💾 Prisma database client disconnected.');
  } catch (err) {
    fastify.log.error({ err }, 'Error disconnecting Prisma');
  }

  // 5. Quit Redis connection
  try {
    await redisConnection.quit();
    fastify.log.info('📡 Redis connection ended.');
  } catch (err) {
    fastify.log.error({ err }, 'Error closing Redis connection');
  }

  fastify.log.info('👋 Graceful shutdown complete. Exiting.');
  process.exit(0);
}

process.on('SIGINT', () => gracefulShutdown('SIGINT'));
process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));

startServer();