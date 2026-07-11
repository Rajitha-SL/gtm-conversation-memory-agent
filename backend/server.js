import Fastify from 'fastify';
import fastifyCors from '@fastify/cors';
import { PrismaClient } from '@prisma/client';
import pg from 'pg';
import { PrismaPg } from '@prisma/adapter-pg';

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
        outboundTriggered: true,
        createdAt: true
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
    // 1. Instantly stage basic execution frame to PostgreSQL via Prisma
    const newRecord = await prisma.callSummary.create({
      data: {
        callId: callId,
        rawTranscript: transcript,
        status: 'PROCESSING',
        outboundTriggered: false
      }
    });

    // 2. Handoff transaction to our background background queue worker node via Redis
    // (Ensure your local fastify instance has your background queue producer hooked up here)
    // e.g., await transcriptQueue.add('analyze', { jobId: newRecord.id });

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

/**
 * -----------------------------------------------------------------------------
 * ENGINE STARTUP PROCEDURES
 * -----------------------------------------------------------------------------
 */
const startServer = async () => {
  try {
    await fastify.listen({ port: 3000, host: '0.0.0.0' });
    console.log('Server is running seamlessly on http://localhost:3000');
  } catch (err) {
    fastify.log.error(err);
    process.exit(1);
  }
};

startServer();