import { dispatchToOutboundPipeline } from './services/clayDispatcher.js';
import { Worker } from 'bullmq';
import IORedis from 'ioredis';
import { GoogleGenAI } from '@google/genai';
import { PrismaClient } from '@prisma/client';
import { PrismaPg } from '@prisma/adapter-pg';
import pino from 'pino';

const logger = pino();

logger.info('🔄 Background Worker Engine Initializing with AI Core...');

// We initialize the Google Gen AI client library dynamically per job execution to support user-configured BYO keys

// Standard, native Prisma v7 driver adapter initialization 
const adapter = new PrismaPg({ connectionString: process.env.DATABASE_URL });
const prisma = new PrismaClient({ adapter });

// Configure a dedicated connection manager to link cleanly with our Docker Redis service
const redisConnection = new IORedis({
  host: process.env.REDIS_HOST || '127.0.0.1',
  port: parseInt(process.env.REDIS_PORT || '6379', 10),
  maxRetriesPerRequest: null // This flag is strictly required by BullMQ framework architectural standards
});

// Initialize the worker thread to process tasks sent to the queue
const transcriptWorker = new Worker(
  'transcript-processing',
  async (job) => {
    // Support both historical queue item naming formats to prevent pipeline blocks
    const callId = job.data.callId;
    const transcript = job.data.transcriptData || job.data.transcript;
    const apiKey = job.data.geminiKey || process.env.GEMINI_API_KEY;
    const ai = new GoogleGenAI({ apiKey });

    if (!transcript) {
      logger.warn({ jobId: job.id, callId }, 'No transcript string text found in job payload keys!');
      throw new Error('No transcript data available');
    }
    logger.info({ jobId: job.id, callId }, 'Sending raw transcript payload to Gemini...');

    try {
      // 1. Run analysis generation using Gemini
      const response = await ai.models.generateContent({
        model: 'gemini-2.5-flash',
        contents: `Analyze the following meeting transcript and provide a structured summary including key decisions and high-priority action items with owners.
        
        At the end of your response, add a section marked exactly with "### Follow-up Email Draft" (on a new line). In this section, write a highly personalized, ready-to-send follow-up email draft based on the meeting.
        
        Ensure this email draft:
        - Addresses the specific primary contact (e.g. Mark, Sarah) instead of generic placeholders.
        - Mentions the specific agreed price, discount, or terms (e.g. $36,000/yr for 2 years) discussed in the transcript.
        - References the exact next steps and who owes what (e.g. Alex sending the ISO 27001 certs to Rachel).
        
        Transcript:
        ${transcript}`
      });

      const analysisText = response.text;
      logger.info({ jobId: job.id, callId }, 'Gemini successfully processed the payload!');

      // 2. Write analysis data to database storage using Prisma (Assigned to 'record' variable)
      logger.info({ jobId: job.id, callId }, 'Updating analysis data in database storage...');
      const record = await prisma.callSummary.upsert({
        where: { callId: callId },
        update: {
          aiAnalysisPass: analysisText,
          status: 'COMPLETED'
        },
        create: {
          callId: callId,
          rawTranscript: transcript,
          aiAnalysisPass: analysisText,
          status: 'COMPLETED'
        }
      });

      logger.info({ jobId: job.id, callId }, 'Successfully saved to database table!'); 

      // 3. Pass payload seamlessly to the outbound automated link
      const triggered = await dispatchToOutboundPipeline(callId, analysisText, apiKey);

      if (triggered) {
        await prisma.callSummary.update({
          where: { id: record.id },
          data: { outboundTriggered: true }
        });
        logger.info({ jobId: job.id, callId }, 'Updated call summary status: outboundTriggered = true.');
      }
      
    } catch (error) {
      logger.error({ jobId: job.id, callId, error: error.message, stack: error.stack }, 'AI/Database Layer Faulted');
      try {
        await prisma.callSummary.update({
          where: { callId: callId },
          data: { status: 'FAULTED' }
        });
      } catch (dbErr) {
        logger.error({ dbErr: dbErr.message }, 'Could not update job status to FAULTED');
      }
      throw error; // Re-throw so BullMQ handles retry logging accurately
    }
  },
  { connection: redisConnection }
);

logger.info('🚀 Background Worker Engine actively listening for queue assignments.');

// Global process exception handlers to monitor connection stability safely
transcriptWorker.on('failed', (job, err) => {
  const attempts = job?.opts?.attempts || 1;
  const attemptsMade = job?.attemptsMade || 0;
  
  if (attemptsMade >= attempts) {
    logger.error({
      event: 'job_failed_permanently',
      jobId: job?.id,
      callId: job?.data?.callId,
      error: err.message,
      stack: err.stack,
      attemptsMade,
      attempts
    }, `❌ [Job #${job?.id}] Failed permanently after ${attemptsMade} attempts.`);
  } else {
    logger.warn({
      event: 'job_attempt_failed',
      jobId: job?.id,
      callId: job?.data?.callId,
      error: err.message,
      attemptsMade,
      attempts
    }, `⚠️ [Job #${job?.id}] Attempt ${attemptsMade} failed. Retrying...`);
  }
});

// Graceful Shutdown Logic
async function gracefulShutdown(signal) {
  logger.info({ signal }, `Starting graceful shutdown of worker...`);

  // 1. Close BullMQ worker
  try {
    await transcriptWorker.close();
    logger.info('BullMQ worker closed.');
  } catch (err) {
    logger.error({ err: err.message }, 'Error closing worker');
  }

  // 2. Disconnect Prisma
  try {
    await prisma.$disconnect();
    logger.info('Prisma database client disconnected.');
  } catch (err) {
    logger.error({ err: err.message }, 'Error disconnecting Prisma');
  }

  // 3. Quit Redis connection
  try {
    await redisConnection.quit();
    logger.info('Redis connection ended.');
  } catch (err) {
    logger.error({ err: err.message }, 'Error closing Redis connection');
  }

  logger.info('Graceful shutdown complete. Exiting.');
  process.exit(0);
}

process.on('SIGINT', () => gracefulShutdown('SIGINT'));
process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));