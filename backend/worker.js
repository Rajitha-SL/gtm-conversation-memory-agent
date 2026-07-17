import { dispatchToOutboundPipeline } from './services/clayDispatcher.js';
import { Worker } from 'bullmq';
import IORedis from 'ioredis';
import { GoogleGenAI } from '@google/genai';
import { PrismaClient } from '@prisma/client';
import { PrismaPg } from '@prisma/adapter-pg';

console.log('🔄 Background Worker Engine Initializing with AI Core...');

// Initialize the Google Gen AI client library instance
const ai = new GoogleGenAI({ apiKey: process.env.GEMINI_API_KEY });

// Standard, native Prisma v7 driver adapter initialization 
const adapter = new PrismaPg({ connectionString: process.env.DATABASE_URL });
const prisma = new PrismaClient({ adapter });

// Configure a dedicated connection manager to link cleanly with our Docker Redis service
const redisConnection = new IORedis({
  host: '127.0.0.1',
  port: 6379,
  maxRetriesPerRequest: null // This flag is strictly required by BullMQ framework architectural standards
});

// Initialize the worker thread to process tasks sent to the queue
const transcriptWorker = new Worker(
  'transcript-processing',
  async (job) => {
    // Support both historical queue item naming formats to prevent pipeline blocks
    const callId = job.data.callId;
    const transcript = job.data.transcriptData || job.data.transcript;

    if (!transcript) {
      console.error(`⚠️ [Job #${job.id}] Warning: No transcript string text found in job payload keys!`);
      throw new Error('No transcript data available');
    }
    console.log(`\n📦 [Job #${job.id}] Sending raw transcript payload to Gemini...`);

    try {
      // 1. Run analysis generation using Gemini
      const response = await ai.models.generateContent({
        model: 'gemini-2.5-flash',
        contents: `Analyze the following meeting transcript and provide a structured summary including key decisions and high-priority action items with owners:\n\n${transcript}`
      });

      const analysisText = response.text;
      console.log(`✨ [Job #${job.id}] Gemini successfully processed the payload!`);
      console.log('--- AI ANALYSIS REPORT ---');
      console.log(analysisText);
      console.log('--------------------------');

      // 2. Write analysis data to database storage using Prisma (Assigned to 'record' variable)
      console.log(`💾 [Job #${job.id}] Updating analysis data in database storage...`);
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

      console.log(`✅ [Job #${job.id}] Successfully saved to database table!`); 

      // 3. Pass payload seamlessly to the outbound automated link
      const triggered = await dispatchToOutboundPipeline(callId, analysisText);

      if (triggered) {
        await prisma.callSummary.update({
          where: { id: record.id },
          data: { outboundTriggered: true }
        });
        console.log(`🔄 [Job #${job.id}] Updated call summary status: outboundTriggered = true.`);
      }
      
    } catch (error) {
      console.error(`❌ [Job #${job.id}] AI/Database Layer Faulted:`, error.message);
      try {
        await prisma.callSummary.update({
          where: { callId: callId },
          data: { status: 'FAULTED' }
        });
      } catch (dbErr) {
        console.error(`⚠️ Could not update job status to FAULTED:`, dbErr.message);
      }
      throw error; // Re-throw so BullMQ handles retry logging accurately
    }
  },
  { connection: redisConnection }
);

console.log('🚀 Background Worker Engine actively listening for queue assignments.');

// Global process exception handlers to monitor connection stability safely
transcriptWorker.on('failed', (job, err) => {
  console.error(`❌ [Job #${job.id}] Failed completely:`, err.message);
});