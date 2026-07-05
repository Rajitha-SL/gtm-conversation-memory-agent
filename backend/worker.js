import * as dotenv from 'dotenv';
import path from 'path';
import { fileURLToPath } from 'url';

// Force an explicit, absolute path resolve to guarantee env parameters load instantly
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
dotenv.config({ path: path.resolve(__dirname, '.env') });

import { Worker } from 'bullmq';
import IORedis from 'ioredis';
import { GoogleGenAI } from '@google/genai';
import { PrismaClient } from '@prisma/client';
import { PrismaPg } from '@prisma/adapter-pg';
import pg from 'pg';

console.log('🔄 Background Worker Engine Initializing with AI Core...');

// Initialize the Google Gen AI client library instance
const ai = new GoogleGenAI({ apiKey: process.env.GEMINI_API_KEY });

// Feed explicit string parameters to the pool driver to bypass connection string parsing bottlenecks
const pool = new pg.Pool({
  user: 'gtm_admin',
  password: 'admin_secure_pass123',
  host: '127.0.0.1',
  port: 5432,
  database: 'gtm_memory_engine'
});

const adapter = new PrismaPg({ pool });
const prisma = new PrismaClient({ adapter });

const redisConnection = new IORedis({
  host: 'localhost',
  port: 6379,
  maxRetriesPerRequest: null
});

const transcriptWorker = new Worker(
  'transcript-processing',
  async (job) => {
    console.log(`\n📦 [Job #${job.id}] Sending raw transcript payload to Gemini...`);
    
    const { callId, transcriptData } = job.data;

    // Define a clear analysis system instruction prompt
    const promptText = `
      You are an expert executive meeting assistant. Analyze the following business meeting call transcript.
      Provide a clean summary, a list of critical decisions made, and high-priority action items with owners.

      Raw Transcript:
      "${transcriptData}"
    `;

    try {
      // Execute the generation request using the high-performance gemini-2.5-flash model
      const response = await ai.models.generateContent({
        model: 'gemini-2.5-flash',
        contents: promptText,
      });

      const aiAnalysisResult = response.text;

      console.log(`✨ [Job #${job.id}] Gemini successfully processed the payload!`);
      console.log(`--- AI ANALYSIS REPORT ---`);
      console.log(aiAnalysisResult);
      console.log(`--------------------------`);

      // 💾 PERSISTENCE LAYER: Save the output directly into PostgreSQL via Prisma v7
      console.log(`💾 [Job #${job.id}] Writing analysis data to database storage...`);
      await prisma.callSummary.create({
        data: {
          callId: callId,
          rawTranscript: transcriptData,
          aiAnalysisPass: aiAnalysisResult
        }
      });
      console.log(`✅ [Job #${job.id}] Database records synced successfully.`);

      // Returning this saves it to the job metadata tracking history inside Redis memory
      return { 
        processed: true, 
        analysis: aiAnalysisResult,
        timestamp: new Date().toISOString() 
      };

    } catch (aiError) {
      console.error(`❌ [Job #${job.id}] AI/Database Layer Faulted:`, aiError.message);
      throw aiError; // Propagates to BullMQ's automatic retry framework
    }
  },
  {
    connection: redisConnection
  }
);

transcriptWorker.on('completed', (job) => {
  console.log(`✅ [Job #${job.id}] Finished pipeline processing.`);
});

transcriptWorker.on('failed', (job, err) => {
  console.error(`❌ [Job #${job?.id}] Failed completely:`, err.message);
});

console.log('🚀 Background Worker Engine actively listening for queue assignments.');