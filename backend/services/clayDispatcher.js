import { GoogleGenAI } from '@google/genai';
import pino from 'pino';

const logger = pino();
const ai = new GoogleGenAI({ apiKey: process.env.GEMINI_API_KEY });

/**
 * Parses the structured summary, extracts target intent parameters, 
 * and dispatches them to the external Clay enrichment waterfall.
 */
export async function dispatchToOutboundPipeline(callId, analysisText) {
  logger.info({ callId }, '🔗 [Outbound Link] Extracting market target profiles...');

  try {
    // 1. Use Gemini to extract highly structured outbound Activation parameters from the summary
    const response = await ai.models.generateContent({
      model: 'gemini-2.5-flash',
      contents: `You are an enterprise growth routing coordinator. Analyze this meeting analysis summary and extract structured outbound campaign Activation coordinates. 
      Identify the target company name, the primary contact's email (if not found, infer a realistic format like info@company.com), GTM insights (key pain points or context), a buying intent score (integer 0 to 100 based on tone and objections), and a suggested outreach sequence strategy name.
      
      Respond strictly with a valid JSON object matching this schema, no markdown wrapping, no prose:
      {
        "contactEmail": "string",
        "companyName": "string",
        "gtmInsights": "string",
        "buyingIntentScore": number,
        "suggestedOutreachSequence": "string"
      }

      Meeting Summary Data:
      ${analysisText}`,
    });

    let cleanText = response.text.trim();
    
    // Eliminate markdown backtick block wrappers if the model returns them
    if (cleanText.startsWith('```')) {
      cleanText = cleanText.replace(/^```json/, '').replace(/^```/, '').replace(/```$/, '').trim();
    }

    const parsedData = JSON.parse(cleanText);
    
    // Skip dispatching safely if no company name is discovered
    if (!parsedData.companyName) {
      logger.info({ callId }, 'ℹ️ [Outbound Link] Skip: No actionable companyName identified.');
      return false;
    }

    logger.info({ 
      callId, 
      companyName: parsedData.companyName, 
      contactEmail: parsedData.contactEmail,
      buyingIntentScore: parsedData.buyingIntentScore 
    }, `🚀 [Outbound Link] Coordinates isolated: Targeting company.`);

    // 2. Dispatch data bundle straight to the Clay Webhook Ingestion URL
    const clayWebhookUrl = process.env.CLAY_WEBHOOK_URL;
    
    const payload = {
      contactEmail: parsedData.contactEmail || 'info@company.com',
      companyName: parsedData.companyName,
      gtmInsights: parsedData.gtmInsights || '',
      buyingIntentScore: parsedData.buyingIntentScore || 50,
      suggestedOutreachSequence: parsedData.suggestedOutreachSequence || 'generic-sequence'
    };

    if (!clayWebhookUrl) {
      logger.warn({ callId, simulatedPayload: payload }, '⚠️ [Outbound Link] Configuration Missing: CLAY_WEBHOOK_URL not set in env. Simulating dispatch logs.');
      return true; // Return true to test state changes locally without a live endpoint
    }

    const webhookResponse = await fetch(clayWebhookUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    });

    if (!webhookResponse.ok) {
      throw new Error(`Clay gateway responded with status: ${webhookResponse.status}`);
    }

    logger.info({ callId }, '✅ [Outbound Link] Payload successfully injected into Clay\'s data waterfall engine.');
    return true;

  } catch (error) {
    logger.error({ callId, error: error.message, stack: error.stack }, '❌ [Outbound Link] Execution failed');
    return false;
  }
}