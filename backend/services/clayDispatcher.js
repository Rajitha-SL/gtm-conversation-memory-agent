import { GoogleGenAI } from '@google/genai';

const ai = new GoogleGenAI({ apiKey: process.env.GEMINI_API_KEY });

/**
 * Parses the structured summary, extracts target intent parameters, 
 * and dispatches them to the external Clay enrichment waterfall.
 */
export async function dispatchToOutboundPipeline(callId, analysisText) {
  console.log(`\n🔗 [Outbound Link] Extracting market target profiles for Call: ${callId}...`);

  try {
    // 1. Use Gemini to extract highly structured outbound parameters from the summary
    const response = await ai.models.generateContent({
      model: 'gemini-2.5-flash',
      contents: `You are an enterprise growth routing coordinator. Analyze this meeting summary and extract structured outbound campaign coordinates. 
      Identify the core software engineering/infra block mentioned (e.g., AWS, GCP, Salesforce) and the professional corporate persona/title blocking the deal (e.g., Security Director, DevOps Lead).
      
      Respond strictly with a valid JSON object matching this schema, no markdown wrapping, no prose:
      {
        "targetTechnology": "string or null",
        "targetPersonaTitle": "string or null",
        "primaryFrictionPoint": "brief 1-sentence description of the objection"
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
    
    // Skip dispatching safely if no high-conviction coordinates are discovered
    if (!parsedData.targetTechnology || !parsedData.targetPersonaTitle) {
      console.log(`ℹ️ [Outbound Link] Skip: No actionable tech stack or persona friction points identified.`);
      return false;
    }

    console.log(`🚀 [Outbound Link] Coordinates isolated: Targeting companies using [${parsedData.targetTechnology}] matching [${parsedData.targetPersonaTitle}] persona.`);

    // 2. Dispatch data bundle straight to the Clay Webhook Ingestion URL
    const clayWebhookUrl = process.env.CLAY_WEBHOOK_URL;
    
    if (!clayWebhookUrl) {
      console.warn(`⚠️ [Outbound Link] Configuration Missing: CLAY_WEBHOOK_URL not set in env. Simulating dispatch logs.`);
      return true; // Return true to test state changes locally without a live endpoint
    }

    const payload = {
      sourceCallId: callId,
      timestamp: new Date().toISOString(),
      enrichmentCriteria: {
        techLookupKeyword: parsedData.targetTechnology,
        personaTitleKeyword: parsedData.targetPersonaTitle,
        contextCue: parsedData.primaryFrictionPoint
      }
    };

    const webhookResponse = await fetch(clayWebhookUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    });

    if (!webhookResponse.ok) {
      throw new Error(`Clay gateway responded with status: ${webhookResponse.status}`);
    }

    console.log(`✅ [Outbound Link] Payload successfully injected into Clay's data waterfall engine.`);
    return true;

  } catch (error) {
    console.error(`❌ [Outbound Link] Execution failed for Call ${callId}:`, error.message);
    return false;
  }
}