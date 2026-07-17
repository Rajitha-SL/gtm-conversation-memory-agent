import pino from 'pino';

const logger = pino();

export async function dispatchToSlack(webhookUrl, callId, analysisPass) {
  let riskScore = 'Low';
  let budgetText = 'None';
  let competitorText = 'None';
  let securityText = 'None';
  let isJson = false;

  try {
    const parsed = JSON.parse(analysisPass);
    riskScore = parsed.dealRiskScore || 'Low';
    budgetText = parsed.objectionRiskBreakdown?.budget || 'None';
    competitorText = parsed.objectionRiskBreakdown?.competitor || 'None';
    securityText = parsed.objectionRiskBreakdown?.security || 'None';
    isJson = true;
  } catch {
    // If fallback plaintext, parse basic metrics
    if (analysisPass.toLowerCase().includes('high')) {
      riskScore = 'High';
    } else if (analysisPass.toLowerCase().includes('medium')) {
      riskScore = 'Medium';
    }
  }

  const riskEmoji = riskScore === 'High' ? '🔴 HIGH' : riskScore === 'Medium' ? '🟡 MEDIUM' : '🟢 LOW';

  const slackPayload = {
    blocks: [
      {
        "type": "header",
        "text": {
          "type": "plain_text",
          "text": "📢 GTM Pipeline Alert: Deal Risk Status Resolved",
          "emoji": true
        }
      },
      {
        "type": "section",
        "fields": [
          {
            "type": "mrkdwn",
            "text": `*Transaction Key:*\n\`${callId}\``
          },
          {
            "type": "mrkdwn",
            "text": `*Deal Risk Score:*\n*${riskEmoji}*`
          }
        ]
      },
      {
        "type": "section",
        "text": {
          "type": "mrkdwn",
          "text": `*Objections Breakdown:*\n• *Budget:* ${budgetText}\n• *Competitor:* ${competitorText}\n• *Security:* ${securityText}`
        }
      },
      {
        "type": "divider"
      }
    ]
  };

  if (webhookUrl && webhookUrl.trim().startsWith('http')) {
    logger.info({ callId }, 'Sending slack webhook alert payload...');
    try {
      const response = await fetch(webhookUrl, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(slackPayload)
      });
      if (response.ok) {
        logger.info({ callId }, 'Slack webhook alert successfully resolved!');
        return true;
      } else {
        const text = await response.text();
        logger.error({ callId, status: response.status, response: text }, 'Slack webhook returned non-2xx status');
      }
    } catch (err) {
      logger.error({ callId, error: err.message }, 'Failed to dispatch Slack webhook alert POST request');
    }
  } else {
    logger.warn({ callId }, 'No Slack Webhook URL configured. Simulated Slack notification details displayed below:');
    console.log('\n=================== SIMULATED SLACK NOTIFICATION ===================');
    console.log(`HEADER: 📢 GTM Pipeline Alert: Deal Risk Status Resolved`);
    console.log(`KEY: ${callId}`);
    console.log(`SCORE: ${riskEmoji}`);
    console.log(`BUDGET: ${budgetText}`);
    console.log(`COMPETITOR: ${competitorText}`);
    console.log(`SECURITY: ${securityText}`);
    console.log('=====================================================================\n');
  }
  return false;
}
