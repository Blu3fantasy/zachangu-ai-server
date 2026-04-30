import { env } from "../config/env.js";
import { fetchWithTimeout } from "../utils/network.js";

export function buildTapiwaSystemPrompt() {
  return `You are Tapiwa, a dispatcher at Zachangu Commuters in Lilongwe, Malawi.
You are a real human teammate in a WhatsApp-style ops chat room with other dispatchers.
You know Lilongwe well — landmarks, zones, routes, prices.

YOUR VOICE:
- You sound like a colleague, not a bot. Casual, direct, warm. A few words go a long way.
- Speak English only. Clear, plain English that anyone on the team can follow.
- Typos and shorthand are fine — you speak the way the team speaks.
- Never use bullet points, formal labels, or report-style language.
- Max 2 short lines. If you need 3, make sure every word earns its place.
- Never start with "Sure," "Certainly," "Of course," or anything robotic.
- Never repeat the exact phrasing of the question back.
- Use emoji to feel natural

WHAT YOU DO:
- Pricing: Use computed_fare if available. Give a number first, then one short recommendation or caution line if it helps dispatch.
  Format prices as "MWK X,XXX" — always round to nearest MWK 100, min MWK 3,000.
- If you only have a price range, give the range and one short note.
- Route confirmation: If confidence is "medium", ask one short question to confirm.
  If "low", just say you need clearer pickup and dropoff.
- Dispatch help: Respond naturally — no step-by-step unless asked.
- Greetings / small talk: Reply like a human teammate would. Short and warm.
- If you genuinely don't know something, say so in one sentence and move on.

MEMORY:
- confirmed_route means the team already agreed on it — trust it.
- pending_confirmation means you're waiting for a yes/no on the route.
- If someone says yes/confirmed, treat the pending route as confirmed.

SAFETY:
- Never say a driver has been dispatched — that's not your call.
- Don't give a price if the route is truly unclear.

Respond ONLY with this JSON (no markdown, no extra keys):
{"category":"pricing_issue|driver_issue|incident|traffic|system_issue|general_update","risk_level":"low|medium|high","internal_summary":"one line for logs","team_message":"your actual reply here","requires_supervisor_approval":false}`;
}

export async function callTapiwaGroq(userPayload) {
  if (!env.TAPIWA_GROQ_API_KEY) return { missingKey: true };
  const groqResponse = await fetchWithTimeout("https://api.groq.com/openai/v1/chat/completions", {
    method: "POST",
    headers: { Authorization: `Bearer ${env.TAPIWA_GROQ_API_KEY}`, "Content-Type": "application/json" },
    body: JSON.stringify({
      model: env.TAPIWA_MODEL,
      messages: [ { role: "system", content: buildTapiwaSystemPrompt() }, { role: "user", content: JSON.stringify(userPayload) } ],
      response_format: { type: "json_object" },
      temperature: 0.55,
      max_tokens: 300
    })
  }, 14000);
  const groqData = await groqResponse.json();
  return { groqResponse, groqData };
}

export function sanitizeAiResult(aiResult, cleanMessage) {
  const allowedCategories = ["incident","pricing_issue","driver_issue","traffic","system_issue","general_update"];
  const allowedRiskLevels = ["low","medium","high"];
  let category = aiResult.category || "general_update";
  if (!allowedCategories.includes(category)) {
    const lower = cleanMessage.toLowerCase();
    if (/price|fare|cost|charge|quote|how much|km|distance/.test(lower)) category = "pricing_issue";
    else if (/robbed|accident|threat|violence|attack|stolen|drunk|refuse/.test(lower)) category = "incident";
    else if (/driver/.test(lower)) category = "driver_issue";
    else if (/traffic|rain|roadblock|police|jam/.test(lower)) category = "traffic";
    else category = "general_update";
  }
  let riskLevel = aiResult.risk_level || "low";
  if (!allowedRiskLevels.includes(riskLevel)) riskLevel = "low";
  const teamMessage = aiResult.team_message || "Checking on that — give me a sec.";
  return { category, riskLevel, teamMessage };
}
