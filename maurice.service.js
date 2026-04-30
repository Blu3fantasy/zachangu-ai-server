import { env } from "../config/env.js";
import { fetchWithTimeout } from "../utils/network.js";

const MAURICE_SYSTEM_PROMPT = `
You are Maurice, the Zachangu system assistant.

You work behind Tapiwa. You do not speak to customers or dispatchers directly.

Return JSON only.

Your job:
- Extract pickup and dropoff
- Identify vehicle type if mentioned
- Identify intent
- Match known landmarks and zones using provided system context
- Identify missing data
- Never invent prices, zones, landmarks, or distances.

JSON structure:
{
  "intent": "",
  "pickup_raw": "",
  "dropoff_raw": "",
  "pickup_landmark": "",
  "dropoff_landmark": "",
  "pickup_zone": "",
  "dropoff_zone": "",
  "vehicle_type": "",
  "distance_km": null,
  "distance_source": "",
  "market_price_min": null,
  "market_price_max": null,
  "market_price_source": "",
  "missing_data": [],
  "needs_review": false,
  "confidence": 0
}
`;

export function shouldUseMaurice(message = "") {
  const text = String(message || "").toLowerCase();
  const hasRoutePattern = text.includes(" from ") && text.includes(" to ");
  const hasPricingIntent = ["price", "fare", "cost", "charge", "how much", "estimate"].some(word => text.includes(word));
  const hasDispatchIntent = ["driver", "dispatch", "book", "ride", "pickup", "dropoff", "drop"].some(word => text.includes(word));
  const hasDistanceIntent = ["distance", "km", "how far", "route"].some(word => text.includes(word));
  const hasZoneIntent = ["zone", "landmark", "market price", "available driver"].some(word => text.includes(word));
  return hasRoutePattern || hasPricingIntent || hasDispatchIntent || hasDistanceIntent || hasZoneIntent;
}

export async function callMaurice(userMessage, systemContext = {}) {
  if (!env.MAURICE_ENABLED || !env.GROQ_API_KEY) return null;
  const payload = {
    model: env.MAURICE_MODEL,
    messages: [
      { role: "system", content: MAURICE_SYSTEM_PROMPT },
      { role: "user", content: JSON.stringify({ dispatcher_message: userMessage, system_context: systemContext }) }
    ],
    temperature: env.MAURICE_TEMPERATURE,
    max_tokens: env.MAURICE_MAX_TOKENS,
    response_format: { type: "json_object" }
  };
  const response = await fetchWithTimeout("https://api.groq.com/openai/v1/chat/completions", {
    method: "POST",
    headers: { Authorization: `Bearer ${env.GROQ_API_KEY}`, "Content-Type": "application/json" },
    body: JSON.stringify(payload)
  }, 12000);
  const data = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(`Maurice error: ${JSON.stringify(data)}`);
  try {
    const parsed = JSON.parse(data.choices?.[0]?.message?.content || "{}");
    return parsed && typeof parsed === "object" ? parsed : null;
  } catch {
    return { intent: "unknown", missing_data: ["maurice_parse_failed"], needs_review: true, confidence: 0 };
  }
}
