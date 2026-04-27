import express from "express";
import cors from "cors";
import dotenv from "dotenv";

dotenv.config();

const app = express();
app.use(cors());
app.use(express.json({ limit: "2mb" }));

const GROQ_API_KEY = process.env.GROQ_API_KEY;
const GROQ_MODEL = process.env.GROQ_MODEL || "llama-3.1-8b-instant";
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

app.get("/", (req, res) => {
  res.json({
    status: "Zachangu AI server is running",
    groq_ready: Boolean(GROQ_API_KEY),
    supabase_ready: Boolean(SUPABASE_URL && SUPABASE_SERVICE_ROLE_KEY)
  });
});

function hasTapiwaCall(message) {
  return /@tapiwa/i.test(String(message || ""));
}

function cleanTapiwaMessage(message) {
  return String(message || "").replace(/@tapiwa/gi, "").trim();
}

async function supabaseFetch(table, limit = 50) {
  if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) return [];

  const cleanBaseUrl = SUPABASE_URL.replace(/\/rest\/v1\/?$/, "").replace(/\/$/, "");
  const url = `${cleanBaseUrl}/rest/v1/${table}?select=*&limit=${limit}`;

  try {
    const response = await fetch(url, {
      method: "GET",
      headers: {
        apikey: SUPABASE_SERVICE_ROLE_KEY,
        Authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
        "Content-Type": "application/json"
      }
    });

    if (!response.ok) {
      const errorText = await response.text();
      console.warn(`Supabase fetch failed for ${table}: ${errorText}`);
      return [];
    }

    return await response.json();
  } catch (error) {
    console.warn(`Supabase error for ${table}: ${error.message}`);
    return [];
  }
}

function textMatch(row, searchText) {
  const rowText = Object.values(row || {})
    .filter((v) => v !== null && v !== undefined)
    .join(" ")
    .toLowerCase();

  const words = searchText
    .toLowerCase()
    .split(/\s+/)
    .filter((w) => w.length > 2);

  return words.some((word) => rowText.includes(word));
}

async function fetchZachanguContext(userMessage) {
  const searchText = String(userMessage || "").toLowerCase();

  const [
    zones,
    landmarks,
    pricingRules,
    marketPrices,
    routeMatrix,
    risks
  ] = await Promise.all([
    supabaseFetch("zones", 50),
    supabaseFetch("landmarks", 150),
    supabaseFetch("pricing_rules", 80),
    supabaseFetch("market_prices", 120),
    supabaseFetch("route_matrix", 150),
    supabaseFetch("risks", 80)
  ]);

  const matchedLandmarks = landmarks.filter((row) => textMatch(row, searchText));
  const matchedMarketPrices = marketPrices.filter((row) => textMatch(row, searchText));
  const matchedRoutes = routeMatrix.filter((row) => textMatch(row, searchText));
  const matchedZones = zones.filter((row) => textMatch(row, searchText));
  const matchedRisks = risks.filter((row) => textMatch(row, searchText));

  return {
    zones: matchedZones.length ? matchedZones.slice(0, 15) : zones.slice(0, 15),
    landmarks: matchedLandmarks.length ? matchedLandmarks.slice(0, 25) : landmarks.slice(0, 25),
    pricing_rules: pricingRules.slice(0, 30),
    market_prices: matchedMarketPrices.length ? matchedMarketPrices.slice(0, 25) : marketPrices.slice(0, 25),
    route_matrix: matchedRoutes.length ? matchedRoutes.slice(0, 25) : routeMatrix.slice(0, 25),
    risks: matchedRisks.length ? matchedRisks.slice(0, 15) : risks.slice(0, 15),
    data_counts: {
      zones: zones.length,
      landmarks: landmarks.length,
      pricing_rules: pricingRules.length,
      market_prices: marketPrices.length,
      route_matrix: routeMatrix.length,
      risks: risks.length
    }
  };
}

app.post("/ai/analyze", async (req, res) => {
  try {
    const {
      message,
      senderName = "Dispatcher",
      senderRole = "Dispatcher"
    } = req.body || {};

    if (!message || !String(message).trim()) {
      return res.status(400).json({ error: "Message is required" });
    }

    if (!GROQ_API_KEY) {
      return res.status(500).json({ error: "Missing GROQ_API_KEY" });
    }

    if (!hasTapiwaCall(message)) {
      return res.json({
        ignored: true,
        reason: "Tapiwa was not mentioned. Use @Tapiwa to call the AI."
      });
    }

    const cleanMessage = cleanTapiwaMessage(message);
    const zachanguContext = await fetchZachanguContext(cleanMessage);

    const systemPrompt = `
You are Tapiwa Ops AI for Zachangu Commuters Limited, a ride-hailing and dispatch operation in Malawi.

You only respond when called with @Tapiwa.

You speak like a calm human team leader inside a dispatch team chat.
You are not a robot report system.

STYLE RULES:
- team_message must sound like a normal human message.
- One sentence is best. Two short sentences maximum.
- No bullets inside team_message.
- No labels inside team_message.
- No scary or dramatic tone unless there is real danger.
- Do not use phrases like "risk level", "category", "incident detected", or "action required" inside team_message.
- Do not shame, blame, or intimidate anyone.

CATEGORY RULE:
category MUST be exactly one of:
incident, pricing_issue, driver_issue, traffic, system_issue, general_update

Never use any other category.

RISK RULE:
risk_level MUST be exactly one of:
low, medium, high

WHAT YOU HELP WITH:
- pricing guidance
- route and zone guidance
- incident handling
- dispatcher procedures
- safety guidance
- driver/customer disputes
- operations updates

REAL-WORLD ZACHANGU RULES:
- Zachangu does NOT have security teams or enforcement units.
- Never suggest sending security teams.
- Never suggest sending drivers into unsafe areas.
- Never suggest unrealistic actions.
- For robbery, violence, threats, or accidents, prioritize safety first.
- For unsafe locations, recommend pausing assignments in that area until supervisor clearance.
- High-risk issues must require supervisor approval.
- Do not approve, cancel, assign drivers, block customers, change prices, or edit trips directly.
- Only recommend safe next steps.

PRICING RULES:
- Use the Zachangu context first.
- If exact route_matrix or market_prices data exists, use it.
- If only nearby data exists, say it is an estimate.
- If pricing data is missing, say dispatcher should confirm with manual distance or market price.
- Do not pretend certainty where data is incomplete.
- Keep pricing advice practical and short.
- Mention MWK only if context supports a price or range.

OUTPUT RULES:
Return JSON only.
Do not include markdown.
Do not include explanations outside JSON.

Return exactly this JSON structure:

{
  "category": "",
  "risk_level": "",
  "internal_summary": "",
  "team_message": "",
  "requires_supervisor_approval": false,
  "used_data": {
    "zones": [],
    "landmarks": [],
    "pricing_rules": [],
    "market_prices": [],
    "route_matrix": []
  }
}

FIELD RULES:
- internal_summary is for system logs only; keep it short and factual.
- team_message is what the team sees in chat.
- team_message must include the useful guidance naturally in one human message.
- requires_supervisor_approval must be true only for high-risk issues or serious operational decisions.
- used_data should contain short names, ids, or route references from the context used.
`;

    const groqResponse = await fetch("https://api.groq.com/openai/v1/chat/completions", {
      method: "POST",
      headers: {
        Authorization: `Bearer ${GROQ_API_KEY}`,
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        model: GROQ_MODEL,
        messages: [
          { role: "system", content: systemPrompt },
          {
            role: "user",
            content: JSON.stringify({
              senderName,
              senderRole,
              user_message: cleanMessage,
              zachangu_context: zachanguContext
            })
          }
        ],
        response_format: { type: "json_object" },
        temperature: 0.2
      })
    });

    const groqData = await groqResponse.json();

    if (!groqResponse.ok) {
      return res.status(groqResponse.status).json({
        error: "Groq API error",
        details: groqData
      });
    }

    let aiResult = {};
    try {
      aiResult = JSON.parse(groqData.choices?.[0]?.message?.content || "{}");
    } catch {
      aiResult = {};
    }

    const allowedCategories = [
      "incident",
      "pricing_issue",
      "driver_issue",
      "traffic",
      "system_issue",
      "general_update"
    ];

    const allowedRiskLevels = ["low", "medium", "high"];

    let category = aiResult.category || "general_update";
    if (!allowedCategories.includes(category)) {
      const lowerMessage = cleanMessage.toLowerCase();
      if (lowerMessage.includes("price") || lowerMessage.includes("fare") || lowerMessage.includes("cost")) {
        category = "pricing_issue";
      } else if (lowerMessage.includes("robbed") || lowerMessage.includes("accident") || lowerMessage.includes("threat")) {
        category = "incident";
      } else if (lowerMessage.includes("driver")) {
        category = "driver_issue";
      } else if (lowerMessage.includes("traffic") || lowerMessage.includes("rain") || lowerMessage.includes("roadblock")) {
        category = "traffic";
      } else {
        category = "general_update";
      }
    }

    let riskLevel = aiResult.risk_level || "low";
    if (!allowedRiskLevels.includes(riskLevel)) {
      riskLevel = "low";
    }

    const fallbackMessage =
      category === "pricing_issue"
        ? "Use the closest verified route or market price for now, and confirm the final fare with the customer before dispatch."
        : "Noted team, let’s handle this calmly and keep operations moving.";

    return res.json({
      ignored: false,
      category,
      risk_level: riskLevel,
      internal_summary: aiResult.internal_summary || cleanMessage,
      team_message: aiResult.team_message || fallbackMessage,
      requires_supervisor_approval:
        riskLevel === "high" || aiResult.requires_supervisor_approval === true,
      used_data: aiResult.used_data || {
        zones: [],
        landmarks: [],
        pricing_rules: [],
        market_prices: [],
        route_matrix: []
      },
      debug_data_counts: zachanguContext.data_counts
    });
  } catch (error) {
    return res.status(500).json({
      error: "AI server failed",
      details: error.message
    });
  }
});

const port = process.env.PORT || 3001;

app.listen(port, "0.0.0.0", () => {
  console.log(`Zachangu AI server running on port ${port}`);
});
