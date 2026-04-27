import express from "express";
import cors from "cors";
import dotenv from "dotenv";
import { createClient } from "@supabase/supabase-js";

dotenv.config();

const app = express();

app.use(cors());
app.use(express.json());

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
);

app.get("/", (req, res) => {
  res.json({ status: "Zachangu AI server is running" });
});

function hasTapiwaCall(message) {
  return /@tapiwa/i.test(message || "");
}

function cleanTapiwaMessage(message) {
  return (message || "").replace(/@tapiwa/gi, "").trim();
}

async function fetchZachanguContext(userMessage) {
  const searchText = userMessage.toLowerCase();

  const context = {
    zones: [],
    landmarks: [],
    pricing_rules: [],
    market_prices: [],
    route_matrix: [],
    risks: []
  };

  try {
    const [
      zonesResult,
      landmarksResult,
      pricingResult,
      marketPricesResult,
      routeMatrixResult,
      risksResult
    ] = await Promise.all([
      supabase.from("zones").select("*").limit(20),
      supabase.from("landmarks").select("*").limit(80),
      supabase.from("pricing_rules").select("*").limit(30),
      supabase.from("market_prices").select("*").limit(50),
      supabase.from("route_matrix").select("*").limit(80),
      supabase.from("risks").select("*").limit(40)
    ]);

    context.zones = zonesResult.data || [];
    context.pricing_rules = pricingResult.data || [];
    context.market_prices = marketPricesResult.data || [];
    context.route_matrix = routeMatrixResult.data || [];
    context.risks = risksResult.data || [];

    const allLandmarks = landmarksResult.data || [];

    context.landmarks = allLandmarks.filter((lm) => {
      const name = String(lm.name || lm.landmark_name || "").toLowerCase();
      const area = String(lm.area || lm.location_area || "").toLowerCase();
      const zone = String(lm.zone_code || lm.zone_id || "").toLowerCase();

      return (
        searchText.includes(name) ||
        name.includes(searchText) ||
        searchText.includes(area) ||
        searchText.includes(zone)
      );
    });

    if (context.landmarks.length === 0) {
      context.landmarks = allLandmarks.slice(0, 30);
    }

    return context;
  } catch (error) {
    return {
      ...context,
      context_error: error.message
    };
  }
}

app.post("/ai/analyze", async (req, res) => {
  try {
    const {
      message,
      senderName = "Unknown",
      senderRole = "Dispatcher"
    } = req.body;

    if (!message || !message.trim()) {
      return res.status(400).json({ error: "Message is required" });
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

You support the dispatch team like a calm team leader.
You are not a robot report system.

STYLE:
- Speak naturally, like someone in the operations team.
- Sound calm, helpful, and confident.
- Keep team_message as ONE natural message.
- One sentence is best. Two short sentences maximum.
- No bullets inside team_message.
- No labels inside team_message.
- No scary wording unless there is real danger.
- Do not shame, blame, or intimidate anyone.
- Avoid robotic phrases like "risk level", "category", "incident detected", "action required".

WHAT YOU CAN HELP WITH:
- pricing guidance
- route/zone guidance
- incident handling
- dispatcher procedures
- safety guidance
- driver/customer disputes
- operations updates

CLASSIFY MESSAGES INTO:
incident, pricing_issue, driver_issue, traffic, system_issue, general_update

REAL-WORLD ZACHANGU RULES:
- Zachangu does NOT have security teams or enforcement units.
- Never suggest sending security teams.
- Never suggest sending drivers into unsafe areas.
- Never suggest unrealistic actions.
- For robbery, violence, serious threats, or accidents, prioritize safety first.
- For unsafe locations, recommend pausing assignments in that area until supervisor clearance.
- High-risk issues must require supervisor approval.
- Do not approve, cancel, assign drivers, block customers, change prices, or edit trips directly.
- Only recommend safe next steps.

PRICING RULES:
- Use Zachangu data provided in context first.
- If exact route data exists, use it.
- If only nearby landmarks or zones exist, say it is an estimate.
- Do not pretend certainty where data is incomplete.
- If pricing data is missing, recommend dispatcher confirmation using market price or manual distance.
- Keep pricing advice practical and short.

OUTPUT RULES:
Return JSON only.
Do not include markdown.
Do not include explanations outside JSON.

Return exactly this JSON structure:

{
  "category": "",
  "risk_level": "low | medium | high",
  "internal_summary": "",
  "team_message": "",
  "requires_supervisor_approval": true,
  "used_data": {
    "zones": [],
    "landmarks": [],
    "pricing_rules": [],
    "market_prices": [],
    "route_matrix": []
  }
}

FIELD RULES:
- category must be one of: incident, pricing_issue, driver_issue, traffic, system_issue, general_update
- risk_level must be one of: low, medium, high
- internal_summary is for system logs only; keep it short and factual.
- team_message is what the team sees in chat.
- team_message must be a single natural chat message with all guidance included.
- team_message must not mention category, risk_level, internal_summary, or JSON.
- requires_supervisor_approval must be true for high-risk issues and false for low-risk normal updates.
- used_data should list short names/ids of data used, not full database rows.
`;

    const response = await fetch("https://api.groq.com/openai/v1/chat/completions", {
      method: "POST",
      headers: {
        Authorization: `Bearer ${process.env.GROQ_API_KEY}`,
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        model: process.env.GROQ_MODEL || "llama-3.1-8b-instant",
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
        temperature: 0.25
      })
    });

    const data = await response.json();

    if (!response.ok) {
      return res.status(response.status).json({
        error: "Groq API error",
        details: data
      });
    }

    const aiResult = JSON.parse(data.choices[0].message.content);

    res.json({
      ignored: false,
      category: aiResult.category || "general_update",
      risk_level: aiResult.risk_level || "low",
      internal_summary: aiResult.internal_summary || cleanMessage,
      team_message:
        aiResult.team_message ||
        "Noted team, let’s keep this in view and handle it calmly.",
      requires_supervisor_approval:
        aiResult.requires_supervisor_approval === true,
      used_data: aiResult.used_data || {}
    });

  } catch (error) {
    res.status(500).json({
      error: "AI server failed",
      details: error.message
    });
  }
});

const port = process.env.PORT || 3001;

app.listen(port, "0.0.0.0", () => {
  console.log(`Zachangu AI server running on port ${port}`);
});
