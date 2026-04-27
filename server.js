import express from "express";
import cors from "cors";
import dotenv from "dotenv";

dotenv.config();

const app = express();

app.use(cors());
app.use(express.json());

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
  return /@tapiwa/i.test(message || "");
}

function cleanTapiwaMessage(message) {
  return String(message || "").replace(/@tapiwa/gi, "").trim();
}

async function supabaseFetch(table, limit = 50) {
  if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) return [];

  const url = `${SUPABASE_URL}/rest/v1/${table}?select=*&limit=${limit}`;

  try {
    const response = await fetch(url, {
      headers: {
        apikey: SUPABASE_SERVICE_ROLE_KEY,
        Authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
        "Content-Type": "application/json"
      }
    });

    if (!response.ok) {
      console.warn(`Supabase fetch failed for ${table}:`, await response.text());
      return [];
    }

    return await response.json();
  } catch (error) {
    console.warn(`Supabase error for ${table}:`, error.message);
    return [];
  }
}

async function fetchZachanguContext(userMessage) {
  const searchText = String(userMessage || "").toLowerCase();

  const [
    zones,
    allLandmarks,
    pricingRules,
    marketPrices,
    routeMatrix,
    risks
  ] = await Promise.all([
    supabaseFetch("zones", 20),
    supabaseFetch("landmarks", 100),
    supabaseFetch("pricing_rules", 50),
    supabaseFetch("market_prices", 80),
    supabaseFetch("route_matrix", 100),
    supabaseFetch("risks", 50)
  ]);

  const matchedLandmarks = allLandmarks.filter((lm) => {
    const values = [
      lm.name,
      lm.landmark_name,
      lm.area,
      lm.location_area,
      lm.zone_code,
      lm.zone_id
    ]
      .filter(Boolean)
      .join(" ")
      .toLowerCase();

    return values && searchText.split(/\s+/).some((word) => word.length > 2 && values.includes(word));
  });

  return {
    zones,
    landmarks: matchedLandmarks.length ? matchedLandmarks.slice(0, 30) : allLandmarks.slice(0, 30),
    pricing_rules: pricingRules,
    market_prices: marketPrices,
    route_matrix: routeMatrix,
    risks
  };
}

app.post("/ai/analyze", async (req, res) => {
  try {
    const {
      message,
      senderName = "Unknown",
      senderRole = "Dispatcher"
    } = req.body || {};

    if (!message || !String(message).trim()) {
      return res.status(400).json({ error: "Message is required" });
    }

    if (!GROQ_API_KEY) {
      return res.status(500).json({ error: "Missing GROQ_API_KEY in Railway variables" });
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
        temperature: 0.25
      })
    });

    const groqData = await groqResponse.json();

    if (!groqResponse.ok) {
      return res.status(groqResponse.status).json({
        error: "Groq API error",
        details: groqData
      });
    }

    const aiResult = JSON.parse(groqData.choices[0].message.content);

    return res.json({
      ignored: false,
      category: aiResult.category || "general_update",
      risk_level: aiResult.risk_level || "low",
      internal_summary: aiResult.internal_summary || cleanMessage,
      team_message: aiResult.team_message || "Noted team, let’s handle this calmly and keep operations moving.",
      requires_supervisor_approval: aiResult.requires_supervisor_approval === true,
      used_data: aiResult.used_data || {}
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
