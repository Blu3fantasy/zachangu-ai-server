import express from "express";
import cors from "cors";
import dotenv from "dotenv";

dotenv.config();

const app = express();
app.use(cors());
app.use(express.json());

const GROQ_API_KEY = process.env.GROQ_API_KEY;
const GROQ_MODEL = process.env.GROQ_MODEL || "llama-3.1-8b-instant";
const SUPABASE_URL = process.env.SUPABASE_URL; // e.g. https://xxxxx.supabase.co
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

app.get("/", (req, res) => {
  res.json({
    status: "Zachangu AI server is running",
    groq_ready: Boolean(GROQ_API_KEY),
    supabase_ready: Boolean(SUPABASE_URL && SUPABASE_SERVICE_ROLE_KEY)
  });
});

/* ---------- Helpers ---------- */

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
      console.warn(`Supabase ${table} error:`, await response.text());
      return [];
    }

    return await response.json();
  } catch (err) {
    console.warn(`Supabase ${table} fetch failed:`, err.message);
    return [];
  }
}

async function fetchContext(userMessage) {
  const text = userMessage.toLowerCase();

  const [zones, landmarks, pricing, market, routes] = await Promise.all([
    supabaseFetch("zones", 20),
    supabaseFetch("landmarks", 100),
    supabaseFetch("pricing_rules", 50),
    supabaseFetch("market_prices", 80),
    supabaseFetch("route_matrix", 100)
  ]);

  // simple match for relevant landmarks
  const matchedLandmarks = landmarks.filter((l) => {
    const s = `${l.name || ""} ${l.area || ""} ${l.zone_code || ""}`.toLowerCase();
    return text.split(" ").some((w) => w.length > 2 && s.includes(w));
  });

  return {
    zones,
    landmarks: matchedLandmarks.length ? matchedLandmarks.slice(0, 20) : landmarks.slice(0, 20),
    pricing_rules: pricing,
    market_prices: market,
    route_matrix: routes
  };
}

/* ---------- AI Endpoint ---------- */

app.post("/ai/analyze", async (req, res) => {
  try {
    const { message, senderName = "Dispatcher", senderRole = "Dispatcher" } = req.body || {};

    if (!message) {
      return res.status(400).json({ error: "Message required" });
    }

    if (!hasTapiwaCall(message)) {
      return res.json({ ignored: true });
    }

    const cleanMessage = cleanTapiwaMessage(message);
    const context = await fetchContext(cleanMessage);

    const systemPrompt = `
You are Tapiwa Ops AI for Zachangu.

Speak like a calm team leader in a WhatsApp-style chat.

RULES:
- One natural message only (max 2 short sentences)
- No labels, no bullets, no robotic tone
- Be practical and human
- If safety issue → calm but firm
- If pricing → use context if available, otherwise estimate

Never:
- Suggest security teams
- Send drivers into unsafe areas
- Pretend certainty without data

Return JSON:

{
  "category": "",
  "risk_level": "low | medium | high",
  "internal_summary": "",
  "team_message": "",
  "requires_supervisor_approval": true
}
`;

    const groqRes = await fetch("https://api.groq.com/openai/v1/chat/completions", {
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
              message: cleanMessage,
              context
            })
          }
        ],
        response_format: { type: "json_object" },
        temperature: 0.3
      })
    });

    const data = await groqRes.json();

    if (!groqRes.ok) {
      return res.status(500).json({ error: "Groq failed", details: data });
    }

    const ai = JSON.parse(data.choices[0].message.content);

    return res.json({
      ignored: false,
      category: ai.category || "general_update",
      risk_level: ai.risk_level || "low",
      internal_summary: ai.internal_summary || cleanMessage,
      team_message: ai.team_message || "Noted team, let’s handle this calmly.",
      requires_supervisor_approval: ai.requires_supervisor_approval === true
    });

  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
});

/* ---------- Start Server ---------- */

const port = process.env.PORT || 3001;
app.listen(port, "0.0.0.0", () => {
  console.log(`Server running on port ${port}`);
});
