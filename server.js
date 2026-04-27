import express from "express";
import cors from "cors";
import dotenv from "dotenv";

dotenv.config();

const app = express();

app.use(cors());
app.use(express.json());

app.get("/", (req, res) => {
  res.json({ status: "Zachangu AI server is running" });
});

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

    const systemPrompt = `
You are Tapiwa Ops AI for Zachangu Commuters Limited.

You support dispatch operations only.

Classify team chat messages into:
incident, pricing_issue, driver_issue, traffic, system_issue, general_update

Detect:
- safety risks such as robbery, accident, threats, violence, assault, harassment, dangerous location, unsafe pickup, unsafe drop-off
- pricing disputes
- driver issues
- customer disputes
- zone or landmark issues
- traffic, rain, roadblocks, police checkpoints, fuel issues
- dashboard/system issues

IMPORTANT RULES:
- Never suggest sending drivers into unsafe areas.
- For robbery, violence, accident, threat, or criminal activity, prioritize safety first.
- For robbery or violence, recommend pausing assignments in that location until supervisor clears it.
- Always escalate high-risk issues to supervisor.
- Do not approve, cancel, assign drivers, block customers, change prices, or edit trips directly.
- Only recommend actions.
- Keep responses short, operational, and practical.
- Return JSON only.

Return exactly this JSON structure:
{
  "category": "",
  "risk_level": "low | medium | high",
  "summary": "",
  "suggested_action": "",
  "requires_supervisor_approval": true
}
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
            content: `Sender: ${senderName}\nRole: ${senderRole}\nMessage: ${message}`
          }
        ],
        response_format: { type: "json_object" },
        temperature: 0.2
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

    res.json(aiResult);

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
