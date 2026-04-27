import express from "express";
import cors from "cors";
import dotenv from "dotenv";
import fetch from "node-fetch"; // <-- IMPORTANT for Node <18

dotenv.config();

const app = express();
app.use(cors());
app.use(express.json({ limit: "5mb" }));

const {
  GROQ_API_KEY,
  GROQ_MODEL = "llama-3.1-70b-versatile",
  SUPABASE_URL,
  SUPABASE_SERVICE_ROLE_KEY
} = process.env;

if (!GROQ_API_KEY) {
  throw new Error("Missing GROQ_API_KEY in environment");
}

const conversationHistory = new Map();

app.get("/", (req, res) => {
  res.json({
    status: "General AI Server Active",
    model: GROQ_MODEL,
    uptime: process.uptime()
  });
});

app.post("/ai/chat", async (req, res) => {
  try {
    const { message, sessionId, userName } = req.body;

    if (!message) {
      return res.status(400).json({ error: "No message provided" });
    }

    const sessionKey = sessionId || "default_user";

    if (!conversationHistory.has(sessionKey)) {
      conversationHistory.set(sessionKey, []);
    }

    const history = conversationHistory.get(sessionKey);

    const systemPrompt = `You are a helpful, witty AI.
User name: ${userName || "friend"}.
Location: Lilongwe, Malawi.`;

    const messages = [
      { role: "system", content: systemPrompt },
      ...history,
      { role: "user", content: message }
    ];

    const response = await fetch(
      "https://api.groq.com/openai/v1/chat/completions",
      {
        method: "POST",
        headers: {
          Authorization: `Bearer ${GROQ_API_KEY}`,
          "Content-Type": "application/json"
        },
        body: JSON.stringify({
          model: GROQ_MODEL,
          messages,
          temperature: 0.7,
          max_tokens: 1024
        })
      }
    );

    const data = await response.json();

    if (!response.ok) {
      console.error("Groq API Error:", data);
      return res.status(500).json({ error: "AI request failed" });
    }

    const aiMessage =
      data?.choices?.[0]?.message?.content || "No response from AI";

    // Update memory
    history.push({ role: "user", content: message });
    history.push({ role: "assistant", content: aiMessage });

    while (history.length > 20) {
      history.splice(0, 2);
    }

    // Optional logging
    if (SUPABASE_URL && SUPABASE_SERVICE_ROLE_KEY) {
      logToSupabase(sessionKey, message, aiMessage);
    }

    res.json({
      reply: aiMessage,
      sessionId: sessionKey
    });

  } catch (error) {
    console.error("Chat Error:", error);
    res.status(500).json({ error: "Internal Server Error" });
  }
});

async function logToSupabase(session, userMsg, aiMsg) {
  try {
    await fetch(`${SUPABASE_URL}/rest/v1/general_chat_logs`, {
      method: "POST",
      headers: {
        apikey: SUPABASE_SERVICE_ROLE_KEY,
        Authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
        "Content-Type": "application/json",
        Prefer: "return=minimal"
      },
      body: JSON.stringify({
        session_id: session,
        user_input: userMsg,
        ai_output: aiMsg,
        created_at: new Date().toISOString()
      })
    });
  } catch {
    console.warn("Supabase logging failed");
  }
}

const PORT = process.env.PORT || 3001;
app.listen(PORT, () =>
  console.log(`Server running on http://localhost:${PORT}`)
);
