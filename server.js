import express from "express";
import cors from "cors";
import dotenv from "dotenv";

dotenv.config();

const app = express();
app.use(cors());
app.use(express.json({ limit: "5mb" }));

// Environment Variables
const GROQ_API_KEY = process.env.GROQ_API_KEY;
const GROQ_MODEL = process.env.GROQ_MODEL || "llama-3.1-70b-versatile";
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

// In-memory conversation history (Resets on server restart)
const conversationHistory = new Map();

app.get("/", (req, res) => {
  res.json({
    status: "General AI Server Active",
    model: GROQ_MODEL,
    uptime: process.uptime()
  });
});

/**
 * Main Chat Endpoint
 */
app.post("/ai/chat", async (req, res) => {
  try {
    const { message, sessionId, userName } = req.body;

    if (!message) return res.status(400).json({ error: "No message provided" });

    // 1. Manage Conversation Memory (Max 10 messages for context)
    const sessionKey = sessionId || "default_user";
    if (!conversationHistory.has(sessionKey)) {
      conversationHistory.set(sessionKey, []);
    }
    const history = conversationHistory.get(sessionKey);

    // 2. Prepare the Payload for Groq
    const systemPrompt = `You are a helpful, witty, and intelligent AI companion. 
    Your tone is conversational and peer-like. Keep responses concise but insightful.
    The user's name is ${userName || "friend"}. 
    Current location: Lilongwe, Malawi.`;

    const messages = [
      { role: "system", content: systemPrompt },
      ...history,
      { role: "user", content: message }
    ];

    // 3. Call Groq API
    const response = await fetch("https://api.groq.com/openai/v1/chat/completions", {
      method: "POST",
      headers: {
        "Authorization": `Bearer ${GROQ_API_KEY}`,
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        model: GROQ_MODEL,
        messages: messages,
        temperature: 0.7,
        max_tokens: 1024
      })
    });

    const data = await response.json();
    const aiMessage = data.choices[0].message.content;

    // 4. Update Memory
    history.push({ role: "user", content: message });
    history.push({ role: "assistant", content: aiMessage });
    if (history.length > 20) history.splice(0, 2); // Keep context lean

    // 5. Log to Supabase (Optional Audit)
    if (SUPABASE_URL && SUPABASE_SERVICE_ROLE_KEY) {
      logToSupabase(sessionKey, message, aiMessage);
    }

    return res.json({
      reply: aiMessage,
      sessionId: sessionKey
    });

  } catch (error) {
    console.error("General Chat Error:", error);
    res.status(500).json({ error: "Internal Server Error" });
  }
});

/**
 * Utility: Log chats for later analysis
 */
async function logToSupabase(session, userMsg, aiMsg) {
  const url = `${SUPABASE_URL}/rest/v1/general_chat_logs`;
  try {
    await fetch(url, {
      method: "POST",
      headers: {
        "apikey": SUPABASE_SERVICE_ROLE_KEY,
        "Authorization": `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
        "Content-Type": "application/json",
        "Prefer": "return=minimal"
      },
      body: JSON.stringify({
        session_id: session,
        user_input: userMsg,
        ai_output: aiMsg,
        created_at: new Date().toISOString()
      })
    });
  } catch (e) {
    console.warn("Logging failed, but continuing chat...");
  }
}

const PORT = process.env.PORT || 3001;
app.listen(PORT, () => console.log(`General AI listening on port ${PORT}`));
