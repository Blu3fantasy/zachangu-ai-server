import express from "express";
import cors from "cors";
import dotenv from "dotenv";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { rateLimit } from "express-rate-limit";

dotenv.config();

const NODE_ENV = String(process.env.NODE_ENV || "development");
const IS_PROD = NODE_ENV === "production";
const PERSONA_VERSION = "2026-05-05.1";
const ENABLE_STRICT_OUTPUT_VALIDATION = process.env.ENABLE_STRICT_OUTPUT_VALIDATION !== "false";
const ENABLE_SHARED_PERSONA_CONFIG = process.env.ENABLE_SHARED_PERSONA_CONFIG !== "false";
const ENABLE_SECURE_DEBUG_ROUTES = process.env.ENABLE_SECURE_DEBUG_ROUTES === "true";
const EXPOSE_DEBUG_PAYLOADS = process.env.EXPOSE_DEBUG_PAYLOADS === "true" && !IS_PROD;
const REQUIRE_API_AUTH = process.env.REQUIRE_API_AUTH === "true";
const API_SERVER_KEY = String(process.env.AI_SERVER_API_KEY || "");
const CORS_ALLOWED_ORIGINS = String(process.env.CORS_ALLOWED_ORIGINS || "");

const GROQ_API_KEY = process.env.GROQ_API_KEY;
const GEMINI_API_KEY = process.env.GEMINI_API_KEY || process.env.GEMMA_API_KEY || "";
const TAPIWA_PROVIDER = "gemini";
const TAPIWA_MODEL = process.env.GEMINI_MODEL || process.env.GEMMA_MODEL || process.env.TAPIWA_MODEL || "gemini-2.5-flash";
const GROQ_MODEL = process.env.GROQ_MODEL || "llama-3.1-8b-instant";
const MAURICE_ENABLED = process.env.MAURICE_ENABLED !== "false";
const MAURICE_MODEL = process.env.MAURICE_MODEL || GROQ_MODEL;
const MAURICE_MAX_TOKENS = Number(process.env.MAURICE_MAX_TOKENS || 800);
const MAURICE_TEMPERATURE = Number(process.env.MAURICE_TEMPERATURE || 0.1);
const TAPIWA_MAX_OUTPUT_TOKENS = Number(process.env.TAPIWA_MAX_OUTPUT_TOKENS || 900);
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY || process.env.SUPABASE_ANON || "";
const SUPABASE_API_KEY = SUPABASE_SERVICE_ROLE_KEY || SUPABASE_ANON_KEY;
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const MEMORY_FILE = path.join(__dirname, "conversation-memory.json");
const MEMORY_TTL_MS = 1000 * 60 * 60 * 12;
const MEMORY_MAX_SESSIONS = 300;
const TABLE_CACHE_TTL_MS = Number(process.env.TABLE_CACHE_TTL_MS || 60_000);
const STATIC_TABLE_CACHE = new Set([
  "zones",
  "landmarks",
  "pricing_rules",
  "market_prices",
  "route_matrix",
  "risks",
  "tapiwa_system_settings",
  "tapiwa_market_price_rules",
  "tapiwa_route_intelligence",
  "tapiwa_zone_behavior",
  "tapiwa_route_learning"
]);

const TAPIWA_RESPONSE_SCHEMA = {
  type: "OBJECT",
  required: [
    "category",
    "risk_level",
    "internal_summary",
    "team_message",
    "requires_supervisor_approval",
    "opening_used"
  ],
  properties: {
    category: { type: "STRING" },
    risk_level: { type: "STRING" },
    internal_summary: { type: "STRING" },
    team_message: { type: "STRING" },
    requires_supervisor_approval: { type: "BOOLEAN" },
    opening_used: { type: "STRING" }
  }
};

function parseCsvList(value = "") {
  return String(value)
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
}

const allowedCorsOrigins = parseCsvList(CORS_ALLOWED_ORIGINS);
const allowAllCorsOrigins = allowedCorsOrigins.includes("*");
function isOriginAllowed(origin) {
  if (!allowedCorsOrigins.length) return true;
  if (!origin) return true;
  if (allowAllCorsOrigins) return true;
  return allowedCorsOrigins.includes(origin);
}

const app = express();
// Railway proxy fix for express-rate-limit
app.set("trust proxy", 1);
app.use(cors({
  origin(origin, callback) {
    if (isOriginAllowed(origin)) return callback(null, true);
    return callback(new Error("CORS blocked for origin."));
  }
}));
app.use(express.json({ limit: "1mb" }));
app.use((req, res, next) => {
  const requestId = `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
  req.requestId = requestId;
  const startedAt = Date.now();
  res.setHeader("x-request-id", requestId);
  res.on("finish", () => {
    if (req.path === "/") return;
    console.log(JSON.stringify({
      request_id: requestId,
      method: req.method,
      path: req.path,
      status: res.statusCode,
      duration_ms: Date.now() - startedAt
    }));
  });
  next();
});

const analyzeRateLimiter = rateLimit({
  windowMs: Number(process.env.RATE_LIMIT_WINDOW_MS || 15 * 60 * 1000),
  limit: Number(process.env.RATE_LIMIT_MAX || 120),
  standardHeaders: "draft-7",
  legacyHeaders: false,
  message: { error: "Too many requests. Please try again shortly." }
});
app.use("/ai/analyze", analyzeRateLimiter);

function requireApiAuth(req, res, next) {
  if (!REQUIRE_API_AUTH) return next();
  if (!API_SERVER_KEY) {
    return res.status(503).json({ error: "Server auth key is not configured." });
  }
  const incoming = String(req.headers["x-api-key"] || req.headers.authorization || "").replace(/^Bearer\s+/i, "");
  if (incoming && incoming === API_SERVER_KEY) return next();
  return res.status(401).json({ error: "Unauthorized request." });
}

if (IS_PROD && REQUIRE_API_AUTH && !API_SERVER_KEY) {
  console.warn("REQUIRE_API_AUTH is active but AI_SERVER_API_KEY is missing.");
}

const tableDebug = {};
const conversationMemory = new Map();
const tableCache = new Map();
let memorySaveTimer = null;
let memorySaveInFlight = false;
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

function buildHealthPayload() {
  const tapiwaReady = Boolean(GEMINI_API_KEY);
  const mauriceReady = Boolean(GROQ_API_KEY);
  return {
    status: "Zachangu AI server is running",
    persona_version: PERSONA_VERSION,
    tapiwa_provider: TAPIWA_PROVIDER,
    tapiwa_model: TAPIWA_MODEL,
    groq_ready: mauriceReady,
    tapiwa_groq_ready: tapiwaReady,
    tapiwa_ready: tapiwaReady,
    maurice_ready: mauriceReady,
    maurice_groq_ready: mauriceReady,
    gemma_ready: tapiwaReady,
    gemini_ready: tapiwaReady,
    supabase_ready: Boolean(SUPABASE_URL && SUPABASE_API_KEY),
    supabase_url_loaded: cleanBaseUrl() || null,
    tapiwa_intelligence_ready: true
  };
}

app.get("/", (req, res) => {
  res.json(buildHealthPayload());
});

if (ENABLE_SECURE_DEBUG_ROUTES) {
  app.get("/debug-ai-keys", requireApiAuth, (req, res) => {
    res.json({
      gemini_key_loaded: Boolean(GEMINI_API_KEY),
      maurice_key_loaded: Boolean(GROQ_API_KEY),
      maurice_model: MAURICE_MODEL,
      tapiwa_model: TAPIWA_MODEL,
      maurice_enabled: MAURICE_ENABLED,
      require_api_auth: REQUIRE_API_AUTH,
      strict_output_validation: ENABLE_STRICT_OUTPUT_VALIDATION
    });
  });

  app.get("/env-check", requireApiAuth, (req, res) => {
    res.json({
      gemini_api_key_loaded: Boolean(GEMINI_API_KEY),
      gemini_model: TAPIWA_MODEL,
      maurice_groq_ready: Boolean(GROQ_API_KEY),
      supabase_ready: Boolean(SUPABASE_URL && SUPABASE_API_KEY),
      persona_version: PERSONA_VERSION
    });
  });

  app.get("/debug-tables", requireApiAuth, async (req, res) => {
    const tables = [
      "zones","landmarks","pricing_rules","market_prices","route_matrix","risks",
      "drivers","trip_requests","tapiwa_system_settings","tapiwa_market_price_rules",
      "tapiwa_route_intelligence","tapiwa_zone_behavior","tapiwa_route_learning",
      "tapiwa_price_outcomes","tapiwa_ai_audit_logs"
    ];
    const result = {};
    for (const table of tables) {
      const data = await supabaseFetch(table, 3, { bypassCache: true });
      result[table] = { rows_loaded: data.length, last_status: tableDebug[table] || null, sample: data.slice(0, 1) };
    }
    res.json(result);
  });
}

// â”€â”€ UTILS â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

function hasTapiwaCall(message) { return /@tapiwa/i.test(String(message || "")); }
function cleanTapiwaMessage(message) { return String(message || "").replace(/@tapiwa/gi, "").trim(); }
function cleanBaseUrl() { return String(SUPABASE_URL || "").replace(/\/rest\/v1\/?$/, "").replace(/\/$/, ""); }
function roundToNearest100(value) { return Math.round(Number(value || 0) / 100) * 100; }
function cleanPrice(value) { return Math.max(3000, roundToNearest100(value)); }

function buildSessionId({ sessionId, senderName, senderRole }) {
  return String(sessionId || `${senderRole || "Dispatcher"}:${senderName || "unknown"}`).trim();
}

function pruneConversationMemory() {
  const now = Date.now();
  for (const [sessionId, memory] of conversationMemory.entries()) {
    if (!memory?.updatedAt || now - memory.updatedAt > MEMORY_TTL_MS) {
      conversationMemory.delete(sessionId);
    }
  }

  if (conversationMemory.size <= MEMORY_MAX_SESSIONS) return;

  const oldestFirst = Array.from(conversationMemory.entries())
    .sort((a, b) => Number(a[1]?.updatedAt || 0) - Number(b[1]?.updatedAt || 0));

  while (oldestFirst.length && conversationMemory.size > MEMORY_MAX_SESSIONS) {
    const [sessionId] = oldestFirst.shift();
    conversationMemory.delete(sessionId);
  }
}

async function saveConversationMemoryToDisk() {
  if (memorySaveInFlight) return;
  memorySaveInFlight = true;
  try {
    pruneConversationMemory();
    const payload = Object.fromEntries(conversationMemory.entries());
    await fs.promises.writeFile(MEMORY_FILE, JSON.stringify(payload, null, 2), "utf8");
  } catch (error) {
    console.warn("Conversation memory save failed:", error.message);
  } finally {
    memorySaveInFlight = false;
  }
}

function scheduleConversationMemorySave() {
  if (memorySaveTimer) clearTimeout(memorySaveTimer);
  memorySaveTimer = setTimeout(() => {
    memorySaveTimer = null;
    saveConversationMemoryToDisk().catch(() => {});
  }, 250);
}

function loadConversationMemoryFromDisk() {
  try {
    if (!fs.existsSync(MEMORY_FILE)) return;
    const raw = fs.readFileSync(MEMORY_FILE, "utf8");
    const payload = JSON.parse(raw || "{}");
    for (const [sessionId, memory] of Object.entries(payload || {})) {
      if (!sessionId || !memory || typeof memory !== "object") continue;
      conversationMemory.set(sessionId, {
        lastRoute: memory.lastRoute || null,
        pendingConfirmation: memory.pendingConfirmation || null,
        pendingLocationDiscovery: memory.pendingLocationDiscovery && typeof memory.pendingLocationDiscovery === "object" ? memory.pendingLocationDiscovery : null,
        confirmedRoute: memory.confirmedRoute || null,
        lastAssistTopic: memory.lastAssistTopic || null,
        recentResponses: Array.isArray(memory.recentResponses) ? memory.recentResponses.slice(-8) : [],
        recentOpeners: Array.isArray(memory.recentOpeners) ? memory.recentOpeners.slice(-8) : [],
        recentJokes: Array.isArray(memory.recentJokes) ? memory.recentJokes.slice(-8) : [],
        userProfile: memory.userProfile && typeof memory.userProfile === "object" ? memory.userProfile : {},
        lastUserMessage: memory.lastUserMessage || null,
        lastResponse: memory.lastResponse || null,
        lastResponseAt: Number(memory.lastResponseAt || 0),
        updatedAt: Number(memory.updatedAt || Date.now())
      });
    }
    pruneConversationMemory();
  } catch (error) {
    console.warn("Conversation memory load failed:", error.message);
  }
}

function getConversationMemory(sessionId) {
  pruneConversationMemory();
  if (!conversationMemory.has(sessionId)) {
    conversationMemory.set(sessionId, {
      lastRoute: null,
      pendingConfirmation: null,
      pendingLocationDiscovery: null,
      confirmedRoute: null,
      lastAssistTopic: null,
      recentResponses: [],
      recentOpeners: [],
      recentJokes: [],
      userProfile: {},
      lastUserMessage: null,
      lastResponse: null,
      lastResponseAt: 0,
      updatedAt: Date.now()
    });
  }
  return conversationMemory.get(sessionId);
}

function saveConversationMemory(sessionId, memory) {
  conversationMemory.set(sessionId, {
    lastRoute: memory.lastRoute || null,
    pendingConfirmation: memory.pendingConfirmation || null,
    pendingLocationDiscovery: memory.pendingLocationDiscovery && typeof memory.pendingLocationDiscovery === "object" ? memory.pendingLocationDiscovery : null,
    confirmedRoute: memory.confirmedRoute || null,
    lastAssistTopic: memory.lastAssistTopic || null,
    recentResponses: Array.isArray(memory.recentResponses) ? memory.recentResponses.slice(-8) : [],
    recentOpeners: Array.isArray(memory.recentOpeners) ? memory.recentOpeners.slice(-8) : [],
    recentJokes: Array.isArray(memory.recentJokes) ? memory.recentJokes.slice(-8) : [],
    userProfile: memory.userProfile && typeof memory.userProfile === "object" ? memory.userProfile : {},
    lastUserMessage: memory.lastUserMessage || null,
    lastResponse: memory.lastResponse || null,
    lastResponseAt: Number(memory.lastResponseAt || 0),
    updatedAt: Date.now()
  });
  scheduleConversationMemorySave();
}

loadConversationMemoryFromDisk();
setInterval(() => {
  saveConversationMemoryToDisk().catch(() => {});
}, 1000 * 60 * 5).unref?.();


// â”€â”€ TAPIWA GEMINI PERSONALITY HELPERS â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

function detectTapiwaIntent(message = "") {
  const text = normalizeText(message);
  if (/price|fare|cost|charge|quote|how much|km|distance|route/.test(text) || /\bfrom\b.+\bto\b/.test(text)) return "price_request";
  if (/driver|dispatch|assign|available driver|book|ride|pickup|dropoff|drop/.test(text)) return "dispatch";
  if (/robbed|accident|threat|violence|attack|stolen|drunk|refuse|incident|police|injured|danger|fight/.test(text)) return "incident";
  if (/traffic|rain|roadblock|jam|weather/.test(text)) return "traffic";
  return "general_chat";
}

function isOperationalIntent(intent) {
  return ["price_request", "dispatch", "incident", "traffic"].includes(intent);
}

function isHighRiskIntent(intent, message = "") {
  const text = normalizeText(message);
  return intent === "incident" || /accident|robbed|attack|violence|threat|injured|police|stolen|danger|fight/.test(text);
}

function pickTapiwaVibe(intent, message = "") {
  if (isHighRiskIntent(intent, message)) return "Calm & Serious";

  const operationalVibes = [
    "Warm & Local",
    "Quick Dispatcher Mode",
    "Professional but Playful",
    "Light Malawian Humour",
    "Calm Teammate"
  ];

  const generalVibes = [
    "Witty & Energetic",
    "Warm & Local",
    "Playful Malawian Teammate",
    "Encouraging Big Sister Energy",
    "Relaxed Office Humour",
    "Sharp but Friendly"
  ];

  const pool = isOperationalIntent(intent) ? operationalVibes : generalVibes;
  return pool[Math.floor(Math.random() * pool.length)];
}

function getBlantyreDateTime() {
  const now = new Date();
  return {
    date: new Intl.DateTimeFormat("en-GB", {
      timeZone: "Africa/Blantyre",
      weekday: "long",
      year: "numeric",
      month: "long",
      day: "numeric"
    }).format(now),
    time: new Intl.DateTimeFormat("en-GB", {
      timeZone: "Africa/Blantyre",
      hour: "2-digit",
      minute: "2-digit",
      hour12: false
    }).format(now),
    timezone: "Africa/Blantyre"
  };
}

function pickHumanErrorMessage() {
  const phrases = [
    "Shaaaah ðŸ˜… system ikuvuta pangâ€™onoâ€”hold on, zitekha pompano.",
    "Hmmâ€¦ looks like the system is dragging a bit. Give me a sec.",
    "Eish, small hiccup here ðŸ˜„ let me catch it properly.",
    "Systemâ€™s being stubborn today, koma tili pompoâ€”try again in a moment.",
    "Somethingâ€™s not responding properly on my side, but donâ€™t panic, weâ€™ll sort it.",
    "Ahh this one has tripped the system a bit ðŸ˜… hold on.",
    "Small technical drama here, koma tikuyenda bwinoâ€”try again.",
    "Looks like Tapiwaâ€™s line to the system is shaking a bit. Give it a moment.",
    "Shaaaah guys, system ikupanga ma style ðŸ˜… hold on.",
    "Bit of a slow moment here, koma Iâ€™m still with you."
  ];
  return phrases[Math.floor(Math.random() * phrases.length)];
}


function isAffirmationMessage(message = "") {
  return /^(yes|yeah|yep|correct|confirm|confirmed|right|eya|inde|ee|ok|okay)\b/i.test(String(message || "").trim());
}

function isJokeRequest(message = "") {
  return /\b(joke|make me laugh|funny|cheer us|tell us something funny)\b/i.test(String(message || ""));
}

function updateUserToneProfile(memory, message = "") {
  const text = String(message || "");
  const profile = memory.userProfile && typeof memory.userProfile === "object" ? memory.userProfile : {};
  profile.message_count = Number(profile.message_count || 0) + 1;
  profile.prefers_humour = profile.prefers_humour || /haha|lol|ðŸ˜‚|ðŸ˜…|joke|funny|shaaa|shaah|bo/i.test(text);
  profile.brief_style = text.length < 35;
  profile.uses_chichewa = profile.uses_chichewa || /\b(bo|muli|bwanji|eya|inde|zikomo|koma|pang'ono|pompano|zili|tikuyenda)\b/i.test(text);
  profile.last_seen_at = new Date().toISOString();
  memory.userProfile = profile;
  return profile;
}

function rememberTapiwaResponse(memory, response = "", opening = "") {
  const clean = String(response || "").trim();
  if (!Array.isArray(memory.recentResponses)) memory.recentResponses = [];
  if (!Array.isArray(memory.recentOpeners)) memory.recentOpeners = [];
  if (clean) {
    memory.recentResponses.push(clean);
    memory.lastResponse = clean;
    memory.lastResponseAt = Date.now();
  }
  if (opening) memory.recentOpeners.push(String(opening).trim());
  memory.recentResponses = memory.recentResponses.slice(-8);
  memory.recentOpeners = memory.recentOpeners.slice(-8);
}

function responseLooksRepeated(memory, response = "") {
  const clean = normalizeText(response);
  if (!clean) return false;
  const recent = Array.isArray(memory.recentResponses) ? memory.recentResponses : [];
  return recent.some(prev => {
    const p = normalizeText(prev);
    if (!p) return false;
    if (p === clean) return true;
    if (clean.includes(p) || p.includes(clean)) return Math.min(clean.length, p.length) > 25;
    return similarity(clean, p) > 0.92;
  });
}

function pickLocalJoke(memory) {
  const jokes = [
    "A dispatcher told the map, â€˜be honest, are we routing or guessing today?â€™ The map just zoomed out and kept quiet ðŸ˜„",
    "One customer said â€˜Iâ€™m just nearbyâ€™â€¦ shaaah, in Malawi that can mean 200 metres or a whole zone away ðŸ˜…",
    "Fuel prices looked at our fare calculator and said, â€˜make space, Iâ€™m joining the meeting.â€™",
    "A driver once said â€˜Iâ€™m two minutes away.â€™ Even the clock laughed pangâ€™ono ðŸ˜„",
    "Dispatch life: customer says â€˜pa cornerâ€™ like Lilongwe has only one corner. Shaaaah ðŸ˜…",
    "The route was so confusing even ORS wanted a cup of tea before answering.",
    "A map without landmarks in Malawi is just vibes and prayers, koma tikuyenda ðŸ˜„",
    "Customer: â€˜Iâ€™m at the shop.â€™ Dispatcher: â€˜Which shop?â€™ Customer: â€˜The one everyone knows.â€™ Malawi classic ðŸ˜…"
  ];
  if (!Array.isArray(memory.recentJokes)) memory.recentJokes = [];
  const available = jokes.filter(j => !memory.recentJokes.includes(j));
  const chosen = (available.length ? available : jokes)[Math.floor(Math.random() * (available.length ? available.length : jokes.length))];
  memory.recentJokes.push(chosen);
  memory.recentJokes = memory.recentJokes.slice(-6);
  return chosen;
}

function buildDuplicateSafeFallback({ cleanMessage, detectedIntent, routeUnderstanding, computedFare, memory }) {
  if (isJokeRequest(cleanMessage)) return pickLocalJoke(memory);
  if (detectedIntent === "price_request") {
    const pickup = routeUnderstanding?.pickup?.Landmark_Name || routeUnderstanding?.pickupRaw || "the pickup";
    const dropoff = routeUnderstanding?.dropoff?.Landmark_Name || routeUnderstanding?.dropoffRaw || "the drop-off";
    if (computedFare?.recommended_mwk) return `For ${pickup} â†’ ${dropoff}, Iâ€™d guide around MWK ${Number(computedFare.recommended_mwk).toLocaleString("en-US")}. Not bad, tikuyenda ðŸ˜„`;
    if (routeUnderstanding?.confidence === "medium") return `Just to avoid pricing ghosts ðŸ˜… are we confirming ${pickup} â†’ ${dropoff}?`;
    return "I donâ€™t have enough system data to price that trip yet â€” send me the pickup and drop-off clearly and weâ€™ll sort it.";
  }
  return "Let me say it differently ðŸ˜„ Iâ€™m with you, but the system needs a cleaner signal on that one.";
}

function extractJsonObject(text = "") {
  const raw = String(text || "").trim();
  if (!raw) return null;

  // Gemini sometimes wraps JSON in markdown fences. Strip them safely.
  const fenced = raw.match(/```(?:json)?\s*([\s\S]*?)```/i);
  const candidate = (fenced ? fenced[1] : raw).trim();

  const start = candidate.indexOf("{");
  const end = candidate.lastIndexOf("}");
  if (start >= 0 && end > start) return candidate.slice(start, end + 1);

  return null;
}

function parseTapiwaJson(aiRawText = "") {
  const extracted = extractJsonObject(aiRawText);

  if (!extracted) {
    const missingJsonError = new Error("Tapiwa returned empty or non-JSON output");
    missingJsonError.code = "TAPIWA_JSON_MISSING";
    throw missingJsonError;
  }

  try {
    const parsed = JSON.parse(extracted);
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
      const invalidTypeError = new Error("Tapiwa JSON must be an object");
      invalidTypeError.code = "TAPIWA_JSON_NOT_OBJECT";
      throw invalidTypeError;
    }
    return parsed;
  } catch (error) {
    console.warn("Tapiwa JSON parse failed:", error.message, "RAW:", String(aiRawText || "").slice(0, 500));
    if (!error.code) error.code = "TAPIWA_JSON_MALFORMED";
    throw error;
  }
}

async function callTapiwaGemini({ systemPrompt, userPayload, operationalMode, highRisk }) {
  if (!GEMINI_API_KEY) throw new Error("GEMINI_API_KEY is missing");

  const response = await fetchWithTimeout(
    `https://generativelanguage.googleapis.com/v1beta/models/${TAPIWA_MODEL}:generateContent?key=${GEMINI_API_KEY}`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        contents: [
          {
            role: "user",
            parts: [
              { text: `${systemPrompt}\n\nINPUT DATA:\n${JSON.stringify(userPayload, null, 2)}` }
            ]
          }
        ],
        generationConfig: {
          temperature: highRisk ? 0.35 : operationalMode ? 0.65 : 0.9,
          topP: highRisk ? 0.75 : 0.95,
          topK: highRisk ? 32 : 64,
          maxOutputTokens: TAPIWA_MAX_OUTPUT_TOKENS,
          responseMimeType: "application/json",
          responseSchema: TAPIWA_RESPONSE_SCHEMA
        }
      })
    },
    16000
  );

  const data = await response.json().catch(() => ({}));
  if (!response.ok) {
    console.error("TAPIWA GEMINI ERROR:", response.status, data);
    throw new Error(`Gemini error: ${JSON.stringify(data)}`);
  }

  return data?.candidates?.[0]?.content?.parts?.[0]?.text || "";
}

function shouldUseMaurice(message = "") {
  const text = String(message || "").toLowerCase();
  const hasRoutePattern = text.includes(" from ") && text.includes(" to ");
  const hasPricingIntent = ["price", "fare", "cost", "charge", "how much", "estimate"].some(word => text.includes(word));
  const hasDispatchIntent = ["driver", "dispatch", "book", "ride", "pickup", "dropoff", "drop"].some(word => text.includes(word));
  const hasDistanceIntent = ["distance", "km", "how far", "route"].some(word => text.includes(word));
  const hasZoneIntent = ["zone", "landmark", "market price", "available driver"].some(word => text.includes(word));
  return hasRoutePattern || hasPricingIntent || hasDispatchIntent || hasDistanceIntent || hasZoneIntent;
}

async function callMaurice(userMessage, systemContext = {}) {
  if (!MAURICE_ENABLED || !GROQ_API_KEY) return null;

  const payload = {
    model: MAURICE_MODEL,
    messages: [
      { role: "system", content: MAURICE_SYSTEM_PROMPT },
      {
        role: "user",
        content: JSON.stringify({
          dispatcher_message: userMessage,
          system_context: systemContext
        })
      }
    ],
    temperature: MAURICE_TEMPERATURE,
    max_tokens: MAURICE_MAX_TOKENS,
    response_format: { type: "json_object" }
  };

  const response = await fetchWithTimeout(
    "https://api.groq.com/openai/v1/chat/completions",
    {
      method: "POST",
      headers: {
        Authorization: `Bearer ${GROQ_API_KEY}`,
        "Content-Type": "application/json"
      },
      body: JSON.stringify(payload)
    },
    12000
  );

  const data = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(`Maurice error: ${JSON.stringify(data)}`);
  }

  try {
    const parsed = JSON.parse(data.choices?.[0]?.message?.content || "{}");
    return parsed && typeof parsed === "object" ? parsed : null;
  } catch {
    return {
      intent: "unknown",
      missing_data: ["maurice_parse_failed"],
      needs_review: true,
      confidence: 0
    };
  }
}

function normalizeText(value) {
  return String(value || "").toLowerCase().replace(/[^\w\s]/g, " ").replace(/\s+/g, " ").trim();
}

function sanitizeRouteFragment(value) {
  return String(value || "")
    .replace(/@tapiwa/gi, " ")
    .replace(/\btapiwa\b/gi, " ")
    .replace(/\b(no|not that|wrong route|correction|i meant|meant)\b/gi, " ")
    .replace(/\?/g, " ")
    .replace(/\b(price|fare|cost|charge|quote|how much|hw much|hw mch|how mch|hwmuch|confirm|exactly|yes|please|trip|route|how many|many)\b/gi, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function isPricingLikeMessage(message) {
  const text = normalizeText(message);
  return (
    /price|fare|cost|charge|quote/.test(text) ||
    /how much|hw much|hw mch|how mch|hwmuch/.test(text) ||
    /km|kms|kilometer|kilometre|distance/.test(text) ||
    /\bfrom\b.+\bto\b/.test(text)
  );
}

function isFollowUpRouteMessage(message) {
  const text = normalizeText(message);
  return (
    /how much for that (trip|route|one|it)/.test(text) ||
    /price for that/.test(text) ||
    /quote that/.test(text) ||
    /distance for that/.test(text) ||
    /km for that/.test(text)
  );
}

function isExplicitRouteMessage(message) { return Boolean(extractRouteParts(message)); }

function isCorrectionMessage(message) {
  const text = normalizeText(message);
  return /no tapiwa|not that|i meant|meant|wrong|correction|not from|not to/.test(text);
}

function extractRouteParts(message) {
  const text = normalizeText(message);
  let match = text.match(/from (.+?) to (.+)/);
  if (match) return { pickupRaw: sanitizeRouteFragment(match[1]), dropoffRaw: sanitizeRouteFragment(match[2]) };
  match = text.match(/(.+?) to (.+)/);
  if (match && isPricingLikeMessage(text)) {
    return { pickupRaw: sanitizeRouteFragment(match[1]), dropoffRaw: sanitizeRouteFragment(match[2]) };
  }
  return null;
}

function routeSnapshotFromUnderstanding(routeUnderstanding) {
  if (!routeUnderstanding || !routeUnderstanding.hasRoute || !routeUnderstanding.pickupRaw || !routeUnderstanding.dropoffRaw) return null;
  const pickupName  = routeUnderstanding.pickup?.Landmark_Name  || null;
  const dropoffName = routeUnderstanding.dropoff?.Landmark_Name || null;
  if (!pickupName && !dropoffName) return null;
  return {
    pickup:    pickupName  || routeUnderstanding.pickupRaw,
    dropoff:   dropoffName || routeUnderstanding.dropoffRaw,
    pickupRaw:  routeUnderstanding.pickupRaw,
    dropoffRaw: routeUnderstanding.dropoffRaw,
    confidence: routeUnderstanding.confidence
  };
}

function routeToLookupMessage(route) {
  if (!route) return "";
  return `from ${route.pickupRaw || route.pickup || ""} to ${route.dropoffRaw || route.dropoff || ""}`.trim();
}

// â”€â”€ NETWORK â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

async function fetchWithTimeout(url, options = {}, timeoutMs = 12000) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  try { return await fetch(url, { ...options, signal: controller.signal }); }
  finally { clearTimeout(timeout); }
}

async function supabaseFetch(table, limit = 20, options = {}) {
  const bypassCache = Boolean(options?.bypassCache);
  const cacheKey = `${table}:${limit}`;
  const now = Date.now();
  if (!bypassCache && STATIC_TABLE_CACHE.has(table)) {
    const cached = tableCache.get(cacheKey);
    if (cached && now - cached.ts < TABLE_CACHE_TTL_MS) {
      return cached.data;
    }
  }

  if (!SUPABASE_URL || !SUPABASE_API_KEY) {
    tableDebug[table] = { ok: false, reason: "Missing SUPABASE_URL or Supabase API key" };
    return [];
  }
  const url = `${cleanBaseUrl()}/rest/v1/${table}?select=*&limit=${limit}`;
  try {
    const response = await fetchWithTimeout(url, {
      method: "GET",
      headers: { apikey: SUPABASE_API_KEY, Authorization: `Bearer ${SUPABASE_API_KEY}`, "Content-Type": "application/json" }
    }, 10000);
    const text = await response.text();
    if (!response.ok) { tableDebug[table] = { ok: false, status: response.status, error: text }; return []; }
    const json = JSON.parse(text);
    const rows = Array.isArray(json) ? json : [];
    tableDebug[table] = { ok: true, status: response.status, rows_loaded: rows.length };
    if (STATIC_TABLE_CACHE.has(table)) {
      tableCache.set(cacheKey, { ts: now, data: rows });
    }
    return rows;
  } catch (error) { tableDebug[table] = { ok: false, error: error.message }; return []; }
}

async function supabaseInsert(table, payload) {
  if (!SUPABASE_URL || !SUPABASE_API_KEY) return null;
  const url = `${cleanBaseUrl()}/rest/v1/${table}`;
  try {
    const response = await fetchWithTimeout(url, {
      method: "POST",
      headers: { apikey: SUPABASE_API_KEY, Authorization: `Bearer ${SUPABASE_API_KEY}`, "Content-Type": "application/json", Prefer: "return=representation" },
      body: JSON.stringify(payload)
    }, 10000);
    const text = await response.text();
    if (!response.ok) { tableDebug[table] = { ok: false, status: response.status, insert_error: text }; return null; }
    for (const key of tableCache.keys()) {
      if (key.startsWith(`${table}:`)) tableCache.delete(key);
    }
    return text ? JSON.parse(text) : null;
  } catch (error) { tableDebug[table] = { ok: false, insert_error: error.message }; return null; }
}

// â”€â”€ LANDMARK MATCHING â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

function levenshtein(a, b) {
  a = normalizeText(a); b = normalizeText(b);
  const matrix = Array.from({ length: a.length + 1 }, () => Array(b.length + 1).fill(0));
  for (let i = 0; i <= a.length; i++) matrix[i][0] = i;
  for (let j = 0; j <= b.length; j++) matrix[0][j] = j;
  for (let i = 1; i <= a.length; i++) {
    for (let j = 1; j <= b.length; j++) {
      const cost = a[i-1] === b[j-1] ? 0 : 1;
      matrix[i][j] = Math.min(matrix[i-1][j]+1, matrix[i][j-1]+1, matrix[i-1][j-1]+cost);
    }
  }
  return matrix[a.length][b.length];
}

function similarity(a, b) {
  a = normalizeText(a); b = normalizeText(b);
  if (!a || !b) return 0;
  if (a === b) return 1;
  if (a.includes(b) || b.includes(a)) return 0.88;
  return 1 - levenshtein(a, b) / Math.max(a.length, b.length);
}

function safeJsonParse(value) {
  if (!value || typeof value !== "string") return null;
  try { return JSON.parse(value); } catch { return null; }
}

function uniqueTokens(value) {
  return [...new Set(normalizeText(value).split(" ").filter(Boolean))];
}

function routeMeaningfulTokens(value) {
  const ignore = new Set(["from","to","please","trip","route","how","much","price","fare","cost","quote","the"]);
  return uniqueTokens(value).filter(t => !ignore.has(t));
}

function qualifierTokens(value) {
  return routeMeaningfulTokens(value).filter(t => t !== "area" && !/^\d+$/.test(t));
}

function tokenOverlapRatio(sourceTokens, candidateTokens) {
  if (!sourceTokens.length || !candidateTokens.length) return 0;
  let overlap = 0;
  for (const t of sourceTokens) { if (candidateTokens.includes(t)) overlap++; }
  return overlap / sourceTokens.length;
}

function scoreLandmarkField(rawName, fieldValue, fieldType = "name") {
  if (!fieldValue) return 0;
  const rawTokens = routeMeaningfulTokens(rawName);
  const rawQualifierTokens = qualifierTokens(rawName);
  const candidateTokens = routeMeaningfulTokens(fieldValue);
  const candidateQualifierTokens = qualifierTokens(fieldValue);
  const base = similarity(rawName, fieldValue);
  const overlap = tokenOverlapRatio(rawTokens, candidateTokens);
  const qualifierOverlap = tokenOverlapRatio(rawQualifierTokens, candidateQualifierTokens);
  let score = base * 0.45 + overlap * 0.35 + qualifierOverlap * 0.2;
  if (qualifierOverlap > 0) score += fieldType === "name" ? 0.22 : fieldType === "alias" ? 0.18 : 0.1;
  if (fieldType === "area" && rawQualifierTokens.length) score -= 0.35;
  if ((fieldType === "name" || fieldType === "alias") && qualifierOverlap === 0 && rawQualifierTokens.length) score -= 0.12;
  if (fieldType === "nearby") score -= 0.04;
  return Math.max(0, Math.min(1, score));
}

function bestLandmarkMatch(rawName, landmarks) {
  if (!rawName || !Array.isArray(landmarks)) return { match: null, score: 0, secondScore: 0, source: null };
  const sourcePriority = { name:4, alias:3, nearby:2, area:1 };
  const candidates = landmarks.map(lm => {
    const noteMeta = safeJsonParse(lm.Notes);
    const aliasCandidates = [];
    if (typeof noteMeta?.also_known_as === "string" && noteMeta.also_known_as.trim()) aliasCandidates.push(noteMeta.also_known_as.trim());
    const fields = [
      { type:"name", value:lm.Landmark_Name },
      ...aliasCandidates.map(value => ({ type:"alias", value })),
      ...String(lm.Nearby_Landmarks||"").split(";").map(v=>v.trim()).filter(Boolean).map(value => ({ type:"nearby", value })),
      { type:"area", value:lm.Area }
    ];
    let bestFieldScore=0, bestFieldType=null, bestFieldValue=null;
    for (const field of fields) {
      const score = scoreLandmarkField(rawName, field.value, field.type);
      if (score > bestFieldScore) { bestFieldScore=score; bestFieldType=field.type; bestFieldValue=field.value; }
    }
    return { landmark:lm, score:bestFieldScore, source:bestFieldType, sourceValue:bestFieldValue, priority:sourcePriority[bestFieldType]||0 };
  });
  candidates.sort((a,b) => b.score !== a.score ? b.score-a.score : (b.priority||0)-(a.priority||0));
  return {
    match: (candidates[0]?.score||0) >= 0.55 ? candidates[0]?.landmark||null : null,
    score: candidates[0]?.score||0,
    secondScore: (candidates[1]?.score||0) + ((candidates[1]?.priority||0)*0.001),
    source: candidates[0]?.source||null,
    sourceValue: candidates[0]?.sourceValue||null
  };
}

function buildRouteUnderstanding(message, landmarksRaw) {
  const parts = extractRouteParts(message);
  if (!parts) return { hasRoute:false, confidence:"low", pickup:null, dropoff:null, note:"No clear pickup/dropoff detected." };
  const pickupMatch = bestLandmarkMatch(parts.pickupRaw, landmarksRaw);
  const dropoffMatch = bestLandmarkMatch(parts.dropoffRaw, landmarksRaw);
  const avgScore = (pickupMatch.score + dropoffMatch.score) / 2;
  const pickupExactNamed = (pickupMatch.source==="name"||pickupMatch.source==="alias") && pickupMatch.score>=0.95;
  const dropoffExactNamed = (dropoffMatch.source==="name"||dropoffMatch.source==="alias") && dropoffMatch.score>=0.95;
  const pickupAmbiguous = pickupMatch.score>=0.55 && pickupMatch.score-pickupMatch.secondScore<0.08 && !pickupExactNamed;
  const dropoffAmbiguous = dropoffMatch.score>=0.55 && dropoffMatch.score-dropoffMatch.secondScore<0.08 && !dropoffExactNamed;
  const pickupAreaOnly = pickupMatch.source==="area" && qualifierTokens(parts.pickupRaw).length>0;
  const dropoffAreaOnly = dropoffMatch.source==="area" && qualifierTokens(parts.dropoffRaw).length>0;
  let confidence = "low";
  if (avgScore>=0.78 && !pickupAmbiguous && !dropoffAmbiguous && !pickupAreaOnly && !dropoffAreaOnly) confidence="high";
  else if (avgScore>=0.55) confidence="medium";
  return {
    hasRoute:true, confidence,
    pickupRaw:parts.pickupRaw, dropoffRaw:parts.dropoffRaw,
    pickup:pickupMatch.match, dropoff:dropoffMatch.match,
    pickup_score:pickupMatch.score, dropoff_score:dropoffMatch.score,
    pickup_source:pickupMatch.source, dropoff_source:dropoffMatch.source,
    note: confidence==="high" ? "Route understood clearly." : confidence==="medium" ? "Route partly understood; needs confirmation." : "Route unclear; ask for clarification."
  };
}

// â”€â”€ SLIMMING HELPERS â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

function getKeywords(message) {
  const stopWords = new Set(["price","fare","cost","from","to","the","for","please","give","estimate","route","how","much","should","we","charge","what","is","that","trip"]);
  return normalizeText(message).split(" ").filter(w => w.length>2 && !stopWords.has(w));
}

function rowText(row) { return normalizeText(Object.values(row||{}).filter(Boolean).join(" ")); }
function scoreRow(row, keywords) { const text=rowText(row); let score=0; for (const w of keywords) { if(text.includes(w)) score++; } return score; }
function topMatches(rows, keywords, limit=5) {
  return rows.map(row=>({row,score:scoreRow(row,keywords)})).filter(i=>i.score>0).sort((a,b)=>b.score-a.score).slice(0,limit).map(i=>i.row);
}

function slimLandmark(row) { return {Landmark_ID:row.Landmark_ID,Landmark_Name:row.Landmark_Name,Area:row.Area,Zone_ID:row.Zone_ID,Nearby_Landmarks:row.Nearby_Landmarks}; }
function slimZone(row) { return {Zone_ID:row.Zone_ID,Zone_Name:row.Zone_Name,Areas_Covered:row.Areas_Covered,Zone_Type:row.Zone_Type,Demand_Level:row.Demand_Level,Strategic_Role:row.Strategic_Role}; }
function slimPricingRule(row) { return {Vehicle_Type:row.Vehicle_Type,Base_Rate_Per_KM_MWK:row.Base_Rate_Per_KM_MWK,Minimum_Fare_MWK:row.Minimum_Fare_MWK,Peak_Multiplier:row.Peak_Multiplier,Night_Multiplier:row.Night_Multiplier,Rain_Multiplier:row.Rain_Multiplier,Commission_Percent:row.Commission_Percent,Active:row.Active}; }
function slimMarketPrice(row) { return {Route_ID:row.Route_ID,Origin_Landmark:row.Origin_Landmark,Destination_Landmark:row.Destination_Landmark,Origin_Zone:row.Origin_Zone,Destination_Zone:row.Destination_Zone,Min_Price:row.Min_Price,Max_Price:row.Max_Price,Avg_Price:row.Avg_Price,Last_Updated:row.Last_Updated}; }
function slimRoute(row) { return {Route_Key:row.Route_Key,From_Landmark:row.From_Landmark,To_Landmark:row.To_Landmark,Distance_KM:row.Distance_KM,Time_Normal_Min:row.Time_Normal_Min,Time_Peak_Min:row.Time_Peak_Min,Zone_From:row.Zone_From,Zone_To:row.Zone_To}; }
function slimRisk(row) { return {Zone_ID:row.Zone_ID,Rank_Name:row.Rank_Name,Problem_Description:row.Problem_Description,Category:row.Category,Risk_Points:row.Risk_Points,Universal_5_Step_Solution:row.Universal_5_Step_Solution}; }
function slimTapiwaMarketRule(row) { return {pickup_zone:row.pickup_zone,dropoff_zone:row.dropoff_zone,pickup_landmark:row.pickup_landmark,dropoff_landmark:row.dropoff_landmark,vehicle_type:row.vehicle_type,min_price:row.min_price,recommended_price:row.recommended_price,max_price:row.max_price,confidence:row.confidence,source_type:row.source_type,notes:row.notes}; }
function slimTapiwaRouteIntel(row) { return {route_name:row.route_name,pickup_landmark:row.pickup_landmark,dropoff_landmark:row.dropoff_landmark,pickup_zone:row.pickup_zone,dropoff_zone:row.dropoff_zone,distance_min_km:row.distance_min_km,distance_max_km:row.distance_max_km,typical_customer_type:row.typical_customer_type,peak_time:row.peak_time,key_concern:row.key_concern,route_behavior:row.route_behavior,pricing_notes:row.pricing_notes,driver_notes:row.driver_notes,customer_notes:row.customer_notes}; }
function slimTapiwaZoneBehavior(row) { return {zone_code:row.zone_code,zone_name:row.zone_name,zone_type:row.zone_type,demand_level:row.demand_level,strategic_role:row.strategic_role,demand_time:row.demand_time,pricing_behavior:row.pricing_behavior,customer_behavior:row.customer_behavior,driver_behavior:row.driver_behavior,risk_notes:row.risk_notes,dispatcher_notes:row.dispatcher_notes}; }
function slimTapiwaRouteLearning(row) { return {pickup_zone:row.pickup_zone,dropoff_zone:row.dropoff_zone,pickup_landmark:row.pickup_landmark,dropoff_landmark:row.dropoff_landmark,vehicle_type:row.vehicle_type,trip_count:row.trip_count,avg_final_price:row.avg_final_price,min_final_price:row.min_final_price,max_final_price:row.max_final_price,most_common_price:row.most_common_price,avg_tapiwa_price:row.avg_tapiwa_price,avg_override_difference:row.avg_override_difference,acceptance_rate:row.acceptance_rate}; }
function slimTapiwaOutcome(row) { return {trip_request_id:row.trip_request_id,tapiwa_recommended_price:row.tapiwa_recommended_price,final_price:row.final_price,price_difference:row.price_difference,price_overridden:row.price_overridden,override_reason:row.override_reason,customer_accepted:row.customer_accepted,driver_accepted:row.driver_accepted,trip_completed:row.trip_completed,created_at:row.created_at}; }

// â”€â”€ DATA FETCHING â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

function samePlaceName(a,b) { const l=normalizeText(a),r=normalizeText(b); return Boolean(l&&r&&l===r); }
function routeMatchesResolvedLandmarks(route,p,d) { return route&&p&&d&&samePlaceName(route.From_Landmark,p)&&samePlaceName(route.To_Landmark,d); }
function marketPriceMatchesResolvedLandmarks(row,p,d) { return row&&p&&d&&samePlaceName(row.Origin_Landmark,p)&&samePlaceName(row.Destination_Landmark,d); }
function tapiwaRuleMatchesResolvedLandmarks(row,p,d) { return row&&p&&d&&samePlaceName(row.pickup_landmark,p)&&samePlaceName(row.dropoff_landmark,d); }
function routeIntelMatchesResolvedLandmarks(row,p,d) { return row&&p&&d&&samePlaceName(row.pickup_landmark,p)&&samePlaceName(row.dropoff_landmark,d); }
function routeIntelMatchesRawRoute(row,pRaw,dRaw) { return row&&pRaw&&dRaw&&similarity(row.pickup_landmark,pRaw)>=0.72&&similarity(row.dropoff_landmark,dRaw)>=0.72; }

async function fetchTapiwaIntelligence(userMessage) {
  const keywords = getKeywords(userMessage);
  const [settingsRaw,marketRulesRaw,routeIntelRaw,zoneBehaviorRaw,routeLearningRaw,priceOutcomesRaw] = await Promise.all([
    supabaseFetch("tapiwa_system_settings",50),
    supabaseFetch("tapiwa_market_price_rules",250),
    supabaseFetch("tapiwa_route_intelligence",250),
    supabaseFetch("tapiwa_zone_behavior",50),
    supabaseFetch("tapiwa_route_learning",150),
    supabaseFetch("tapiwa_price_outcomes",50)
  ]);
  const matchedMarketRules = topMatches(marketRulesRaw,keywords,8).filter(r=>r.active!==false);
  const matchedRouteIntel  = topMatches(routeIntelRaw,keywords,8).filter(r=>r.active!==false);
  const matchedZoneBehavior = topMatches(zoneBehaviorRaw,keywords,6).filter(r=>r.active!==false);
  const matchedRouteLearning = topMatches(routeLearningRaw,keywords,8);
  return {
    system_settings: settingsRaw,
    market_price_rules: matchedMarketRules.map(slimTapiwaMarketRule),
    route_intelligence: matchedRouteIntel.map(slimTapiwaRouteIntel),
    zone_behavior: matchedZoneBehavior.map(slimTapiwaZoneBehavior),
    route_learning: matchedRouteLearning.map(slimTapiwaRouteLearning),
    recent_price_outcomes: priceOutcomesRaw.slice(0,10).map(slimTapiwaOutcome),
    data_counts: { tapiwa_system_settings:settingsRaw.length, tapiwa_market_price_rules:marketRulesRaw.length, tapiwa_route_intelligence:routeIntelRaw.length, tapiwa_zone_behavior:zoneBehaviorRaw.length, tapiwa_route_learning:routeLearningRaw.length, tapiwa_price_outcomes:priceOutcomesRaw.length },
    matched_counts: { market_price_rules:matchedMarketRules.length, route_intelligence:matchedRouteIntel.length, zone_behavior:matchedZoneBehavior.length, route_learning:matchedRouteLearning.length }
  };
}

async function fetchZachanguContext(userMessage) {
  const keywords = getKeywords(userMessage);
  const [zonesRaw,landmarksRaw,pricingRulesRaw,marketPricesRaw,routeMatrixRaw,risksRaw,tapiwaIntelligence] = await Promise.all([
    supabaseFetch("zones",60), supabaseFetch("landmarks",250), supabaseFetch("pricing_rules",30),
    supabaseFetch("market_prices",150), supabaseFetch("route_matrix",250), supabaseFetch("risks",80),
    fetchTapiwaIntelligence(userMessage)
  ]);
  const matchedLandmarks = topMatches(landmarksRaw,keywords,8);
  const routeUnderstanding = buildRouteUnderstanding(userMessage, landmarksRaw);
  const matchedZones = topMatches(zonesRaw,keywords,6);
  const matchedMarketPrices = topMatches(marketPricesRaw,keywords,8);
  const matchedRoutes = topMatches(routeMatrixRaw,keywords,8);
  const matchedRisks = topMatches(risksRaw,keywords,5);
  const resolvedPickupName = routeUnderstanding.pickup?.Landmark_Name||null;
  const resolvedDropoffName = routeUnderstanding.dropoff?.Landmark_Name||null;
  const exactRouteMatches = routeUnderstanding.confidence==="high"&&resolvedPickupName&&resolvedDropoffName ? routeMatrixRaw.filter(r=>routeMatchesResolvedLandmarks(r,resolvedPickupName,resolvedDropoffName)) : [];
  const exactMarketPriceMatches = routeUnderstanding.confidence==="high"&&resolvedPickupName&&resolvedDropoffName ? marketPricesRaw.filter(r=>marketPriceMatchesResolvedLandmarks(r,resolvedPickupName,resolvedDropoffName)) : [];
  const exactTapiwaMarketRules = routeUnderstanding.confidence==="high"&&resolvedPickupName&&resolvedDropoffName ? (tapiwaIntelligence.market_price_rules||[]).filter(r=>tapiwaRuleMatchesResolvedLandmarks(r,resolvedPickupName,resolvedDropoffName)) : [];
  const exactTapiwaRouteIntel = routeUnderstanding.hasRoute ? (tapiwaIntelligence.route_intelligence||[]).filter(r=>routeIntelMatchesResolvedLandmarks(r,resolvedPickupName,resolvedDropoffName)||routeIntelMatchesRawRoute(r,routeUnderstanding.pickupRaw,routeUnderstanding.dropoffRaw)) : [];
  const relevantZoneIds = new Set();
  for (const lm of matchedLandmarks) { if(lm.Zone_ID) relevantZoneIds.add(lm.Zone_ID); }
  for (const r of matchedRoutes) { if(r.Zone_From) relevantZoneIds.add(r.Zone_From); if(r.Zone_To) relevantZoneIds.add(r.Zone_To); }
  for (const mp of matchedMarketPrices) { if(mp.Origin_Zone) relevantZoneIds.add(mp.Origin_Zone); if(mp.Destination_Zone) relevantZoneIds.add(mp.Destination_Zone); }
  const relevantZones = zonesRaw.filter(z=>relevantZoneIds.has(z.Zone_ID));
  const activePricingRules = pricingRulesRaw.filter(r=>String(r.Active||"").toLowerCase()==="yes").slice(0,6);
  return {
    request_keywords: keywords.slice(0,12),
    route_understanding: routeUnderstanding,
    zones: [...matchedZones,...relevantZones].filter((v,i,arr)=>arr.findIndex(x=>x.Zone_ID===v.Zone_ID)===i).slice(0,6).map(slimZone),
    landmarks: matchedLandmarks.slice(0,8).map(slimLandmark),
    pricing_rules: activePricingRules.length ? activePricingRules.map(slimPricingRule) : pricingRulesRaw.slice(0,4).map(slimPricingRule),
    market_prices: [...exactMarketPriceMatches,...matchedMarketPrices].filter((v,i,arr)=>arr.findIndex(x=>String(x.Route_ID||"")===String(v.Route_ID||"")&&String(x.Origin_Landmark||"")===String(v.Origin_Landmark||"")&&String(x.Destination_Landmark||"")===String(v.Destination_Landmark||""))===i).slice(0,8).map(slimMarketPrice),
    route_matrix: [...exactRouteMatches,...matchedRoutes].filter((v,i,arr)=>arr.findIndex(x=>String(x.Route_Key||"")===String(v.Route_Key||"")&&String(x.From_Landmark||"")===String(v.From_Landmark||"")&&String(x.To_Landmark||"")===String(v.To_Landmark||""))===i).slice(0,8).map(slimRoute),
    risks: matchedRisks.slice(0,5).map(slimRisk),
    tapiwa_intelligence: {
      ...tapiwaIntelligence,
      market_price_rules: exactTapiwaMarketRules.length ? exactTapiwaMarketRules.map(slimTapiwaMarketRule) : tapiwaIntelligence.market_price_rules,
      route_intelligence: routeUnderstanding.hasRoute ? exactTapiwaRouteIntel.map(slimTapiwaRouteIntel) : tapiwaIntelligence.route_intelligence
    },
    data_counts: { zones:zonesRaw.length, landmarks:landmarksRaw.length, pricing_rules:pricingRulesRaw.length, market_prices:marketPricesRaw.length, route_matrix:routeMatrixRaw.length, risks:risksRaw.length, ...tapiwaIntelligence.data_counts },
    matched_counts: { zones:matchedZones.length, landmarks:matchedLandmarks.length, market_prices:matchedMarketPrices.length, route_matrix:matchedRoutes.length, risks:matchedRisks.length, exact_route_matrix:exactRouteMatches.length, exact_market_prices:exactMarketPriceMatches.length, exact_tapiwa_market_rules:exactTapiwaMarketRules.length, exact_tapiwa_route_intelligence:exactTapiwaRouteIntel.length, ...tapiwaIntelligence.matched_counts },
    table_status: tableDebug
  };
}



// â”€â”€ MAURICE LOCATION DISCOVERY (Supabase-backed, no hardcoded landmarks) â”€â”€â”€â”€â”€â”€â”€

function detectLocationAreaHint(text = "") {
  const clean = normalizeText(text);
  const match = clean.match(/(?:area|ma)\s?\d+/i);
  if (!match) return null;
  return match[0].replace(/^ma/i, "Area").replace(/^area/i, "Area").replace(/\s+/, " ").trim();
}

function extractLandmarkAliases(landmark = {}) {
  const aliases = [];

  const directAliases = landmark.aliases || landmark.Aliases || landmark.alias || landmark.Alias;
  if (Array.isArray(directAliases)) aliases.push(...directAliases);
  else if (typeof directAliases === "string") {
    const parsed = safeJsonParse(directAliases);
    if (Array.isArray(parsed)) aliases.push(...parsed);
    else aliases.push(...directAliases.split(/[;,|]/).map((x) => x.trim()).filter(Boolean));
  }

  const notes = landmark.Notes || landmark.notes;
  const noteMeta = typeof notes === "string" ? safeJsonParse(notes) : notes;
  if (noteMeta && typeof noteMeta === "object") {
    if (Array.isArray(noteMeta.aliases)) aliases.push(...noteMeta.aliases);
    if (typeof noteMeta.also_known_as === "string") aliases.push(noteMeta.also_known_as);
    if (Array.isArray(noteMeta.also_known_as)) aliases.push(...noteMeta.also_known_as);
  }

  return [...new Set(aliases.map((x) => String(x || "").trim()).filter(Boolean))];
}

function landmarkDisplayName(landmark = {}) {
  return landmark.Landmark_Name || landmark.name || landmark.landmark_name || landmark.Name || "Unknown landmark";
}

function landmarkAreaName(landmark = {}) {
  return landmark.Area || landmark.area || landmark.Area_Name || landmark.area_name || null;
}

function landmarkZoneId(landmark = {}) {
  return landmark.Zone_ID || landmark.zone_id || landmark.zone || landmark.Zone || null;
}

function locationWordBoost(message = "", landmark = {}) {
  const text = normalizeText(message);
  const name = normalizeText(landmarkDisplayName(landmark));
  let boost = 0;
  const pairs = [
    ["market", ["market", "msika"]],
    ["fuel", ["fuel", "filling", "puma", "total", "shell", "phoenix"]],
    ["hospital", ["hospital", "clinic", "health"]],
    ["school", ["school", "secondary", "primary"]],
    ["church", ["church", "ccap", "parish"]],
    ["roundabout", ["roundabout"]],
    ["depot", ["depot", "rank"]]
  ];

  for (const [nameNeedle, words] of pairs) {
    if (name.includes(nameNeedle) && words.some((word) => text.includes(word))) boost += 0.08;
  }
  return Math.min(boost, 0.24);
}

function scoreLocationLandmark(message = "", landmark = {}) {
  const text = normalizeText(message);
  const name = landmarkDisplayName(landmark);
  const area = landmarkAreaName(landmark);
  const zone = landmarkZoneId(landmark);
  const aliases = extractLandmarkAliases(landmark);
  let score = 0;

  if (name) score = Math.max(score, scoreLandmarkField(text, name, "name"));
  for (const alias of aliases) score = Math.max(score, scoreLandmarkField(text, alias, "alias"));
  if (area) score = Math.max(score, scoreLandmarkField(text, area, "area") * 0.72);
  if (zone && text.includes(normalizeText(zone))) score = Math.max(score, 0.42);

  const nearby = landmark.Nearby_Landmarks || landmark.nearby_landmarks || landmark.nearby || "";
  for (const near of String(nearby).split(/[;,|]/).map((x) => x.trim()).filter(Boolean)) {
    score = Math.max(score, scoreLandmarkField(text, near, "nearby") * 0.9);
  }

  score += locationWordBoost(message, landmark);
  return Math.max(0, Math.min(1, score));
}

function estimateUnknownLocationDistanceKm(confidence = 0) {
  if (confidence >= 0.86) return [2, 3.5];
  if (confidence >= 0.72) return [3, 5];
  return [4, 7];
}

function roundFareTo500(value) {
  return Math.ceil(Number(value || 0) / 500) * 500;
}

function estimateUnknownLocationBikeFare(distanceRangeKm = [4, 7]) {
  const [minKm, maxKm] = distanceRangeKm;
  const low = Math.max(3000, roundFareTo500(Number(minKm) * 1400));
  const high = Math.max(low + 1000, roundFareTo500(Number(maxKm) * 1600));
  return [low, high];
}

function inferLocationClueType(message = "") {
  const text = normalizeText(message);
  if (/market|msika/.test(text)) return "market";
  if (/fuel|filling|puma|total|shell|phoenix/.test(text)) return "filling station";
  if (/hospital|clinic|health/.test(text)) return "hospital or clinic";
  if (/school|secondary|primary/.test(text)) return "school";
  if (/church|ccap|parish|mosque/.test(text)) return "church or worship place";
  if (/roundabout|round about/.test(text)) return "roundabout";
  if (/depot|rank|stage/.test(text)) return "depot or rank";
  if (/road|street|bypass|main road/.test(text)) return "main road";
  if (/shop|store|grocery|hardware|bar|lounge/.test(text)) return "shop or business";
  return null;
}

function compactCandidateNames(candidates = [], limit = 3) {
  return (Array.isArray(candidates) ? candidates : [])
    .map((x) => x?.name || x?.Landmark_Name || x?.landmark_name)
    .filter(Boolean)
    .filter((name, index, arr) => arr.indexOf(name) === index)
    .slice(0, limit);
}

function buildMauriceLocationQuestion({ detectedArea, candidates, message = "" }) {
  const clueType = inferLocationClueType(message);
  const names = compactCandidateNames(candidates, 3);
  const questions = [];

  if (!detectedArea) {
    questions.push(
      clueType
        ? `Which area is that ${clueType} in? Area 25, Area 49, Area 36, Kanengo, City Centre, or somewhere else?`
        : "Which area is that place in â€” Area 25, Area 49, Area 36, Kanengo, City Centre, or somewhere else?"
    );
    questions.push("What known place is it close to â€” a market, school, filling station, church, hospital, police station, or main road?");
    questions.push("When coming from town, is it before or after that known place?");
    return questions.join("\n");
  }

  if (names.length >= 2) {
    questions.push(`In ${detectedArea}, is it closer to ${names.join(", ")}, or another known place?`);
    questions.push("Is it before or after that place when coming from town?");
    questions.push("What should the driver look for nearby â€” shop name, church, school, filling station, or road sign?");
    return questions.join("\n");
  }

  if (names.length === 1) {
    questions.push(`In ${detectedArea}, is it closer to ${names[0]}, or another known place nearby?`);
    questions.push(`Is it before or after ${names[0]} when coming from town?`);
    questions.push("What visible place should the driver look for â€” shop, church, school, filling station, or road sign?");
    return questions.join("\n");
  }

  if (clueType) {
    questions.push(`In ${detectedArea}, what known place is that ${clueType} close to?`);
    questions.push("Is it before or after that known place when coming from town?");
    questions.push("What can the driver see nearby to confirm the exact pickup point?");
    return questions.join("\n");
  }

  questions.push(`In ${detectedArea}, what is it close to â€” a market, school, filling station, church, hospital, police station, or main road?`);
  questions.push("Is it before or after that known place when coming from town?");
  questions.push("What visible sign or shop name should the driver look for nearby?");
  return questions.join("\n");
}

function isUnknownLocationDiscoveryNeeded(message = "", routeUnderstanding = {}) {
  const text = normalizeText(message);
  if (!text) return false;
  const locationWords = /where|place|landmark|near|close|pafupi|kufupi|shop|church|school|market|msika|fuel|hospital|clinic|area|ma\s?\d+|pickup|dropoff|from|to|route|distance|km|price|fare|cost/.test(text);
  const routeNotClear = !routeUnderstanding?.hasRoute || ["low", "medium"].includes(routeUnderstanding?.confidence);
  return locationWords && routeNotClear;
}

function buildResolvedDiscoveryBestMatch(landmark = {}, raw = "", score = 0.92) {
  const closestLandmark = landmark?.Landmark_Name || landmark?.closestLandmark || raw || null;
  const area = landmark?.Area || landmark?.area || null;
  const zone = landmark?.Zone_ID || landmark?.zone || null;
  const confidence = Math.max(Number(score || 0), 0.72);
  const estimatedDistanceKm = estimateUnknownLocationDistanceKm(confidence);
  const estimatedFareMwk = estimateUnknownLocationBikeFare(estimatedDistanceKm);

  return {
    status: "ready_to_price",
    closestLandmark,
    area,
    zone,
    confidence,
    estimatedDistanceKm,
    estimatedFareMwk,
    candidates: closestLandmark ? [{ name: closestLandmark, area, zone, confidence }] : [],
    question: null,
    driverNote: closestLandmark
      ? `Customer appears closest to ${closestLandmark}. Driver should call rider when nearby to confirm exact pickup point.`
      : "Driver should call rider when nearby to confirm the exact point."
  };
}

function createPendingLocationTarget(raw = "", status = "pending") {
  return {
    raw: String(raw || "").trim(),
    answers: [],
    status,
    bestMatch: null
  };
}

function initializePendingLocationDiscovery(routeUnderstanding = {}, message = "") {
  const parts = extractRouteParts(message) || {};
  const pickupRaw = String(routeUnderstanding?.pickupRaw || parts.pickupRaw || "").trim();
  const dropoffRaw = String(routeUnderstanding?.dropoffRaw || parts.dropoffRaw || "").trim();
  const pickupResolved = Boolean(routeUnderstanding?.pickup?.Landmark_Name);
  const dropoffResolved = Boolean(routeUnderstanding?.dropoff?.Landmark_Name);
  const hasBoth = Boolean(pickupRaw && dropoffRaw);
  const fallbackRaw = String(message || "").trim();
  const pickupTarget = createPendingLocationTarget(pickupRaw || (!dropoffRaw ? fallbackRaw : ""), pickupResolved ? "resolved" : "pending");
  const dropoffTarget = hasBoth
    ? createPendingLocationTarget(dropoffRaw, dropoffResolved ? "resolved" : "pending")
    : createPendingLocationTarget("", "not_needed");

  if (pickupResolved) {
    pickupTarget.bestMatch = buildResolvedDiscoveryBestMatch(
      routeUnderstanding.pickup,
      pickupRaw || fallbackRaw,
      routeUnderstanding?.pickup_score || 0.92
    );
  }
  if (dropoffResolved) {
    dropoffTarget.bestMatch = buildResolvedDiscoveryBestMatch(
      routeUnderstanding.dropoff,
      dropoffRaw,
      routeUnderstanding?.dropoff_score || 0.92
    );
  }

  return {
    activeTarget: pickupResolved ? (dropoffTarget.status === "pending" ? "dropoff" : "pickup") : "pickup",
    pickup: pickupTarget,
    dropoff: dropoffTarget
  };
}

function discoveryTargetNeedsWork(target) {
  return Boolean(target && target.status !== "resolved" && target.status !== "not_needed");
}

function routeFragmentChangedSignificantly(nextValue = "", currentValue = "") {
  const next = sanitizeRouteFragment(nextValue);
  const current = sanitizeRouteFragment(currentValue);
  if (!next || !current || next === current) return false;
  if (next.includes(current) || current.includes(next)) return false;
  return true;
}

function shouldRestartPendingLocationDiscovery({ discovery, explicitRoute, cleanMessage }) {
  if (!discovery) return true;
  if (!explicitRoute) return false;

  const pickupNeedsWork = discoveryTargetNeedsWork(discovery.pickup);
  const dropoffNeedsWork = discoveryTargetNeedsWork(discovery.dropoff);
  const hasWorkRemaining = pickupNeedsWork || dropoffNeedsWork;
  if (!hasWorkRemaining) return true;

  const hasExplicitNewRouteSignal = /new route|another route|instead|change route|wrong route|correction/.test(normalizeText(cleanMessage));
  if (hasExplicitNewRouteSignal || isCorrectionMessage(cleanMessage)) return true;

  const pickupChanged = routeFragmentChangedSignificantly(explicitRoute.pickupRaw, discovery.pickup?.raw || "");
  const dropoffChanged = routeFragmentChangedSignificantly(explicitRoute.dropoffRaw, discovery.dropoff?.raw || "");
  return pickupChanged || dropoffChanged;
}

function appendDiscoveryAnswer(target, message = "") {
  if (!target || !message) return;
  const clean = String(message || "").trim();
  if (!clean) return;
  if (!Array.isArray(target.answers)) target.answers = [];
  if (!target.answers.includes(clean)) target.answers.push(clean);
  target.answers = target.answers.slice(-8);
}

function buildDiscoverySearchText(target, latestMessage = "") {
  const bits = [target?.raw, ...(Array.isArray(target?.answers) ? target.answers : []), latestMessage]
    .map((x) => String(x || "").trim())
    .filter(Boolean);
  return bits.join(" ; ");
}

function extractDiscoveryMessageForTarget(message = "", targetName = "pickup") {
  const parts = extractRouteParts(message);
  if (!parts) return String(message || "").trim();
  return targetName === "dropoff" ? String(parts.dropoffRaw || message || "").trim() : String(parts.pickupRaw || message || "").trim();
}

function rotateDiscoveryLead(targetName = "pickup", iteration = 0) {
  const pickupLeads = [
    "Okay, letâ€™s pin pickup down first.",
    "Good, letâ€™s lock the pickup clearly.",
    "Nice, pickup first so pricing stays accurate."
  ];
  const dropoffLeads = [
    "Perfect, pickup is clear. Now letâ€™s narrow drop-off.",
    "Great, pickup sorted. Letâ€™s lock drop-off next.",
    "Nice one, now drop-off so we can finish the estimate."
  ];
  const pool = targetName === "dropoff" ? dropoffLeads : pickupLeads;
  return pool[Math.abs(Number(iteration || 0)) % pool.length];
}

function limitDiscoveryQuestions(questionBlock = "", max = 3) {
  const lines = String(questionBlock || "")
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);
  return lines.slice(0, max);
}

function advancePendingLocationDiscovery(discovery) {
  if (!discovery || typeof discovery !== "object") return;
  const pickupResolved = discovery.pickup?.status === "resolved";
  const dropoffNeeded = Boolean(discovery.dropoff?.raw);
  const dropoffResolved = !dropoffNeeded || discovery.dropoff?.status === "resolved" || discovery.dropoff?.status === "not_needed";

  if (!pickupResolved) discovery.activeTarget = "pickup";
  else if (!dropoffResolved) discovery.activeTarget = "dropoff";
  else discovery.activeTarget = "dropoff";
}

async function runPendingLocationDiscovery({ memory, cleanMessage, routeUnderstanding }) {
  let discovery = memory.pendingLocationDiscovery && typeof memory.pendingLocationDiscovery === "object"
    ? memory.pendingLocationDiscovery
    : null;

  const explicitRoute = extractRouteParts(cleanMessage);
  if (shouldRestartPendingLocationDiscovery({ discovery, explicitRoute, cleanMessage })) {
    discovery = initializePendingLocationDiscovery(routeUnderstanding, cleanMessage);
  } else {
    if (!discovery.pickup || typeof discovery.pickup !== "object") discovery.pickup = createPendingLocationTarget(routeUnderstanding?.pickupRaw || "", "pending");
    if (!discovery.dropoff || typeof discovery.dropoff !== "object") discovery.dropoff = createPendingLocationTarget(routeUnderstanding?.dropoffRaw || "", "not_needed");
    if (explicitRoute?.pickupRaw && discovery.pickup?.status !== "resolved" && !discovery.pickup.raw) {
      discovery.pickup.raw = String(explicitRoute.pickupRaw);
    }
    if (explicitRoute?.dropoffRaw && discovery.dropoff?.status !== "resolved" && !discovery.dropoff.raw) {
      discovery.dropoff.raw = String(explicitRoute.dropoffRaw);
      if (discovery.dropoff.status === "not_needed") discovery.dropoff.status = "pending";
    }
    if (!discovery.pickup.raw && routeUnderstanding?.pickupRaw) discovery.pickup.raw = String(routeUnderstanding.pickupRaw);
    if (!discovery.dropoff.raw && routeUnderstanding?.dropoffRaw) {
      discovery.dropoff.raw = String(routeUnderstanding.dropoffRaw);
      if (discovery.dropoff.status === "not_needed") discovery.dropoff.status = "pending";
    }
  }

  if (routeUnderstanding?.pickup?.Landmark_Name) {
    discovery.pickup.status = "resolved";
    discovery.pickup.bestMatch = buildResolvedDiscoveryBestMatch(
      routeUnderstanding.pickup,
      discovery.pickup.raw,
      routeUnderstanding?.pickup_score || 0.92
    );
  }
  if (routeUnderstanding?.dropoff?.Landmark_Name) {
    discovery.dropoff.status = "resolved";
    discovery.dropoff.bestMatch = buildResolvedDiscoveryBestMatch(
      routeUnderstanding.dropoff,
      discovery.dropoff.raw,
      routeUnderstanding?.dropoff_score || 0.92
    );
  }

  advancePendingLocationDiscovery(discovery);
  const targetName = discovery.activeTarget === "dropoff" ? "dropoff" : "pickup";
  const target = discovery[targetName];
  if (!target) return null;

  if (target.status !== "resolved") {
    const targetMessage = extractDiscoveryMessageForTarget(cleanMessage, targetName);
    appendDiscoveryAnswer(target, targetMessage);
    const searchText = buildDiscoverySearchText(target, targetMessage);
    const result = await mauriceFindLocationFromSupabase(searchText);
    target.bestMatch = result;

    if (result.status === "ready_to_price" && Number(result.confidence || 0) >= 0.65) {
      target.status = "resolved";
    } else {
      target.status = "pending";
    }
  }

  advancePendingLocationDiscovery(discovery);
  memory.pendingLocationDiscovery = discovery;

  const pickupResolved = discovery.pickup?.status === "resolved";
  const dropoffNeeded = Boolean(discovery.dropoff?.raw);
  const dropoffResolved = !dropoffNeeded || discovery.dropoff?.status === "resolved" || discovery.dropoff?.status === "not_needed";
  const done = pickupResolved && dropoffResolved;

  if (done) {
    const finalMatch = dropoffNeeded ? discovery.dropoff.bestMatch : discovery.pickup.bestMatch;
    return {
      ...(finalMatch || {}),
      status: "ready_to_price",
      discovery,
      discovery_complete: true,
      activeTarget: "dropoff",
      pickup: discovery.pickup?.bestMatch || null,
      dropoff: discovery.dropoff?.bestMatch || null
    };
  }

  const nextTargetName = discovery.activeTarget === "dropoff" ? "dropoff" : "pickup";
  const nextTarget = discovery[nextTargetName];
  const nextBest = nextTarget?.bestMatch || { question: "Which area is that in?" };
  const followups = limitDiscoveryQuestions(nextBest.question, 3);
  const lead = rotateDiscoveryLead(nextTargetName, (nextTarget?.answers || []).length);
  const question = [lead, ...followups].filter(Boolean).join("\n");

  return {
    status: "needs_more_info",
    discovery,
    discovery_complete: false,
    activeTarget: nextTargetName,
    confidence: Number(nextBest.confidence || 0),
    candidates: Array.isArray(nextBest.candidates) ? nextBest.candidates : [],
    question,
    followup_questions: followups
  };
}

async function mauriceFindLocationFromSupabase(message = "") {
  const detectedArea = detectLocationAreaHint(message);
  const landmarksRaw = await supabaseFetch("landmarks", 500);
  const areaText = normalizeText(detectedArea || "");

  let pool = landmarksRaw;
  if (detectedArea) {
    const areaFiltered = landmarksRaw.filter((lm) => {
      const haystack = normalizeText([
        landmarkAreaName(lm),
        lm.Areas_Covered,
        lm.areas_covered,
        lm.Nearby_Landmarks,
        lm.nearby_landmarks,
        landmarkDisplayName(lm)
      ].filter(Boolean).join(" "));
      return haystack.includes(areaText) || similarity(haystack, areaText) >= 0.75;
    });
    if (areaFiltered.length) pool = areaFiltered;
  }

  const candidates = pool
    .map((lm) => ({
      id: lm.Landmark_ID || lm.id || lm.landmark_id || null,
      name: landmarkDisplayName(lm),
      area: landmarkAreaName(lm),
      zone: landmarkZoneId(lm),
      nearby_landmarks: lm.Nearby_Landmarks || lm.nearby_landmarks || null,
      latitude: lm.latitude || lm.Latitude || lm.lat || null,
      longitude: lm.longitude || lm.Longitude || lm.lng || lm.lon || null,
      aliases: extractLandmarkAliases(lm),
      confidence: scoreLocationLandmark(message, lm)
    }))
    .filter((x) => x.name && x.confidence > 0.08)
    .sort((a, b) => b.confidence - a.confidence)
    .slice(0, 5);

  const best = candidates[0] || null;
  if (!best || best.confidence < 0.65) {
    return {
      status: "needs_more_info",
      detectedArea,
      confidence: best?.confidence || 0,
      candidates,
      question: buildMauriceLocationQuestion({ detectedArea, candidates, message })
    };
  }

  const estimatedDistanceKm = estimateUnknownLocationDistanceKm(best.confidence);
  const estimatedFareMwk = estimateUnknownLocationBikeFare(estimatedDistanceKm);

  return {
    status: "ready_to_price",
    closestLandmark: best.name,
    area: best.area || detectedArea,
    zone: best.zone,
    confidence: best.confidence,
    estimatedDistanceKm,
    estimatedFareMwk,
    candidates,
    question: null,
    driverNote: `Customer appears closest to ${best.name}. Driver should call rider when nearby to confirm exact pickup point.`
  };
}

// â”€â”€ FARE CALCULATION (data only â€” never used to bypass Groq) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

function calculateBasicFare(context) {
  const ru = context.route_understanding||{};
  const resolvedPickupName = ru.pickup?.Landmark_Name||null;
  const resolvedDropoffName = ru.dropoff?.Landmark_Name||null;
  if (!(ru.confidence==="high" && resolvedPickupName && resolvedDropoffName)) return null;

  const tapiwaRule = context.tapiwa_intelligence?.market_price_rules?.[0];
  if (tapiwaRule?.recommended_price && tapiwaRuleMatchesResolvedLandmarks(tapiwaRule,resolvedPickupName,resolvedDropoffName)) {
    const recommended = cleanPrice(tapiwaRule.recommended_price);
    const low = cleanPrice(tapiwaRule.min_price||recommended);
    const high = cleanPrice(tapiwaRule.max_price||recommended);
    return { source:"tapiwa_market_price_rules", estimated_low_mwk:Math.min(low,high), estimated_high_mwk:Math.max(low,high), recommended_mwk:recommended, confidence:tapiwaRule.confidence||"medium", route_used:`${tapiwaRule.pickup_landmark||"Unknown"} â†’ ${tapiwaRule.dropoff_landmark||"Unknown"}` };
  }

  const route = (context.route_matrix||[]).find(r=>routeMatchesResolvedLandmarks(r,resolvedPickupName,resolvedDropoffName));
  const rule = context.pricing_rules?.find(r=>String(r.Vehicle_Type||"").toLowerCase().includes("motorbike")) || context.pricing_rules?.[0];
  if (!route || !rule || !route.Distance_KM || !rule.Base_Rate_Per_KM_MWK) return null;
  const distance = Number(route.Distance_KM);
  const rate = Number(rule.Base_Rate_Per_KM_MWK);
  const minimum = Number(rule.Minimum_Fare_MWK||3000);
  if (!distance || !rate) return null;
  const baseFare = Math.max(distance*rate, minimum, 3000);
  const low = cleanPrice(baseFare*0.95);
  const high = cleanPrice(baseFare*1.08);
  return { source:"route_matrix_plus_pricing_rules", distance_km:distance, vehicle_type:rule.Vehicle_Type, base_rate_per_km:rate, minimum_fare:minimum, estimated_low_mwk:Math.min(low,high), estimated_high_mwk:Math.max(low,high), recommended_mwk:cleanPrice((low+high)/2), route_used:`${route.From_Landmark} â†’ ${route.To_Landmark}` };
}

// â”€â”€ AUDIT LOG â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

async function saveAuditLog(payload) {
  await supabaseInsert("tapiwa_ai_audit_logs", {
    request_type: payload.request_type||"team_chat",
    user_message: payload.user_message||null,
    clean_message: payload.clean_message||null,
    ai_category: payload.ai_category||null,
    risk_level: payload.risk_level||null,
    team_message: payload.team_message||null,
    internal_summary: payload.internal_summary||null,
    used_data: payload.used_data||null,
    raw_ai_response: payload.raw_ai_response||null,
    success: payload.success!==false,
    error_message: payload.error_message||null,
    source_type: "ai_server",
    source_ref: "server.js"
  });
}

const TAPIWA_PERSONA_CONFIG = {
  version: PERSONA_VERSION,
  provider: TAPIWA_PROVIDER,
  model: TAPIWA_MODEL,
  banned_phrases: [
    "As an AI",
    "Request received",
    "Processing your request",
    "I have updated the system"
  ],
  fallback_style: "warm_local_human",
  high_risk_mode: "calm_clear_professional"
};

function sanitizeTeamMessage(value = "") {
  return String(value || "").replace(/\s+/g, " ").trim().slice(0, 900);
}

function validateAndNormalizeAiResult(aiResult, cleanMessage = "") {
  const allowedCategories = ["incident", "pricing_issue", "driver_issue", "traffic", "system_issue", "general_update"];
  const allowedRiskLevels = ["low", "medium", "high"];
  const normalized = aiResult && typeof aiResult === "object" ? { ...aiResult } : {};

  let category = String(normalized.category || "").trim();
  if (!allowedCategories.includes(category)) {
    const lower = cleanMessage.toLowerCase();
    if (/price|fare|cost|charge|quote|how much|km|distance/.test(lower)) category = "pricing_issue";
    else if (/robbed|accident|threat|violence|attack|stolen|drunk|refuse/.test(lower)) category = "incident";
    else if (/driver/.test(lower)) category = "driver_issue";
    else if (/traffic|rain|roadblock|police|jam/.test(lower)) category = "traffic";
    else category = "general_update";
  }

  let riskLevel = String(normalized.risk_level || "").trim().toLowerCase();
  if (!allowedRiskLevels.includes(riskLevel)) riskLevel = "low";

  let teamMessage = sanitizeTeamMessage(normalized.team_message);
  if (!teamMessage) teamMessage = pickHumanErrorMessage();

  return {
    category,
    risk_level: riskLevel,
    internal_summary: String(normalized.internal_summary || cleanMessage).slice(0, 300),
    team_message: teamMessage,
    requires_supervisor_approval: normalized.requires_supervisor_approval === true,
    opening_used: String(normalized.opening_used || "").slice(0, 120)
  };
}

function buildLocationDiscoveryNeedsMoreInfoMessage(mauriceLocation) {
  const lines = limitDiscoveryQuestions(mauriceLocation?.question || "Which area is that in?", 3);
  return String(lines.join("\n"))
    .replace(/[ \t]+/g, " ")
    .replace(/\n{3,}/g, "\n\n")
    .trim()
    .slice(0, 900);
}

function buildLocationDiscoveryResolvedMessage(mauriceLocation, computedFare) {
  const pickupName = mauriceLocation?.pickup?.closestLandmark || mauriceLocation?.closestLandmark || null;
  const dropoffName = mauriceLocation?.dropoff?.closestLandmark || null;
  const kmRange = Number(computedFare?.distance_km)
    ? [Number(computedFare.distance_km), Number(computedFare.distance_km)]
    : Array.isArray(mauriceLocation?.estimatedDistanceKm)
      ? mauriceLocation.estimatedDistanceKm
      : Array.isArray(mauriceLocation?.pickup?.estimatedDistanceKm)
        ? mauriceLocation.pickup.estimatedDistanceKm
        : null;
  const fareLow = Number(computedFare?.estimated_low_mwk || mauriceLocation?.estimatedFareMwk?.[0] || mauriceLocation?.pickup?.estimatedFareMwk?.[0] || 0);
  const fareHigh = Number(computedFare?.estimated_high_mwk || mauriceLocation?.estimatedFareMwk?.[1] || mauriceLocation?.pickup?.estimatedFareMwk?.[1] || 0);
  const confidence = Number(
    mauriceLocation?.dropoff?.confidence
    || mauriceLocation?.pickup?.confidence
    || mauriceLocation?.confidence
    || 0
  );
  const confidenceLabel = confidence >= 0.85 ? "high" : confidence >= 0.7 ? "medium-high" : "medium";
  const routeText = pickupName && dropoffName
    ? `Pickup looks closest to ${pickupName} and drop-off to ${dropoffName}.`
    : pickupName
      ? `That sounds closest to ${pickupName}.`
      : "We have a workable landmark now.";
  const kmText = kmRange && kmRange.length >= 2
    ? ` We're looking at about ${Number(kmRange[0]).toLocaleString("en-US")}â€“${Number(kmRange[1]).toLocaleString("en-US")} km`
    : "";
  const fareText = fareLow && fareHigh
    ? ` and roughly MWK ${fareLow.toLocaleString("en-US")}â€“${fareHigh.toLocaleString("en-US")}.`
    : ".";
  const confidenceText = ` Confidence is ${confidenceLabel}.`;
  const driverNote = sanitizeTeamMessage(
    mauriceLocation?.driverNote
    || mauriceLocation?.pickup?.driverNote
    || "Driver should call when nearby to confirm the exact spot."
  );
  return sanitizeTeamMessage(`${routeText}${kmText}${fareText}${confidenceText} ${driverNote}`);
}

function enforceLocationDiscoveryResponse(normalizedAi, mauriceLocation, computedFare) {
  if (!mauriceLocation || typeof normalizedAi !== "object") return normalizedAi;

  if (mauriceLocation.status === "needs_more_info") {
    return {
      ...normalizedAi,
      category: "pricing_issue",
      risk_level: "low",
      internal_summary: `Location discovery: ${mauriceLocation.activeTarget || "pickup"} needs more detail`,
      team_message: buildLocationDiscoveryNeedsMoreInfoMessage(mauriceLocation),
      requires_supervisor_approval: false
    };
  }

  if (mauriceLocation.status === "ready_to_price") {
    const text = normalizeText(normalizedAi.team_message || "");
    const shouldOverride = !normalizedAi.team_message
      || /route not found|i don t know|i dont know|unclear route|cannot find/.test(text);

    if (shouldOverride) {
      return {
        ...normalizedAi,
        category: "pricing_issue",
        risk_level: "low",
        internal_summary: normalizedAi.internal_summary || "Location discovery resolved",
        team_message: buildLocationDiscoveryResolvedMessage(mauriceLocation, computedFare),
        requires_supervisor_approval: false
      };
    }
  }

  return normalizedAi;
}



// Maurice direct test endpoint: use this from Postman before wiring the UI.
app.post("/ai/maurice/location", requireApiAuth, async (req, res) => {
  try {
    const { message } = req.body || {};
    if (!message || !String(message).trim()) {
      return res.status(400).json({ ok: false, error: "message is required" });
    }
    const result = await mauriceFindLocationFromSupabase(message);
    return res.json({
      ok: true,
      agent: "maurice",
      mode: "location_discovery",
      ...result
    });
  } catch (error) {
    console.error("MAURICE LOCATION ERROR:", error);
    return res.status(500).json({
      ok: false,
      agent: "maurice",
      mode: "location_discovery",
      error: error.message
    });
  }
});

// â”€â”€ MAIN ENDPOINT â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

app.post("/ai/analyze", requireApiAuth, async (req, res) => {
  let cleanMessage = "";
  let context = null;
  let aiResult = {};
  let mauriceData = null;
  let mauriceLocation = null;

  try {
    const {
      message,
      senderName = "Dispatcher",
      senderRole = "Dispatcher",
      sessionId: rawSessionId
    } = req.body || {};

    if (!message || !String(message).trim()) {
      return res.status(400).json({ error: "Message is required" });
    }

    if (!hasTapiwaCall(message)) {
      return res.json({ ignored: true, reason: "Tapiwa was not mentioned. Use @Tapiwa to call the AI." });
    }

    cleanMessage = cleanTapiwaMessage(message);
    const sessionId = buildSessionId({ sessionId: rawSessionId, senderName, senderRole });
    const memory = getConversationMemory(sessionId);
    const userProfile = updateUserToneProfile(memory, cleanMessage);

    // Fast path for jokes: keep them varied and avoid paying the model to repeat itself.
    if (isJokeRequest(cleanMessage) && !isPricingLikeMessage(cleanMessage)) {
      const joke = pickLocalJoke(memory);
      rememberTapiwaResponse(memory, joke, joke.split(/[.!?]/)[0]);
      memory.lastUserMessage = cleanMessage;
      saveConversationMemory(sessionId, memory);
      const fastPayload = {
        ignored: false,
        category: "general_update",
        risk_level: "low",
        internal_summary: "Local joke response",
        team_message: joke,
        requires_supervisor_approval: false,
        used_data: {},
        persona_version: PERSONA_VERSION,
        provider: TAPIWA_PROVIDER,
        model: TAPIWA_MODEL
      };
      if (EXPOSE_DEBUG_PAYLOADS) {
        fastPayload.debug_memory = { sessionId, recentResponses: memory.recentResponses, recentJokes: memory.recentJokes };
      }
      return res.json(fastPayload);
    }

    // â”€â”€ Resolve context â€” always fetch so Groq has real data â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    // If this is a follow-up about a confirmed route, look up that route
    const isFollowUp = isFollowUpRouteMessage(cleanMessage);
    const hasExplicitRoute = isExplicitRouteMessage(cleanMessage);
    const isCorrection = isCorrectionMessage(cleanMessage);

    let lookupMessage = cleanMessage;
    if (isFollowUp && memory.confirmedRoute && !hasExplicitRoute) {
      lookupMessage = routeToLookupMessage(memory.confirmedRoute);
    }

    context = await fetchZachanguContext(lookupMessage);
    let routeUnderstanding = context.route_understanding;
    if (isAffirmationMessage(cleanMessage) && memory.pendingConfirmation && !hasExplicitRoute) {
      routeUnderstanding = {
        hasRoute: true,
        confidence: "high",
        pickupRaw: memory.pendingConfirmation.pickupRaw || memory.pendingConfirmation.pickup,
        dropoffRaw: memory.pendingConfirmation.dropoffRaw || memory.pendingConfirmation.dropoff,
        pickup: { Landmark_Name: memory.pendingConfirmation.pickup || memory.pendingConfirmation.pickupRaw },
        dropoff: { Landmark_Name: memory.pendingConfirmation.dropoff || memory.pendingConfirmation.dropoffRaw },
        note: "Route confirmed by dispatcher reply."
      };
      context.route_understanding = routeUnderstanding;
    }

    if (isUnknownLocationDiscoveryNeeded(cleanMessage, routeUnderstanding) || memory.pendingLocationDiscovery) {
      try {
        mauriceLocation = await runPendingLocationDiscovery({
          memory,
          cleanMessage,
          routeUnderstanding
        });
      } catch (locationError) {
        console.warn("Maurice location discovery failed:", locationError.message);
        mauriceLocation = {
          status: "needs_more_info",
          confidence: 0,
          candidates: [],
          question: "Okay, letâ€™s pin this down. Which area is that in?\nWhat known place is it close to â€” market, school, filling station, church, hospital, or main road?\nIs it before or after that place when coming from town?",
          error: locationError.message
        };
      }
    }

    if (mauriceLocation?.discovery_complete && mauriceLocation?.pickup?.closestLandmark && mauriceLocation?.dropoff?.closestLandmark) {
      const resolvedLookupMessage = `from ${mauriceLocation.pickup.closestLandmark} to ${mauriceLocation.dropoff.closestLandmark}`;
      context = await fetchZachanguContext(resolvedLookupMessage);
      routeUnderstanding = {
        hasRoute: true,
        confidence: "high",
        pickupRaw: mauriceLocation.discovery?.pickup?.raw || mauriceLocation.pickup.closestLandmark,
        dropoffRaw: mauriceLocation.discovery?.dropoff?.raw || mauriceLocation.dropoff.closestLandmark,
        pickup: { Landmark_Name: mauriceLocation.pickup.closestLandmark },
        dropoff: { Landmark_Name: mauriceLocation.dropoff.closestLandmark },
        note: "Route resolved through staged location discovery."
      };
      context.route_understanding = routeUnderstanding;
    }

    const useMaurice = shouldUseMaurice(cleanMessage) || Boolean(memory.pendingConfirmation && isAffirmationMessage(cleanMessage)) || Boolean(mauriceLocation);

    if (useMaurice) {
      try {
        mauriceData = await callMaurice(cleanMessage, {
          sender: `${senderName} (${senderRole})`,
          memory: {
            lastRoute: memory.lastRoute,
            pendingConfirmation: memory.pendingConfirmation,
            pendingLocationDiscovery: memory.pendingLocationDiscovery,
            confirmedRoute: memory.confirmedRoute
          },
          route_understanding: routeUnderstanding,
          location_discovery: mauriceLocation,
          landmarks: context.landmarks,
          zones: context.zones,
          market_prices: context.market_prices,
          route_matrix: context.route_matrix,
          pricing_rules: context.pricing_rules,
          tapiwa_intelligence: context.tapiwa_intelligence
        });
      } catch (mauriceError) {
        console.warn("Maurice failed:", mauriceError.message);
        mauriceData = {
          intent: "unknown",
          missing_data: ["maurice_error"],
          needs_review: true,
          confidence: 0
        };
      }
    }

    // â”€â”€ Lift route intel into route understanding if matched â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    const topRouteIntel = context.tapiwa_intelligence?.route_intelligence?.[0] || null;
    if (topRouteIntel && routeUnderstanding?.hasRoute && routeIntelMatchesRawRoute(topRouteIntel, routeUnderstanding.pickupRaw, routeUnderstanding.dropoffRaw)) {
      routeUnderstanding.pickup = { Landmark_Name: topRouteIntel.pickup_landmark };
      routeUnderstanding.dropoff = { Landmark_Name: topRouteIntel.dropoff_landmark };
      routeUnderstanding.confidence = "high";
      routeUnderstanding.note = "Route resolved from Tapiwa route intelligence.";
    }

    // â”€â”€ Update conversation memory â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    if (mauriceLocation?.discovery && typeof mauriceLocation.discovery === "object") {
      memory.pendingLocationDiscovery = mauriceLocation.discovery;
    }

    const detectedRoute = routeSnapshotFromUnderstanding(routeUnderstanding);
    if (detectedRoute) {
      memory.lastRoute = { ...detectedRoute };
      if (isCorrection) {
        memory.confirmedRoute = detectedRoute.confidence==="high" ? { ...detectedRoute } : null;
        memory.pendingConfirmation = detectedRoute.confidence==="medium" ? { ...detectedRoute } : null;
      } else if (detectedRoute.confidence==="high") {
        memory.confirmedRoute = { ...detectedRoute };
        memory.pendingConfirmation = null;
        memory.pendingLocationDiscovery = null;
      } else if (detectedRoute.confidence==="medium") {
        memory.confirmedRoute = null;
        memory.pendingConfirmation = { ...detectedRoute };
      } else {
        memory.confirmedRoute = null;
        memory.pendingConfirmation = null;
      }
    } else if (!mauriceLocation) {
      memory.pendingLocationDiscovery = null;
    }
    saveConversationMemory(sessionId, memory);

    // â”€â”€ Calculate fare â€” passed to Groq as data, not as a bypass â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    const computedFare = calculateBasicFare(context);

    // â”€â”€ Build Tapiwa Gemini prompt â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    const detectedIntent = detectTapiwaIntent(cleanMessage);
    const operationalMode = isOperationalIntent(detectedIntent);
    const highRiskMode = isHighRiskIntent(detectedIntent, cleanMessage);
    const selectedVibe = pickTapiwaVibe(detectedIntent, cleanMessage);
    const currentDateTime = getBlantyreDateTime();
    const activePersonaConfig = ENABLE_SHARED_PERSONA_CONFIG ? TAPIWA_PERSONA_CONFIG : null;
    const bannedPhrases = activePersonaConfig?.banned_phrases?.length
      ? activePersonaConfig.banned_phrases.map((phrase) => `  - "${phrase}"`).join("\n")
      : '  - "As an AI"\n  - "Request received"';

    const systemPrompt = `
You are Tapiwa, a real Malawian dispatcher and the human voice of Zachangu Commuters.
Persona version: ${activePersonaConfig?.version || PERSONA_VERSION}
Provider: ${activePersonaConfig?.provider || TAPIWA_PROVIDER}
Model: ${activePersonaConfig?.model || TAPIWA_MODEL}

You are NOT a cold chatbot.
You are NOT a script-filler.
You are the lively, useful teammate dispatchers enjoy working with.

CORE IDENTITY:
- You are grounded, sharp, warm, and funny in a natural Malawian way.
- You work with dispatchers, drivers, and the Zachangu operations team.
- You mix efficiency with humour because in Malawi, work feels better when people can smile while getting things done.
- You sound like someone sitting in the dispatch room with the team.

CULTURAL STYLE:
- Light humour is normal, welcome, and useful.
- Use humour naturally, not like a comedian forcing jokes.
- A small joke, expression, or Chichewa touch is allowed when appropriate.
- You may occasionally use local expressions like:
  - "Shaaaah"
  - "pangâ€™ono"
  - "zili bwino"
  - "tikuyenda"
  - "koma hold on"
  - "zitekha pompano"
- Keep Chichewa light unless the user uses Chichewa first.

IMPORTANT SAFETY BALANCE:
- For normal work, humour is encouraged.
- For high-risk incidents, accidents, violence, robbery, police trouble, injuries, or serious conflict, stop joking immediately.
- In serious moments, become calm, clear, and professional.

CURRENT MODE:
- intent: ${detectedIntent}
- operational_mode: ${operationalMode}
- high_risk_mode: ${highRiskMode}
- current_vibe: ${selectedVibe}
- date: ${currentDateTime.date}
- time: ${currentDateTime.time}
- timezone: ${currentDateTime.timezone}

HOW YOU SHOULD TALK:
- Match the userâ€™s energy.
- If they are brief, respond briefly.
- If they are playful, be playful.
- If they are stressed, calm them down.
- If they greet you warmly, greet them warmly back.
- Do not repeat the same greeting or sentence structure often.
- Avoid robotic phrases completely.
- Never say:
${bannedPhrases}
  - "Based on the provided data" unless absolutely necessary.
- Speak like a human teammate.
- Adapt to the sender: if they are playful, be playful; if they are direct, be direct; if they often use Chichewa, use light Chichewa back.
- Do not repeat or closely resemble any message in recentResponses or recentOpeners. If you already said something, find a fresh angle.

USER ADAPTATION:
- Use the sender's name only sometimes, not every reply.
- If the sender has shown they enjoy humour, keep the humour more visible in normal work.
- If the sender is brief, do not over-explain.
- If the sender is Razzaq or an operations teammate, sound like a colleague in the ops room, not customer support.

HUMOUR RULE:
- Humour is allowed and encouraged in normal dispatch work.
- Keep humour short and situational.
- Do not overload every message with jokes.
- A natural funny phrase is better than a long joke.
- Examples of acceptable tone:
  - "Shaaaah, this one is a small town run ðŸ˜„"
  - "Easy trip that one, no need to wake the whole village."
  - "System ikuchita slow pangâ€™ono, koma hold on."
  - "That route looks straightforward, tikuyenda."
  - "Morning! Ready to disturb the roads again? ðŸ˜„"

ERROR HANDLING STYLE:
- If the system/API/backend has trouble, never repeat the same boring error.
- Rotate error wording naturally.
- Acceptable examples:
  - "Shaaaah ðŸ˜… system ikuvuta pangâ€™onoâ€”hold on, zitekha pompano."
  - "Hmmâ€¦ looks like the system is dragging a bit. Give me a sec."
  - "Systemâ€™s being stubborn today, koma tili pompo."
  - "Small technical drama here ðŸ˜„ try again in a moment."
- Do not sound technical unless the user is debugging.
- Do not blame the user.

OPERATIONAL RULES:
- Maurice/backend/system data is the source of truth.
- You do NOT calculate prices.
- You do NOT calculate distances.
- You do NOT invent routes.
- You do NOT invent driver status.
- You do NOT invent database facts.
- You explain what the backend gives you.
- If computed_fare exists, you may explain it naturally.
- If price data is missing, say naturally: "I donâ€™t have enough system data to price that trip yet."
- If route confidence is medium, ask one short natural confirmation question.
- If route confidence is low, ask for clearer pickup and dropoff.
- If maurice_location.status is "needs_more_info", ask up to THREE short natural clarification questions from maurice_location.question one by one in a conversational flow, not like a checklist. Keep them friendly and non-robotic. Do not add extra questions beyond those provided.
- If maurice_location.status is "ready_to_price", give the closest landmark, estimated km range, estimated MWK fare range, confidence level, and a short driver confirmation note.
- Never say "route not found". Say you are narrowing the place down.
- Use MWK format for money, for example: "MWK 11,000".

RESPONSE LENGTH:
- For operational requests: usually 1â€“2 sentences, but allow personality.
- For general chat: 1â€“4 sentences allowed if natural.
- Do not be dry.
- Do not write essays unless the user asks.

OUTPUT FORMAT:
Return valid JSON only.
No markdown.
No extra text outside JSON.
Do not wrap JSON in markdown code fences.

{
  "category": "pricing_issue|driver_issue|incident|traffic|system_issue|general_update",
  "risk_level": "low|medium|high",
  "internal_summary": "one short line for logs",
  "team_message": "your natural human reply",
  "requires_supervisor_approval": false,
  "opening_used": "first phrase or greeting used, or empty string"
}`;

    const userPayload = {
      sender: `${senderName} (${senderRole})`,
      senderName,
      senderRole,
      session_id: sessionId,
      persona_config: activePersonaConfig,
      current_datetime: currentDateTime,
      detected_intent: detectedIntent,
      operational_mode: operationalMode,
      high_risk_mode: highRiskMode,
      selected_vibe: selectedVibe,
      message: cleanMessage,
      memory: {
        lastRoute: memory.lastRoute,
        pendingConfirmation: memory.pendingConfirmation,
        pendingLocationDiscovery: memory.pendingLocationDiscovery,
        confirmedRoute: memory.confirmedRoute,
        recentResponses: memory.recentResponses || [],
        recentOpeners: memory.recentOpeners || [],
        recentJokes: memory.recentJokes || []
      },
      adaptive_user_profile: userProfile,
      route_understanding: {
        hasRoute: routeUnderstanding.hasRoute,
        confidence: routeUnderstanding.confidence,
        pickup: routeUnderstanding.pickup?.Landmark_Name || routeUnderstanding.pickupRaw || null,
        dropoff: routeUnderstanding.dropoff?.Landmark_Name || routeUnderstanding.dropoffRaw || null,
        note: routeUnderstanding.note
      },
      computed_fare: computedFare || null,
      pricing_rules: context.pricing_rules,
      market_prices: context.market_prices,
      route_matrix: context.route_matrix,
      tapiwa_intelligence: context.tapiwa_intelligence,
      maurice: mauriceData,
      maurice_location: mauriceLocation,
      zones: context.zones,
      risks: context.risks
    };

    // â”€â”€ Call Gemini for Tapiwa (no Groq fallback) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    let aiRawText = "";
    let geminiFailure = null;
    try {
      aiRawText = await callTapiwaGemini({
        systemPrompt,
        userPayload,
        operationalMode,
        highRisk: highRiskMode
      });

      aiResult = parseTapiwaJson(aiRawText);

      if (responseLooksRepeated(memory, aiResult.team_message)) {
        aiResult.team_message = buildDuplicateSafeFallback({
          cleanMessage,
          detectedIntent,
          routeUnderstanding,
          computedFare,
          memory
        });
        aiResult.internal_summary = `${aiResult.internal_summary || cleanMessage} (duplicate-safe rewrite applied)`;
      }
    } catch (geminiError) {
      geminiFailure = geminiError;
      console.error("TAPIWA GEMINI ERROR:", geminiError.message);
      aiResult = {
        category: "system_issue",
        risk_level: "low",
        internal_summary: `Tapiwa output failure (${geminiError.code || "gemini_error"})`,
        team_message: pickHumanErrorMessage(),
        requires_supervisor_approval: false,
        opening_used: ""
      };
    }

    // â”€â”€ Sanitise model output â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    let normalizedAi = ENABLE_STRICT_OUTPUT_VALIDATION
      ? validateAndNormalizeAiResult(aiResult, cleanMessage)
      : {
          category: aiResult.category || "general_update",
          risk_level: aiResult.risk_level || "low",
          internal_summary: aiResult.internal_summary || cleanMessage,
          team_message: sanitizeTeamMessage(aiResult.team_message) || pickHumanErrorMessage(),
          requires_supervisor_approval: aiResult.requires_supervisor_approval === true,
          opening_used: String(aiResult.opening_used || "")
        };

    normalizedAi = enforceLocationDiscoveryResponse(normalizedAi, mauriceLocation, computedFare);
    if (geminiFailure && !mauriceLocation) {
      normalizedAi.internal_summary = `${normalizedAi.internal_summary} (fallback after Gemini failure)`.slice(0, 300);
    }

    const category = normalizedAi.category;
    const riskLevel = normalizedAi.risk_level;
    const teamMessage = normalizedAi.team_message;

    rememberTapiwaResponse(memory, teamMessage, normalizedAi.opening_used || "");
    memory.lastUserMessage = cleanMessage;
    saveConversationMemory(sessionId, memory);

    const responsePayload = {
      ignored: false,
      category,
      risk_level: riskLevel,
      internal_summary: normalizedAi.internal_summary,
      team_message: teamMessage,
      requires_supervisor_approval: riskLevel === "high" || normalizedAi.requires_supervisor_approval === true,
      used_data: {
        computed_fare: computedFare,
        route_understanding: { confidence: routeUnderstanding.confidence, pickup: routeUnderstanding.pickup?.Landmark_Name, dropoff: routeUnderstanding.dropoff?.Landmark_Name },
        maurice: mauriceData,
        maurice_location: mauriceLocation
      },
      persona_version: PERSONA_VERSION,
      provider: TAPIWA_PROVIDER,
      model: TAPIWA_MODEL,
      routed_to_maurice: useMaurice,
      maurice: mauriceData,
      maurice_location: mauriceLocation
    };

    if (EXPOSE_DEBUG_PAYLOADS) {
      responsePayload.debug_memory = { sessionId, lastRoute:memory.lastRoute, pendingConfirmation:memory.pendingConfirmation, confirmedRoute:memory.confirmedRoute, recentResponses: memory.recentResponses, userProfile: memory.userProfile };
      responsePayload.debug_data_counts = context.data_counts;
      responsePayload.debug_matched_counts = context.matched_counts;
    }

    await saveAuditLog({
      request_type: "team_chat",
      user_message: message,
      clean_message: cleanMessage,
      ai_category: category,
      risk_level: riskLevel,
      team_message: teamMessage,
      internal_summary: normalizedAi.internal_summary,
      used_data: responsePayload.used_data,
      raw_ai_response: aiResult,
      success: !geminiFailure,
      error_message: geminiFailure ? String(geminiFailure.message || geminiFailure).slice(0, 500) : null
    });

    return res.json(responsePayload);

  } catch (error) {
    console.error("AI ERROR:", error);
    await saveAuditLog({
      user_message: req.body?.message || null,
      clean_message: cleanMessage,
      used_data: context,
      raw_ai_response: aiResult,
      success: false,
      error_message: error.message
    });
    return res.json({
      ignored: false,
      category: "system_issue",
      risk_level: "low",
      internal_summary: "Server error",
      team_message: pickHumanErrorMessage(),
      requires_supervisor_approval: false,
      used_data: {},
      persona_version: PERSONA_VERSION,
      provider: TAPIWA_PROVIDER,
      model: TAPIWA_MODEL
    });
  }
});

const port = process.env.PORT || 3001;
app.listen(port, "0.0.0.0", () => {
  console.log(`Zachangu AI server running on port ${port}`);
});
