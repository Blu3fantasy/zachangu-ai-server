import express from "express";
import cors from "cors";
import dotenv from "dotenv";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

dotenv.config();

const app = express();
app.use(cors());
app.use(express.json({ limit: "1mb" }));

const GROQ_API_KEY = process.env.GROQ_API_KEY;
const TAPIWA_GROQ_API_KEY = process.env.TAPIWA_GROQ_API_KEY;
const GEMMA_API_KEY = process.env.GEMMA_API_KEY || process.env.GEMINI_API_KEY || "";
const TAPIWA_PROVIDER = "gemma";
const TAPIWA_MODEL = process.env.GEMMA_MODEL || process.env.TAPIWA_MODEL || "gemini-2.5-flash";
const GROQ_MODEL = process.env.GROQ_MODEL || "llama-3.1-8b-instant";
const MAURICE_ENABLED = process.env.MAURICE_ENABLED !== "false";
const MAURICE_MODEL = process.env.MAURICE_MODEL || GROQ_MODEL;
const MAURICE_MAX_TOKENS = Number(process.env.MAURICE_MAX_TOKENS || 800);
const MAURICE_TEMPERATURE = Number(process.env.MAURICE_TEMPERATURE || 0.1);
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY || process.env.SUPABASE_ANON || "";
const SUPABASE_API_KEY = SUPABASE_SERVICE_ROLE_KEY || SUPABASE_ANON_KEY;
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const MEMORY_FILE = path.join(__dirname, "conversation-memory.json");
const MEMORY_TTL_MS = 1000 * 60 * 60 * 12;
const MEMORY_MAX_SESSIONS = 300;

const tableDebug = {};
const conversationMemory = new Map();
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

app.get("/", (req, res) => {
  res.json({
    status: "Zachangu AI server is running",
    maurice_groq_ready: Boolean(GROQ_API_KEY),
    gemma_ready: Boolean(GEMMA_API_KEY),
    tapiwa_provider: TAPIWA_PROVIDER,
    tapiwa_model: TAPIWA_MODEL,
    supabase_ready: Boolean(SUPABASE_URL && SUPABASE_API_KEY),
    supabase_url_loaded: cleanBaseUrl() || null,
    tapiwa_intelligence_ready: true
  });
});
app.get("/debug-ai-keys", (req, res) => {
  res.json({
    maurice_key_loaded: Boolean(GROQ_API_KEY),
    tapiwa_key_loaded: Boolean(TAPIWA_GROQ_API_KEY),
    maurice_model: MAURICE_MODEL,
    tapiwa_model: TAPIWA_MODEL,
    maurice_enabled: MAURICE_ENABLED
  });
});
app.get("/env-check", (req, res) => {
  res.json({
    gemma_api_key_loaded: Boolean(GEMMA_API_KEY),
    gemma_key_prefix: GEMMA_API_KEY ? GEMMA_API_KEY.slice(0, 4) : null,
    gemma_model: TAPIWA_MODEL,
    maurice_groq_ready: Boolean(GROQ_API_KEY),
    supabase_ready: Boolean(SUPABASE_URL && SUPABASE_API_KEY)
  });
});
app.get("/debug-tables", async (req, res) => {
  const tables = [
    "zones","landmarks","pricing_rules","market_prices","route_matrix","risks",
    "drivers","trip_requests","tapiwa_system_settings","tapiwa_market_price_rules",
    "tapiwa_route_intelligence","tapiwa_zone_behavior","tapiwa_route_learning",
    "tapiwa_price_outcomes","tapiwa_ai_audit_logs"
  ];
  const result = {};
  for (const table of tables) {
    const data = await supabaseFetch(table, 3);
    result[table] = { rows_loaded: data.length, last_status: tableDebug[table] || null, sample: data.slice(0, 1) };
  }
  res.json(result);
});

// ── UTILS ──────────────────────────────────────────────────────────────────────

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

function saveConversationMemoryToDisk() {
  try {
    pruneConversationMemory();
    const payload = Object.fromEntries(conversationMemory.entries());
    fs.writeFileSync(MEMORY_FILE, JSON.stringify(payload, null, 2), "utf8");
  } catch (error) {
    console.warn("Conversation memory save failed:", error.message);
  }
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
  saveConversationMemoryToDisk();
}

loadConversationMemoryFromDisk();
setInterval(saveConversationMemoryToDisk, 1000 * 60 * 5).unref?.();


// ── TAPIWA GEMINI PERSONALITY HELPERS ─────────────────────────────────────────

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
    "Shaaaah 😅 system ikuvuta pang’ono—hold on, zitekha pompano.",
    "Hmm… looks like the system is dragging a bit. Give me a sec.",
    "Eish, small hiccup here 😄 let me catch it properly.",
    "System’s being stubborn today, koma tili pompo—try again in a moment.",
    "Something’s not responding properly on my side, but don’t panic, we’ll sort it.",
    "Ahh this one has tripped the system a bit 😅 hold on.",
    "Small technical drama here, koma tikuyenda bwino—try again.",
    "Looks like Tapiwa’s line to the system is shaking a bit. Give it a moment.",
    "Shaaaah guys, system ikupanga ma style 😅 hold on.",
    "Bit of a slow moment here, koma I’m still with you."
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
  profile.prefers_humour = profile.prefers_humour || /haha|lol|😂|😅|joke|funny|shaaa|shaah|bo/i.test(text);
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
    "A dispatcher told the map, ‘be honest, are we routing or guessing today?’ The map just zoomed out and kept quiet 😄",
    "One customer said ‘I’m just nearby’… shaaah, in Malawi that can mean 200 metres or a whole zone away 😅",
    "Fuel prices looked at our fare calculator and said, ‘make space, I’m joining the meeting.’",
    "A driver once said ‘I’m two minutes away.’ Even the clock laughed pang’ono 😄",
    "Dispatch life: customer says ‘pa corner’ like Lilongwe has only one corner. Shaaaah 😅",
    "The route was so confusing even ORS wanted a cup of tea before answering.",
    "A map without landmarks in Malawi is just vibes and prayers, koma tikuyenda 😄",
    "Customer: ‘I’m at the shop.’ Dispatcher: ‘Which shop?’ Customer: ‘The one everyone knows.’ Malawi classic 😅"
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
    if (computedFare?.recommended_mwk) return `For ${pickup} → ${dropoff}, I’d guide around MWK ${Number(computedFare.recommended_mwk).toLocaleString("en-US")}. Not bad, tikuyenda 😄`;
    if (routeUnderstanding?.confidence === "medium") return `Just to avoid pricing ghosts 😅 are we confirming ${pickup} → ${dropoff}?`;
    return "I don’t have enough system data to price that trip yet — send me the pickup and drop-off clearly and we’ll sort it.";
  }
  return "Let me say it differently 😄 I’m with you, but the system needs a cleaner signal on that one.";
}

function extractJsonObject(text = "") {
  const raw = String(text || "").trim();
  const fenced = raw.match(/```(?:json)?\s*([\s\S]*?)```/i);
  const candidate = fenced ? fenced[1].trim() : raw;
  const start = candidate.indexOf("{");
  const end = candidate.lastIndexOf("}");
  if (start >= 0 && end > start) return candidate.slice(start, end + 1);
  return candidate;
}

async function callTapiwaGemini({ systemPrompt, userPayload, operationalMode, highRisk }) {
  if (!GEMMA_API_KEY) throw new Error("GEMMA_API_KEY is missing");

  const response = await fetchWithTimeout(
    `https://generativelanguage.googleapis.com/v1beta/models/${TAPIWA_MODEL}:generateContent?key=${GEMMA_API_KEY}`,
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
          maxOutputTokens: operationalMode ? 320 : 450
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

// ── NETWORK ────────────────────────────────────────────────────────────────────

async function fetchWithTimeout(url, options = {}, timeoutMs = 12000) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  try { return await fetch(url, { ...options, signal: controller.signal }); }
  finally { clearTimeout(timeout); }
}

async function supabaseFetch(table, limit = 20) {
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
    tableDebug[table] = { ok: true, status: response.status, rows_loaded: Array.isArray(json) ? json.length : 0 };
    return Array.isArray(json) ? json : [];
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
    return text ? JSON.parse(text) : null;
  } catch (error) { tableDebug[table] = { ok: false, insert_error: error.message }; return null; }
}

// ── LANDMARK MATCHING ──────────────────────────────────────────────────────────

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

// ── SLIMMING HELPERS ───────────────────────────────────────────────────────────

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

// ── DATA FETCHING ──────────────────────────────────────────────────────────────

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

// ── FARE CALCULATION (data only — never used to bypass Groq) ──────────────────

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
    return { source:"tapiwa_market_price_rules", estimated_low_mwk:Math.min(low,high), estimated_high_mwk:Math.max(low,high), recommended_mwk:recommended, confidence:tapiwaRule.confidence||"medium", route_used:`${tapiwaRule.pickup_landmark||"Unknown"} → ${tapiwaRule.dropoff_landmark||"Unknown"}` };
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
  return { source:"route_matrix_plus_pricing_rules", distance_km:distance, vehicle_type:rule.Vehicle_Type, base_rate_per_km:rate, minimum_fare:minimum, estimated_low_mwk:Math.min(low,high), estimated_high_mwk:Math.max(low,high), recommended_mwk:cleanPrice((low+high)/2), route_used:`${route.From_Landmark} → ${route.To_Landmark}` };
}

// ── AUDIT LOG ──────────────────────────────────────────────────────────────────

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

// ── MAIN ENDPOINT ──────────────────────────────────────────────────────────────

app.post("/ai/analyze", async (req, res) => {
  let cleanMessage = "";
  let context = null;
  let aiResult = {};
  let mauriceData = null;

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
      return res.json({
        ignored: false,
        category: "general_update",
        risk_level: "low",
        internal_summary: "Local joke response",
        team_message: joke,
        requires_supervisor_approval: false,
        used_data: {},
        debug_memory: { sessionId, recentResponses: memory.recentResponses, recentJokes: memory.recentJokes }
      });
    }

    // ── Resolve context — always fetch so Groq has real data ──────────────────
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

    const useMaurice = shouldUseMaurice(cleanMessage) || Boolean(memory.pendingConfirmation && isAffirmationMessage(cleanMessage));

    if (useMaurice) {
      try {
        mauriceData = await callMaurice(cleanMessage, {
          sender: `${senderName} (${senderRole})`,
          memory: {
            lastRoute: memory.lastRoute,
            pendingConfirmation: memory.pendingConfirmation,
            confirmedRoute: memory.confirmedRoute
          },
          route_understanding: routeUnderstanding,
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

    // ── Lift route intel into route understanding if matched ──────────────────
    const topRouteIntel = context.tapiwa_intelligence?.route_intelligence?.[0] || null;
    if (topRouteIntel && routeUnderstanding?.hasRoute && routeIntelMatchesRawRoute(topRouteIntel, routeUnderstanding.pickupRaw, routeUnderstanding.dropoffRaw)) {
      routeUnderstanding.pickup = { Landmark_Name: topRouteIntel.pickup_landmark };
      routeUnderstanding.dropoff = { Landmark_Name: topRouteIntel.dropoff_landmark };
      routeUnderstanding.confidence = "high";
      routeUnderstanding.note = "Route resolved from Tapiwa route intelligence.";
    }

    // ── Update conversation memory ────────────────────────────────────────────
    const detectedRoute = routeSnapshotFromUnderstanding(routeUnderstanding);
    if (detectedRoute) {
      memory.lastRoute = { ...detectedRoute };
      if (isCorrection) {
        memory.confirmedRoute = detectedRoute.confidence==="high" ? { ...detectedRoute } : null;
        memory.pendingConfirmation = detectedRoute.confidence==="medium" ? { ...detectedRoute } : null;
      } else if (detectedRoute.confidence==="high") {
        memory.confirmedRoute = { ...detectedRoute };
        memory.pendingConfirmation = null;
      } else if (detectedRoute.confidence==="medium") {
        memory.confirmedRoute = null;
        memory.pendingConfirmation = { ...detectedRoute };
      } else {
        memory.confirmedRoute = null;
        memory.pendingConfirmation = null;
      }
    }
    saveConversationMemory(sessionId, memory);

    // ── Calculate fare — passed to Groq as data, not as a bypass ─────────────
    const computedFare = calculateBasicFare(context);

    // ── Build Tapiwa Gemini prompt ────────────────────────────────────────────
    const detectedIntent = detectTapiwaIntent(cleanMessage);
    const operationalMode = isOperationalIntent(detectedIntent);
    const highRiskMode = isHighRiskIntent(detectedIntent, cleanMessage);
    const selectedVibe = pickTapiwaVibe(detectedIntent, cleanMessage);
    const currentDateTime = getBlantyreDateTime();

    const systemPrompt = `
You are Tapiwa, a real Malawian dispatcher and the human voice of Zachangu Commuters.

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
  - "pang’ono"
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
- Match the user’s energy.
- If they are brief, respond briefly.
- If they are playful, be playful.
- If they are stressed, calm them down.
- If they greet you warmly, greet them warmly back.
- Do not repeat the same greeting or sentence structure often.
- Avoid robotic phrases completely.
- Never say:
  - "As an AI"
  - "Request received"
  - "Processing your request"
  - "I have updated the system"
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
  - "Shaaaah, this one is a small town run 😄"
  - "Easy trip that one, no need to wake the whole village."
  - "System ikuchita slow pang’ono, koma hold on."
  - "That route looks straightforward, tikuyenda."
  - "Morning! Ready to disturb the roads again? 😄"

ERROR HANDLING STYLE:
- If the system/API/backend has trouble, never repeat the same boring error.
- Rotate error wording naturally.
- Acceptable examples:
  - "Shaaaah 😅 system ikuvuta pang’ono—hold on, zitekha pompano."
  - "Hmm… looks like the system is dragging a bit. Give me a sec."
  - "System’s being stubborn today, koma tili pompo."
  - "Small technical drama here 😄 try again in a moment."
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
- If price data is missing, say naturally: "I don’t have enough system data to price that trip yet."
- If route confidence is medium, ask one short natural confirmation question.
- If route confidence is low, ask for clearer pickup and dropoff.
- Use MWK format for money, for example: "MWK 11,000".

RESPONSE LENGTH:
- For operational requests: usually 1–2 sentences, but allow personality.
- For general chat: 1–4 sentences allowed if natural.
- Do not be dry.
- Do not write essays unless the user asks.

OUTPUT FORMAT:
Return valid JSON only.
No markdown.
No extra text outside JSON.

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
      current_datetime: currentDateTime,
      detected_intent: detectedIntent,
      operational_mode: operationalMode,
      high_risk_mode: highRiskMode,
      selected_vibe: selectedVibe,
      message: cleanMessage,
      memory: {
        lastRoute: memory.lastRoute,
        pendingConfirmation: memory.pendingConfirmation,
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
      zones: context.zones,
      risks: context.risks
    };

    // ── Call Gemini for Tapiwa (no Groq fallback) ─────────────────────────────
    let aiRawText = "";
    try {
      aiRawText = await callTapiwaGemini({
        systemPrompt,
        userPayload,
        operationalMode,
        highRisk: highRiskMode
      });

      aiResult = JSON.parse(extractJsonObject(aiRawText));

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
      console.error("TAPIWA GEMINI ERROR:", geminiError.message);
      const humanError = pickHumanErrorMessage();

      await saveAuditLog({
        user_message: message,
        clean_message: cleanMessage,
        success: false,
        error_message: geminiError.message,
        raw_ai_response: aiRawText || null
      });

      return res.json({
        ignored: false,
        category: "system_issue",
        risk_level: "low",
        internal_summary: "Tapiwa Gemini error",
        team_message: humanError,
        requires_supervisor_approval: false,
        used_data: {}
      });
    }

    // ── Sanitise Groq output ──────────────────────────────────────────────────
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

    const teamMessage = aiResult.team_message || pickHumanErrorMessage();

    rememberTapiwaResponse(memory, teamMessage, aiResult.opening_used || "");
    memory.lastUserMessage = cleanMessage;
    saveConversationMemory(sessionId, memory);

    const responsePayload = {
      ignored: false,
      category,
      risk_level: riskLevel,
      internal_summary: aiResult.internal_summary || cleanMessage,
      team_message: teamMessage,
      requires_supervisor_approval: riskLevel === "high" || aiResult.requires_supervisor_approval === true,
      used_data: {
        computed_fare: computedFare,
        route_understanding: { confidence: routeUnderstanding.confidence, pickup: routeUnderstanding.pickup?.Landmark_Name, dropoff: routeUnderstanding.dropoff?.Landmark_Name },
        maurice: mauriceData
      },
      debug_memory: { sessionId, lastRoute:memory.lastRoute, pendingConfirmation:memory.pendingConfirmation, confirmedRoute:memory.confirmedRoute, recentResponses: memory.recentResponses, userProfile: memory.userProfile },
      debug_data_counts: context.data_counts,
      debug_matched_counts: context.matched_counts,
      routed_to_maurice: useMaurice,
      maurice: mauriceData
    };

    await saveAuditLog({
      request_type: "team_chat",
      user_message: message,
      clean_message: cleanMessage,
      ai_category: category,
      risk_level: riskLevel,
      team_message: teamMessage,
      internal_summary: aiResult.internal_summary || cleanMessage,
      used_data: responsePayload.used_data,
      raw_ai_response: aiResult,
      success: true
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
      used_data: {}
    });
  }
});

const port = process.env.PORT || 3001;
app.listen(port, "0.0.0.0", () => {
  console.log(`Zachangu AI server running on port ${port}`);
});
