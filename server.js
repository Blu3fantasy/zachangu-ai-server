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
const TAPIWA_GROQ_API_KEY = process.env.TAPIWA_GROQ_API_KEY || process.env.GROQ_API_KEY;
const GROQ_MODEL = process.env.GROQ_MODEL || "llama-3.1-8b-instant";
const TAPIWA_MODEL = process.env.TAPIWA_MODEL || process.env.GROQ_MODEL || "llama-3.1-8b-instant";
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
    groq_ready: Boolean(GROQ_API_KEY),
    tapiwa_groq_ready: Boolean(TAPIWA_GROQ_API_KEY),
    supabase_ready: Boolean(SUPABASE_URL && SUPABASE_API_KEY),
    supabase_url_loaded: SUPABASE_URL || null,
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
        pendingConfirmation: Boolean(memory.pendingConfirmation),
        confirmedRoute: memory.confirmedRoute || null,
        lastAssistTopic: memory.lastAssistTopic || null,
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
      pendingConfirmation: false,
      confirmedRoute: null,
      lastAssistTopic: null,
      updatedAt: Date.now()
    });
  }
  return conversationMemory.get(sessionId);
}

function saveConversationMemory(sessionId, memory) {
  conversationMemory.set(sessionId, {
    lastRoute: memory.lastRoute || null,
    pendingConfirmation: Boolean(memory.pendingConfirmation),
    confirmedRoute: memory.confirmedRoute || null,
    lastAssistTopic: memory.lastAssistTopic || null,
    updatedAt: Date.now()
  });
  saveConversationMemoryToDisk();
}

loadConversationMemoryFromDisk();
setInterval(saveConversationMemoryToDisk, 1000 * 60 * 5).unref?.();

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

function takeItems(items, limit) {
  return Array.isArray(items) ? items.slice(0, limit) : [];
}

function buildCompactAssistantContext(context, memory, mauriceData) {
  const routeUnderstanding = context?.route_understanding || {};
  const compact = {
    route_understanding: {
      hasRoute: !!routeUnderstanding.hasRoute,
      confidence: routeUnderstanding.confidence || "low",
      pickup: routeUnderstanding.pickup?.Landmark_Name || routeUnderstanding.pickupRaw || null,
      dropoff: routeUnderstanding.dropoff?.Landmark_Name || routeUnderstanding.dropoffRaw || null,
      note: routeUnderstanding.note || null
    },
    zones: takeItems(context?.zones, 3),
    landmarks: takeItems(context?.landmarks, 4),
    pricing_rules: takeItems(context?.pricing_rules, 2),
    market_prices: takeItems(context?.market_prices, 3),
    route_matrix: takeItems(context?.route_matrix, 3),
    risks: takeItems(context?.risks, 2),
    tapiwa_intelligence: {
      market_price_rules: takeItems(context?.tapiwa_intelligence?.market_price_rules, 2),
      route_intelligence: takeItems(context?.tapiwa_intelligence?.route_intelligence, 2),
      zone_behavior: takeItems(context?.tapiwa_intelligence?.zone_behavior, 2),
      route_learning: takeItems(context?.tapiwa_intelligence?.route_learning, 2)
    },
    data_counts: context?.data_counts || {},
    matched_counts: context?.matched_counts || {},
    memory: {
      lastRoute: memory?.lastRoute || null,
      pendingConfirmation: !!memory?.pendingConfirmation,
      confirmedRoute: memory?.confirmedRoute || null
    }
  };

  if (mauriceData) compact.maurice = mauriceData;
  return compact;
}

function formatMwk(value) {
  const amount = Number(value || 0);
  if (!amount) return null;
  return `MWK ${amount.toLocaleString("en-US")}`;
}

function buildDispatchNote({ computedFare, mauriceData, routeUnderstanding }) {
  if (computedFare?.distance_km) {
    return `Note: about ${computedFare.distance_km} km, so keep bike pricing only.`;
  }
  if (mauriceData?.missing_data?.includes("vehicle_type")) {
    return "Note: confirm vehicle type before you lock the fare.";
  }
  if (routeUnderstanding?.confidence === "medium") {
    return "Note: confirm the exact pickup before you quote finally.";
  }
  return "Note: confirm with the driver if traffic or weather changes the run.";
}

function buildPriceReply({ amountText, noteText, rangeText = "" }) {
  const head = rangeText || amountText;
  return noteText ? `${head}\n${noteText}` : head;
}

function buildLocalTapiwaFallback({ cleanMessage, computedFare, routeUnderstanding, mauriceData, memory }) {
  const text = normalizeText(cleanMessage);
  const greeting = /\b(hello|hi|hey|morning|goodmorning|good morning|afternoon|good afternoon|evening|good evening)\b/.test(text);

  if (greeting && !isPricingLikeMessage(cleanMessage) && !isExplicitRouteMessage(cleanMessage)) {
    return {
      ignored: false,
      category: "general_update",
      risk_level: "low",
      internal_summary: "Local greeting fallback",
      team_message: "Morning all! 🌞",
      requires_supervisor_approval: false
    };
  }

  if (computedFare?.recommended_mwk) {
    const amountText = formatMwk(computedFare.recommended_mwk);
    return {
      ignored: false,
      category: "pricing_issue",
      risk_level: "low",
      internal_summary: `Computed fare: ${amountText}`,
      team_message: buildPriceReply({
        amountText,
        noteText: buildDispatchNote({ computedFare, mauriceData, routeUnderstanding })
      }),
      requires_supervisor_approval: false
    };
  }

  if (mauriceData?.market_price_min || mauriceData?.market_price_max) {
    const min = Number(mauriceData.market_price_min || 0);
    const max = Number(mauriceData.market_price_max || 0);
    const estimate = max ? cleanPrice((min + max) / 2) : cleanPrice(min);
    const hasClearRoute = routeUnderstanding?.confidence === "high" || Number(mauriceData.confidence || 0) >= 0.75;
    const rangeText = min && max ? `${formatMwk(min)} - ${formatMwk(max)}` : formatMwk(estimate);
    return {
      ignored: false,
      category: "pricing_issue",
      risk_level: hasClearRoute ? "low" : "medium",
      internal_summary: "Local market-price fallback",
      team_message: hasClearRoute
        ? buildPriceReply({
            amountText: formatMwk(estimate),
            rangeText,
            noteText: buildDispatchNote({ computedFare, mauriceData, routeUnderstanding })
          })
        : buildPriceReply({
            amountText: formatMwk(estimate),
            noteText: "Note: confirm the exact route before you quote finally."
          }),
      requires_supervisor_approval: false
    };
  }

  if (routeUnderstanding?.confidence === "medium" || memory?.pendingConfirmation) {
    const pickup = mauriceData?.pickup_landmark || routeUnderstanding?.pickup?.Landmark_Name || routeUnderstanding?.pickupRaw || memory?.lastRoute?.pickupRaw || "the pickup";
    const dropoff = mauriceData?.dropoff_landmark || routeUnderstanding?.dropoff?.Landmark_Name || routeUnderstanding?.dropoffRaw || memory?.lastRoute?.dropoffRaw || "the dropoff";
    return {
      ignored: false,
      category: "pricing_issue",
      risk_level: "medium",
      internal_summary: "Route needs confirmation",
      team_message: `Confirm for me: ${pickup} to ${dropoff}?`,
      requires_supervisor_approval: false
    };
  }

  return {
    ignored: false,
    category: "system_issue",
    risk_level: "low",
    internal_summary: "Local fallback",
    team_message: "I need a clearer pickup and dropoff before I quote that.",
    requires_supervisor_approval: false
  };
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
