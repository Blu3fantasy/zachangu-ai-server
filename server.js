import express from "express";
import cors from "cors";
import dotenv from "dotenv";

dotenv.config();

const app = express();
app.use(cors());
app.use(express.json({ limit: "1mb" }));

const GROQ_API_KEY = process.env.GROQ_API_KEY;
const GROQ_MODEL = process.env.GROQ_MODEL || "llama-3.1-8b-instant";
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY || process.env.SUPABASE_ANON || "";
const SUPABASE_API_KEY = SUPABASE_SERVICE_ROLE_KEY || SUPABASE_ANON_KEY;

const tableDebug = {};
const conversationMemory = new Map();

// --- FIX 1: Small Talk Detection Helper ---
function isSmallTalk(message) {
  const text = normalizeText(message);
  return /^(hi|hello|hey|how are you|how far|muli bwanji|moni|morning|afternoon|evening|tapiwa)$/i.test(text) || 
         (text.length < 15 && /^(how are you|good morning|good afternoon|boss|hello)/.test(text));
}

app.get("/", (req, res) => {
  res.json({
    status: "Zachangu AI server is running",
    groq_ready: Boolean(GROQ_API_KEY),
    supabase_ready: Boolean(SUPABASE_URL && SUPABASE_API_KEY),
    supabase_url_loaded: SUPABASE_URL || null,
    tapiwa_intelligence_ready: true
  });
});

app.get("/debug-tables", async (req, res) => {
  const tables = [
    "zones", "landmarks", "pricing_rules", "market_prices", "route_matrix", 
    "risks", "drivers", "trip_requests", "tapiwa_system_settings", 
    "tapiwa_market_price_rules", "tapiwa_route_intelligence", "tapiwa_zone_behavior", 
    "tapiwa_route_learning", "tapiwa_price_outcomes", "tapiwa_ai_audit_logs"
  ];
  const result = {};
  for (const table of tables) {
    const data = await supabaseFetch(table, 3);
    result[table] = {
      rows_loaded: data.length,
      last_status: tableDebug[table] || null,
      sample: data.slice(0, 1)
    };
  }
  res.json(result);
});

function hasTapiwaCall(message) {
  return /@tapiwa/i.test(String(message || ""));
}

function cleanTapiwaMessage(message) {
  return String(message || "").replace(/@tapiwa/gi, "").trim();
}

function cleanBaseUrl() {
  return String(SUPABASE_URL || "").replace(/\/rest\/v1\/?$/, "").replace(/\/$/, "");
}

function roundToNearest100(value) {
  return Math.round(Number(value || 0) / 100) * 100;
}

function cleanPrice(value) {
  return Math.max(3000, roundToNearest100(value));
}

function buildTemporaryTapiwaFallback() {
  return {
    ignored: false,
    category: "system_issue",
    risk_level: "low",
    internal_summary: "Temporary system issue",
    team_message: "Hmm, something went off on my side — try that again.",
    requires_supervisor_approval: false,
    used_data: {}
  };
}

function buildSessionId({ sessionId, senderName, senderRole }) {
  return String(sessionId || `${senderRole || "Dispatcher"}:${senderName || "unknown"}`).trim();
}

function getConversationMemory(sessionId) {
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
}

function normalizeText(value) {
  return String(value || "").toLowerCase().replace(/[^\w\s]/g, " ").replace(/\s+/g, " ").trim();
}

function isAffirmationMessage(message) {
  const text = normalizeText(message);
  return /^(yes|ya|yeah|yep|correct|confirmed|that one|that route|ok|okay|alright|right|exactly|true)$/i.test(text);
}

// --- FIX 2: Relaxed Sanitization (Don't delete keywords Tapiwa needs to see) ---
function sanitizeRouteFragment(value) {
  return String(value || "")
    .replace(/@tapiwa/gi, " ")
    .replace(/\btapiwa\b/gi, " ")
    .replace(/\b(no|not that|wrong route|correction|i meant|meant)\b/gi, " ")
    .replace(/\?/g, " ")
    // Removed price/km/route from deletion list so matching doesn't fail
    .replace(/\s+/g, " ")
    .trim();
}

function isFollowUpRouteMessage(message) {
  const text = normalizeText(message);
  return /how much for that trip|how much for that route|how much for that|price for that|quote that|distance for that|km for that/.test(text);
}

function extractRouteParts(message) {
  const text = normalizeText(message);
  let match = text.match(/from (.+?) to (.+)/);
  if (match) {
    return {
      pickupRaw: sanitizeRouteFragment(match[1]),
      dropoffRaw: sanitizeRouteFragment(match[2])
    };
  }
  match = text.match(/(.+?) to (.+)/);
  if (match && (isPricingLikeMessage(text) || isCorrectionMessage(text))) {
    return {
      pickupRaw: sanitizeRouteFragment(match[1]),
      dropoffRaw: sanitizeRouteFragment(match[2])
    };
  }
  return null;
}

function isExplicitRouteMessage(message) {
  return Boolean(extractRouteParts(message));
}

function routeSnapshotFromUnderstanding(routeUnderstanding) {
  if (!routeUnderstanding || !routeUnderstanding.hasRoute || !routeUnderstanding.pickupRaw || !routeUnderstanding.dropoffRaw) {
    return null;
  }
  return {
    pickup: routeUnderstanding.pickup?.Landmark_Name || null,
    dropoff: routeUnderstanding.dropoff?.Landmark_Name || null,
    pickupRaw: routeUnderstanding.pickupRaw,
    dropoffRaw: routeUnderstanding.dropoffRaw,
    confidence: routeUnderstanding.confidence
  };
}

function routeToLookupMessage(route) {
  if (!route) return "";
  const pickup = route.pickupRaw || route.pickup || "";
  const dropoff = route.dropoffRaw || route.dropoff || "";
  return `from ${pickup} to ${dropoff}`.trim();
}

async function fetchWithTimeout(url, options = {}, timeoutMs = 12000) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  try {
    return await fetch(url, { ...options, signal: controller.signal });
  } finally {
    clearTimeout(timeout);
  }
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
    if (!response.ok) {
      tableDebug[table] = { ok: false, status: response.status, error: text };
      return [];
    }
    const json = JSON.parse(text);
    tableDebug[table] = { ok: true, status: response.status, rows_loaded: Array.isArray(json) ? json.length : 0 };
    return Array.isArray(json) ? json : [];
  } catch (error) {
    tableDebug[table] = { ok: false, error: error.message };
    return [];
  }
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
    if (!response.ok) {
      tableDebug[table] = { ok: false, status: response.status, insert_error: text };
      return null;
    }
    return text ? JSON.parse(text) : null;
  } catch (error) {
    tableDebug[table] = { ok: false, insert_error: error.message };
    return null;
  }
}

function levenshtein(a, b) {
  a = normalizeText(a); b = normalizeText(b);
  const matrix = Array.from({ length: a.length + 1 }, () => Array(b.length + 1).fill(0));
  for (let i = 0; i <= a.length; i++) matrix[i][0] = i;
  for (let j = 0; j <= b.length; j++) matrix[0][j] = j;
  for (let i = 1; i <= a.length; i++) {
    for (let j = 1; j <= b.length; j++) {
      const cost = a[i - 1] === b[j - 1] ? 0 : 1;
      matrix[i][j] = Math.min(matrix[i - 1][j] + 1, matrix[i][j - 1] + 1, matrix[i - 1][j - 1] + cost);
    }
  }
  return matrix[a.length][b.length];
}

function similarity(a, b) {
  a = normalizeText(a); b = normalizeText(b);
  if (!a || !b) return 0;
  if (a === b) return 1;
  if (a.includes(b) || b.includes(a)) return 0.88;
  const distance = levenshtein(a, b);
  const maxLength = Math.max(a.length, b.length);
  return 1 - distance / maxLength;
}

function isPricingLikeMessage(message) {
  const text = normalizeText(message);
  return /price|fare|cost|charge|quote|how much|hw much|hw mch|how mch|hwmuch|km|kms|kilometer|kilometre|distance/.test(text) || /\bfrom\b.+\bto\b/.test(text);
}

function isCorrectionMessage(message) {
  const text = normalizeText(message);
  return /no tapiwa|not that|i meant|meant|wrong|correction|not from|not to/.test(text);
}

function isHelpGuidanceMessage(message) {
  const text = normalizeText(message);
  return /start guiding me|guide me|create a trip|how can i create a trip|how do i create a trip|is this working|where do i click|how do i|view driver/.test(text) || (/see driver/.test(text) && !/send driver/.test(text));
}

function isOffTopicMessage(message) {
  const text = normalizeText(message);
  return /weather|temperature|sunny|rain today/.test(text);
}

// --- FIX 3: Less Greedier Dispatch Intent (Avoid greeting collisions) ---
function hasDispatchIntent(message) {
  const text = normalizeText(message);
  // Must contain a trigger word AND a direction/subject to be counted as dispatch
  return /send driver|send car|dispatch|driver sent|need driver|need car|car come|snd drver|snd driver|send drver|tumizani driver|tumizani galimoto/.test(text) || 
         ((/drver|driver|car/.test(text)) && (/send|snd|where is|cust|customer|wants to go|pickup|dispatch/.test(text)));
}

function hasUrgencyTone(message) {
  const text = normalizeText(message);
  return /asap|hurry|urgent|now|pls pls|waiting long|waitng long/.test(text);
}

function hasUncertaintyMarkers(message) {
  const text = normalizeText(message);
  return /i think|maybe|not sure|somewhere|pafupi|near|maybe it s|if possible/.test(text);
}

function compactLocation(value) {
  return String(value || "").replace(/\s+/g, " ").trim();
}

function extractDispatchHints(message) {
  const raw = String(message || "");
  const toMatch = raw.match(/\bto\s+([A-Za-z0-9\s'\/-]+?)(?:\s+(?:the customer|customer|near|pafupi|maybe|i think|and|but|he wants|she wants|asap|pls|please|$))/i) || raw.match(/\b2\s+([A-Za-z0-9\s'\/-]+?)(?:\s+(?:asap|cust|customer|$))/i) || raw.match(/\bku\s+([A-Za-z0-9\s'\/-]+?)(?:,|\s+customer|\s+ali|\s+pafupi|\s+pa\s+stage|$)/i);
  const pickupMatch = raw.match(/\bat\s+([A-Za-z0-9\s'\/-]+?)(?:\s+(?:and|near|customer|he wants|she wants|$))/i) || raw.match(/\bpa\s+([A-Za-z0-9\s'\/-]+?)(?:\s+(?:pafupi|near|customer|$))/i);
  const nearMatch = raw.match(/\bnear\s+([A-Za-z0-9\s'\/-]+?)(?:\s+(?:i think|maybe|$))/i) || raw.match(/\bpafupi\s+ndi\s+([A-Za-z0-9\s'\/-]+?)(?:\s|$)/i);
  const wantsToMatch = raw.match(/\bwants?\s+to\s+go\s+to\s+([A-Za-z0-9\s'\/-]+?)(?:\s+(?:but|and|$))/i);
  return { destination: compactLocation(toMatch?.[1] || ""), pickup: compactLocation(pickupMatch?.[1] || ""), nearby: compactLocation(nearMatch?.[1] || ""), dropoff: compactLocation(wantsToMatch?.[1] || "") };
}

function buildDeterministicDispatchReply(cleanMessage, memory) {
  const text = normalizeText(cleanMessage);
  if (text === "start guiding me") {
    if (memory) memory.lastAssistTopic = "general_onboarding";
    return { category: "general_update", risk_level: "low", internal_summary: "Started dispatcher onboarding guidance", team_message: "I’m ready to guide you. You can ask about creating a trip, finding drivers, zones, or admin tools.", requires_supervisor_approval: false, used_data: { assist_topic: "general_onboarding" } };
  }
  if (/how can i create a trip|how do i create a trip|create a trip/.test(text)) {
    if (memory) memory.lastAssistTopic = "trip_creation";
    return { category: "general_update", risk_level: "low", internal_summary: "Explained trip creation flow", team_message: "To create a trip, open the Trips tab, tap the plus button, enter the pickup and drop-off, then choose the driver and confirm the trip.", requires_supervisor_approval: false, used_data: { assist_topic: "trip_creation" } };
  }
  if (isHelpGuidanceMessage(cleanMessage)) {
    if (memory) memory.lastAssistTopic = "drivers_tab";
    return { category: "general_update", risk_level: "low", internal_summary: "Guided dispatcher to driver screen", team_message: "Yes, the system is working. Tap the Drivers tab to view available drivers, and I can guide you step by step if you want.", requires_supervisor_approval: false, used_data: { assist_topic: "drivers_tab" } };
  }
  if (isOffTopicMessage(cleanMessage)) {
    return { category: "general_update", risk_level: "low", internal_summary: "Redirected off-topic request to dispatch scope", team_message: "I’m here for dispatch operations. If you need a driver, send the pickup location and I’ll help from there.", requires_supervisor_approval: false, used_data: {} };
  }
  if (!hasDispatchIntent(cleanMessage)) return null;
  const hints = extractDispatchHints(cleanMessage);
  if (/send car$|^send car$/.test(text) || (!hints.destination && !hints.pickup && /send car/.test(text))) {
    return { category: "driver_issue", risk_level: "low", internal_summary: "Dispatch requested without pickup location", team_message: "Sure — please provide the pickup location so I can dispatch the car.", requires_supervisor_approval: false, used_data: { dispatch_hints: hints } };
  }
  if (/town/.test(text) && hasUncertaintyMarkers(cleanMessage)) {
    return { category: "driver_issue", risk_level: "low", internal_summary: "Dispatch request too vague for safe action", team_message: "I can help send a driver. Please tell me the exact pickup location in town.", requires_supervisor_approval: false, used_data: { dispatch_hints: hints } };
  }
  if (hints.destination || hints.pickup) {
    const target = hints.destination || hints.pickup;
    return { category: "driver_issue", risk_level: "low", internal_summary: "Dispatch request missing final confirmation", team_message: hasUncertaintyMarkers(cleanMessage) ? `I can help with ${target}. Please confirm the exact pickup point before I dispatch.` : `Understood. Please confirm the pickup point is ${target} so I can dispatch the driver.`, requires_supervisor_approval: false, used_data: { dispatch_hints: hints } };
  }
  return { category: "driver_issue", risk_level: "low", internal_summary: "Dispatch request missing pickup location", team_message: "Sure — please provide the pickup location so I can dispatch a driver safely.", requires_supervisor_approval: false, used_data: { dispatch_hints: hints } };
}

function safeJsonParse(value) {
  if (!value || typeof value !== "string") return null;
  try { return JSON.parse(value); } catch { return null; }
}

function uniqueTokens(value) {
  return [...new Set(normalizeText(value).split(" ").filter(Boolean))];
}

function routeMeaningfulTokens(value) {
  const ignore = new Set(["from", "to", "please", "trip", "route", "how", "much", "price", "fare", "cost", "quote", "the"]);
  return uniqueTokens(value).filter((token) => !ignore.has(token));
}

function qualifierTokens(value) {
  return routeMeaningfulTokens(value).filter((token) => token !== "area" && !/^\d+$/.test(token));
}

function tokenOverlapRatio(sourceTokens, candidateTokens) {
  if (!sourceTokens.length || !candidateTokens.length) return 0;
  let overlap = 0;
  for (const token of sourceTokens) { if (candidateTokens.includes(token)) overlap += 1; }
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
  if (qualifierOverlap > 0) { score += fieldType === "name" ? 0.22 : fieldType === "alias" ? 0.18 : 0.1; }
  if (fieldType === "area" && rawQualifierTokens.length) { score -= 0.35; }
  if ((fieldType === "name" || fieldType === "alias") && qualifierOverlap === 0 && rawQualifierTokens.length) { score -= 0.12; }
  if (fieldType === "nearby") { score -= 0.04; }
  return Math.max(0, Math.min(1, score));
}

function bestLandmarkMatch(rawName, landmarks) {
  if (!rawName || !Array.isArray(landmarks)) { return { match: null, score: 0, secondScore: 0, source: null }; }
  const sourcePriority = { name: 4, alias: 3, nearby: 2, area: 1 };
  const candidates = landmarks.map((lm) => {
    const noteMeta = safeJsonParse(lm.Notes);
    const aliasCandidates = [];
    if (typeof noteMeta?.also_known_as === "string" && noteMeta.also_known_as.trim()) { aliasCandidates.push(noteMeta.also_known_as.trim()); }
    const fields = [{ type: "name", value: lm.Landmark_Name }, ...aliasCandidates.map((value) => ({ type: "alias", value })), ...(String(lm.Nearby_Landmarks || "").split(";").map((value) => value.trim()).filter(Boolean).map((value) => ({ type: "nearby", value }))), { type: "area", value: lm.Area }];
    let bestFieldScore = 0; let bestFieldType = null; let bestFieldValue = null;
    for (const field of fields) {
      const score = scoreLandmarkField(rawName, field.value, field.type);
      if (score > bestFieldScore) { bestFieldScore = score; bestFieldType = field.type; bestFieldValue = field.value; }
    }
    return { landmark: lm, score: bestFieldScore, source: bestFieldType, sourceValue: bestFieldValue, priority: sourcePriority[bestFieldType] || 0 };
  });
  candidates.sort((a, b) => { if (b.score !== a.score) return b.score - a.score; return (b.priority || 0) - (a.priority || 0); });
  return { match: (candidates[0]?.score || 0) >= 0.55 ? candidates[0]?.landmark || null : null, score: candidates[0]?.score || 0, secondScore: (candidates[1]?.score || 0) + ((candidates[1]?.priority || 0) * 0.001), source: candidates[0]?.source || null, sourceValue: candidates[0]?.sourceValue || null };
}

function samePlaceName(a, b) { const left = normalizeText(a); const right = normalizeText(b); return Boolean(left && right && left === right); }
function routeMatchesResolvedLandmarks(route, pickupName, dropoffName) { if (!route || !pickupName || !dropoffName) return false; return samePlaceName(route.From_Landmark, pickupName) && samePlaceName(route.To_Landmark, dropoffName); }
function marketPriceMatchesResolvedLandmarks(row, pickupName, dropoffName) { if (!row || !pickupName || !dropoffName) return false; return samePlaceName(row.Origin_Landmark, pickupName) && samePlaceName(row.Destination_Landmark, dropoffName); }
function tapiwaRuleMatchesResolvedLandmarks(row, pickupName, dropoffName) { if (!row || !pickupName || !dropoffName) return false; return samePlaceName(row.pickup_landmark, pickupName) && samePlaceName(row.dropoff_landmark, dropoffName); }
function routeIntelMatchesResolvedLandmarks(row, pickupName, dropoffName) { if (!row || !pickupName || !dropoffName) return false; return samePlaceName(row.pickup_landmark, pickupName) && samePlaceName(row.dropoff_landmark, dropoffName); }
function routeIntelMatchesRawRoute(row, pickupRaw, dropoffRaw) { if (!row || !pickupRaw || !dropoffRaw) return false; return similarity(row.pickup_landmark, pickupRaw) >= 0.72 && similarity(row.dropoff_landmark, dropoffRaw) >= 0.72; }

function buildRouteUnderstanding(message, landmarksRaw) {
  const parts = extractRouteParts(message);
  if (!parts) { return { hasRoute: false, confidence: "low", pickup: null, dropoff: null, note: "No clear pickup/dropoff detected." }; }
  const pickupMatch = bestLandmarkMatch(parts.pickupRaw, landmarksRaw);
  const dropoffMatch = bestLandmarkMatch(parts.dropoffRaw, landmarksRaw);
  const avgScore = (pickupMatch.score + dropoffMatch.score) / 2;
  const pickupExactNamed = (pickupMatch.source === "name" || pickupMatch.source === "alias") && pickupMatch.score >= 0.95;
  const dropoffExactNamed = (dropoffMatch.source === "name" || dropoffMatch.source === "alias") && dropoffMatch.score >= 0.95;
  const pickupAmbiguous = pickupMatch.score >= 0.55 && pickupMatch.score - pickupMatch.secondScore < 0.08 && !pickupExactNamed;
  const dropoffAmbiguous = dropoffMatch.score >= 0.55 && dropoffMatch.score - dropoffMatch.secondScore < 0.08 && !dropoffExactNamed;
  let confidence = "low";
  if (avgScore >= 0.78 && !pickupAmbiguous && !dropoffAmbiguous) { confidence = "high"; } else if (avgScore >= 0.55) { confidence = "medium"; }
  return { hasRoute: true, confidence, pickupRaw: parts.pickupRaw, dropoffRaw: parts.dropoffRaw, pickup: pickupMatch.match, dropoff: dropoffMatch.match, pickup_score: pickupMatch.score, dropoff_score: dropoffMatch.score, pickup_source: pickupMatch.source, dropoff_source: dropoffMatch.source, note: confidence === "high" ? "Route understood clearly." : confidence === "medium" ? "Route partly understood; needs confirmation." : "Route unclear; ask for clarification." };
}

function getKeywords(message) {
  const stopWords = new Set(["price", "fare", "cost", "from", "to", "the", "for", "please", "give", "estimate", "route", "how", "much", "should", "we", "charge", "what", "is", "that", "trip"]);
  return normalizeText(message).split(" ").filter((word) => word.length > 2 && !stopWords.has(word));
}

function rowText(row) { return normalizeText(Object.values(row || {}).filter(Boolean).join(" ")); }
function scoreRow(row, keywords) { const text = rowText(row); let score = 0; for (const word of keywords) { if (text.includes(word)) score += 1; } return score; }
function topMatches(rows, keywords, limit = 5) { return rows.map((row) => ({ row, score: scoreRow(row, keywords) })).filter((item) => item.score > 0).sort((a, b) => b.score - a.score).slice(0, limit).map((item) => item.row); }

function slimLandmark(row) { return { Landmark_ID: row.Landmark_ID, Landmark_Name: row.Landmark_Name, Area: row.Area, Zone_ID: row.Zone_ID, Nearby_Landmarks: row.Nearby_Landmarks }; }
function slimZone(row) { return { Zone_ID: row.Zone_ID, Zone_Name: row.Zone_Name, Areas_Covered: row.Areas_Covered, Zone_Type: row.Zone_Type, Demand_Level: row.Demand_Level, Strategic_Role: row.Strategic_Role }; }
function slimPricingRule(row) { return { Vehicle_Type: row.Vehicle_Type, Base_Rate_Per_KM_MWK: row.Base_Rate_Per_KM_MWK, Minimum_Fare_MWK: row.Minimum_Fare_MWK, Peak_Multiplier: row.Peak_Multiplier, Active: row.Active }; }
function slimMarketPrice(row) { return { Route_ID: row.Route_ID, Origin_Landmark: row.Origin_Landmark, Destination_Landmark: row.Destination_Landmark, Min_Price: row.Min_Price, Max_Price: row.Max_Price, Avg_Price: row.Avg_Price }; }
function slimRoute(row) { return { Route_Key: row.Route_Key, From_Landmark: row.From_Landmark, To_Landmark: row.To_Landmark, Distance_KM: row.Distance_KM, Zone_From: row.Zone_From, Zone_To: row.Zone_To }; }
function slimRisk(row) { return { Zone_ID: row.Zone_ID, Rank_Name: row.Rank_Name, Problem_Description: row.Problem_Description, Risk_Points: row.Risk_Points }; }
function slimTapiwaMarketRule(row) { return { pickup_landmark: row.pickup_landmark, dropoff_landmark: row.dropoff_landmark, min_price: row.min_price, recommended_price: row.recommended_price, max_price: row.max_price, confidence: row.confidence }; }
function slimTapiwaRouteIntel(row) { return { route_name: row.route_name, pickup_landmark: row.pickup_landmark, dropoff_landmark: row.dropoff_landmark, distance_min_km: row.distance_min_km, distance_max_km: row.distance_max_km, pricing_notes: row.pricing_notes }; }
function slimTapiwaZoneBehavior(row) { return { zone_code: row.zone_code, zone_name: row.zone_name, demand_level: row.demand_level, pricing_behavior: row.pricing_behavior }; }
function slimTapiwaRouteLearning(row) { return { pickup_landmark: row.pickup_landmark, dropoff_landmark: row.dropoff_landmark, trip_count: row.trip_count, avg_final_price: row.avg_final_price }; }
function slimTapiwaOutcome(row) { return { trip_request_id: row.trip_request_id, final_price: row.final_price, created_at: row.created_at }; }

async function fetchTapiwaIntelligence(userMessage) {
  const keywords = getKeywords(userMessage);
  const [settingsRaw, marketRulesRaw, routeIntelRaw, zoneBehaviorRaw, routeLearningRaw, priceOutcomesRaw] = await Promise.all([
    supabaseFetch("tapiwa_system_settings", 50),
    supabaseFetch("tapiwa_market_price_rules", 250),
    supabaseFetch("tapiwa_route_intelligence", 250),
    supabaseFetch("tapiwa_zone_behavior", 50),
    supabaseFetch("tapiwa_route_learning", 150),
    supabaseFetch("tapiwa_price_outcomes", 50)
  ]);
  const matchedMarketRules = topMatches(marketRulesRaw, keywords, 8).filter((r) => r.active !== false);
  const matchedRouteIntel = topMatches(routeIntelRaw, keywords, 8).filter((r) => r.active !== false);
  const matchedZoneBehavior = topMatches(zoneBehaviorRaw, keywords, 6).filter((r) => r.active !== false);
  const matchedRouteLearning = topMatches(routeLearningRaw, keywords, 8);
  return { system_settings: settingsRaw, market_price_rules: matchedMarketRules.map(slimTapiwaMarketRule), route_intelligence: matchedRouteIntel.map(slimTapiwaRouteIntel), zone_behavior: matchedZoneBehavior.map(slimTapiwaZoneBehavior), route_learning: matchedRouteLearning.map(slimTapiwaRouteLearning), recent_price_outcomes: priceOutcomesRaw.slice(0, 10).map(slimTapiwaOutcome), data_counts: { tapiwa_system_settings: settingsRaw.length, tapiwa_market_price_rules: marketRulesRaw.length, tapiwa_route_intelligence: routeIntelRaw.length, tapiwa_zone_behavior: zoneBehaviorRaw.length, tapiwa_route_learning: routeLearningRaw.length, tapiwa_price_outcomes: priceOutcomesRaw.length }, matched_counts: { market_price_rules: matchedMarketRules.length, route_intelligence: matchedRouteIntel.length, zone_behavior: matchedZoneBehavior.length, route_learning: matchedRouteLearning.length } };
}

async function fetchZachanguContext(userMessage) {
  const keywords = getKeywords(userMessage);
  const [zonesRaw, landmarksRaw, pricingRulesRaw, marketPricesRaw, routeMatrixRaw, risksRaw, tapiwaIntelligence] = await Promise.all([
    supabaseFetch("zones", 60), supabaseFetch("landmarks", 250), supabaseFetch("pricing_rules", 30), supabaseFetch("market_prices", 150), supabaseFetch("route_matrix", 250), supabaseFetch("risks", 80), fetchTapiwaIntelligence(userMessage)
  ]);
  const matchedLandmarks = topMatches(landmarksRaw, keywords, 8);
  const routeUnderstanding = buildRouteUnderstanding(userMessage, landmarksRaw);
  const matchedZones = topMatches(zonesRaw, keywords, 6);
  const matchedMarketPrices = topMatches(marketPricesRaw, keywords, 8);
  const matchedRoutes = topMatches(routeMatrixRaw, keywords, 8);
  const matchedRisks = topMatches(risksRaw, keywords, 5);
  const resolvedPickupName = routeUnderstanding.pickup?.Landmark_Name || null;
  const resolvedDropoffName = routeUnderstanding.dropoff?.Landmark_Name || null;
  const exactRouteMatches = routeUnderstanding.confidence === "high" && resolvedPickupName && resolvedDropoffName ? routeMatrixRaw.filter((route) => routeMatchesResolvedLandmarks(route, resolvedPickupName, resolvedDropoffName)) : [];
  const exactMarketPriceMatches = routeUnderstanding.confidence === "high" && resolvedPickupName && resolvedDropoffName ? marketPricesRaw.filter((row) => marketPriceMatchesResolvedLandmarks(row, resolvedPickupName, resolvedDropoffName)) : [];
  const exactTapiwaMarketRules = routeUnderstanding.confidence === "high" && resolvedPickupName && resolvedDropoffName ? (tapiwaIntelligence.market_price_rules || []).filter((row) => tapiwaRuleMatchesResolvedLandmarks(row, resolvedPickupName, resolvedDropoffName)) : [];
  const exactTapiwaRouteIntel = routeUnderstanding.hasRoute ? (tapiwaIntelligence.route_intelligence || []).filter((row) => routeIntelMatchesResolvedLandmarks(row, resolvedPickupName, resolvedDropoffName) || routeIntelMatchesRawRoute(row, routeUnderstanding.pickupRaw, routeUnderstanding.dropoffRaw)) : [];
  const relevantZoneIds = new Set();
  for (const lm of matchedLandmarks) { if (lm.Zone_ID) relevantZoneIds.add(lm.Zone_ID); }
  for (const route of matchedRoutes) { if (route.Zone_From) relevantZoneIds.add(route.Zone_From); if (route.Zone_To) relevantZoneIds.add(route.Zone_To); }
  const relevantZones = zonesRaw.filter((z) => relevantZoneIds.has(z.Zone_ID));
  const activePricingRules = pricingRulesRaw.filter((rule) => String(rule.Active || "").toLowerCase() === "yes").slice(0, 6);
  return { request_keywords: keywords.slice(0, 12), route_understanding: routeUnderstanding, zones: [...matchedZones, ...relevantZones].filter((v, i, arr) => arr.findIndex((x) => x.Zone_ID === v.Zone_ID) === i).slice(0, 6).map(slimZone), landmarks: matchedLandmarks.slice(0, 8).map(slimLandmark), pricing_rules: activePricingRules.length ? activePricingRules.map(slimPricingRule) : pricingRulesRaw.slice(0, 4).map(slimPricingRule), market_prices: [...exactMarketPriceMatches, ...matchedMarketPrices].filter((v, i, arr) => arr.findIndex((x) => String(x.Route_ID || "") === String(v.Route_ID || "")) === i).slice(0, 8).map(slimMarketPrice), route_matrix: [...exactRouteMatches, ...matchedRoutes].filter((v, i, arr) => arr.findIndex((x) => String(x.Route_Key || "") === String(v.Route_Key || "")) === i).slice(0, 8).map(slimRoute), risks: matchedRisks.slice(0, 5).map(slimRisk), tapiwa_intelligence: { ...tapiwaIntelligence, market_price_rules: exactTapiwaMarketRules.length ? exactTapiwaMarketRules.map(slimTapiwaMarketRule) : tapiwaIntelligence.market_price_rules, route_intelligence: routeUnderstanding.hasRoute ? exactTapiwaRouteIntel.map(slimTapiwaRouteIntel) : tapiwaIntelligence.route_intelligence }, data_counts: { zones: zonesRaw.length, landmarks: landmarksRaw.length, pricing_rules: pricingRulesRaw.length, market_prices: marketPricesRaw.length, route_matrix: routeMatrixRaw.length, risks: risksRaw.length, ...tapiwaIntelligence.data_counts }, matched_counts: { zones: matchedZones.length, landmarks: matchedLandmarks.length, market_prices: matchedMarketPrices.length, route_matrix: matchedRoutes.length, risks: matchedRisks.length, exact_route_matrix: exactRouteMatches.length, exact_market_prices: exactMarketPriceMatches.length, ...tapiwaIntelligence.matched_counts }, table_status: tableDebug };
}

function calculateBasicFare(context) {
  const routeUnderstanding = context.route_understanding || {};
  const resolvedPickupName = routeUnderstanding.pickup?.Landmark_Name || null;
  const resolvedDropoffName = routeUnderstanding.dropoff?.Landmark_Name || null;
  if (!(routeUnderstanding.confidence === "high" && resolvedPickupName && resolvedDropoffName)) return null;
  const tapiwaRule = context.tapiwa_intelligence?.market_price_rules?.[0];
  if (tapiwaRule?.recommended_price && tapiwaRuleMatchesResolvedLandmarks(tapiwaRule, resolvedPickupName, resolvedDropoffName)) {
    const recommended = cleanPrice(tapiwaRule.recommended_price);
    return { source: "tapiwa_market_price_rules", estimated_low_mwk: cleanPrice(tapiwaRule.min_price || recommended), estimated_high_mwk: cleanPrice(tapiwaRule.max_price || recommended), recommended_mwk: recommended, route_used: `${tapiwaRule.pickup_landmark} → ${tapiwaRule.dropoff_landmark}` };
  }
  const route = (context.route_matrix || []).find((row) => routeMatchesResolvedLandmarks(row, resolvedPickupName, resolvedDropoffName));
  const rule = context.pricing_rules?.find((r) => String(r.Vehicle_Type || "").toLowerCase().includes("motorbike")) || context.pricing_rules?.[0];
  if (!route || !rule || !route.Distance_KM || !rule.Base_Rate_Per_KM_MWK) return null;
  const distance = Number(route.Distance_KM);
  const baseFare = Math.max(distance * Number(rule.Base_Rate_Per_KM_MWK), Number(rule.Minimum_Fare_MWK || 3000));
  return { source: "route_matrix_plus_pricing_rules", distance_km: distance, estimated_low_mwk: cleanPrice(baseFare * 0.95), estimated_high_mwk: cleanPrice(baseFare * 1.08), recommended_mwk: cleanPrice(baseFare), route_used: `${route.From_Landmark} → ${route.To_Landmark}` };
}

function buildDeterministicPricingReply(cleanMessage, context, memory) {
  const routeUnderstanding = context.route_understanding || {};
  const computedFare = calculateBasicFare(context);
  const routeIntel = context.tapiwa_intelligence?.route_intelligence?.[0] || null;
  const confirmedRoute = memory?.confirmedRoute || null;
  if (routeUnderstanding?.hasRoute && routeUnderstanding.confidence === "low") return { category: "pricing_issue", risk_level: "low", internal_summary: "Route unclear", team_message: "I can’t lock that route yet — send the pickup and drop-off clearly.", used_data: { route_understanding: routeUnderstanding, computed_fare: null } };
  if (routeUnderstanding?.hasRoute && routeUnderstanding.confidence === "medium") {
    const p = routeUnderstanding.pickup?.Landmark_Name || routeUnderstanding.pickupRaw;
    const d = routeUnderstanding.dropoff?.Landmark_Name || routeUnderstanding.dropoffRaw;
    return { category: "pricing_issue", risk_level: "low", internal_summary: "Route needs confirmation", team_message: `I think you mean ${p} to ${d} — confirm that route before I quote it.`, used_data: { route_understanding: routeUnderstanding } };
  }
  if (computedFare) return { category: "pricing_issue", risk_level: "low", internal_summary: `Quoted ${computedFare.route_used}`, team_message: `For ${computedFare.route_used}, quote around MWK ${Number(computedFare.recommended_mwk).toLocaleString()} — range is MWK ${Number(computedFare.estimated_low_mwk).toLocaleString()} to ${Number(computedFare.estimated_high_mwk).toLocaleString()}.`, used_data: { computed_fare: computedFare } };
  if (routeIntel) return { category: "pricing_issue", risk_level: "low", internal_summary: `Used route intelligence`, team_message: `For ${routeIntel.pickup_landmark} to ${routeIntel.dropoff_landmark}, I see about ${routeIntel.distance_min_km}km. ${routeIntel.pricing_notes || ""}`, used_data: { route_intelligence: [routeIntel] } };
  return null;
}

async function saveAuditLog(payload) {
  await supabaseInsert("tapiwa_ai_audit_logs", { request_type: payload.request_type || "team_chat", user_message: payload.user_message, clean_message: payload.clean_message, ai_category: payload.ai_category, team_message: payload.team_message, internal_summary: payload.internal_summary, success: payload.success !== false, source_type: "ai_server" });
}

app.post("/ai/analyze", async (req, res) => {
  let cleanMessage = ""; let context = null; let aiResult = {};
  try {
    const { message, senderName = "Dispatcher", senderRole = "Dispatcher", sessionId: rawSessionId } = req.body || {};
    if (!message || !String(message).trim()) return res.status(400).json({ error: "Message is required" });
    if (!hasTapiwaCall(message)) return res.json({ ignored: true, reason: "Use @Tapiwa to call the AI." });

    cleanMessage = cleanTapiwaMessage(message);
    const sessionId = buildSessionId({ sessionId: rawSessionId, senderName, senderRole });
    const memory = getConversationMemory(sessionId);

    // --- FIX: Small Talk Bypass ---
    if (isSmallTalk(cleanMessage)) {
       // Skip deterministic rules and go straight to GROQ for a human response
       console.log("Small talk detected, skipping deterministic rules.");
    } else {
        // Run deterministic rules for dispatch and pricing
        const isAffirmation = isAffirmationMessage(cleanMessage);
        const isFollowUp = isFollowUpRouteMessage(cleanMessage);
        const hasExplicitRoute = isExplicitRouteMessage(cleanMessage);

        if (isAffirmation && memory.pendingConfirmation && memory.lastRoute) {
          memory.confirmedRoute = { ...memory.lastRoute }; memory.pendingConfirmation = false; saveConversationMemory(sessionId, memory);
          return res.json({ category: "pricing_issue", team_message: `Confirmed ${memory.confirmedRoute.pickup || memory.confirmedRoute.pickupRaw} to ${memory.confirmedRoute.dropoff || memory.confirmedRoute.dropoffRaw}.` });
        }

        context = await fetchZachanguContext(cleanMessage);
        const deterministicPricingReply = isPricingLikeMessage(cleanMessage) ? buildDeterministicPricingReply(cleanMessage, context, memory) : null;
        const deterministicDispatchReply = !deterministicPricingReply ? buildDeterministicDispatchReply(cleanMessage, memory) : null;

        if (deterministicPricingReply) return res.json({ ignored: false, ...deterministicPricingReply });
        if (deterministicDispatchReply) return res.json({ ignored: false, ...deterministicDispatchReply });
    }

    // Default: Ask the AI
    if (!context) context = await fetchZachanguContext(cleanMessage);
    const systemPrompt = `You are Dispatch Tapiwa, a person on the Zachangu dispatch team in Lilongwe. Sound human, calm, and short. JSON only output.`;
    const groqResponse = await fetchWithTimeout("https://api.groq.com/openai/v1/chat/completions", {
      method: "POST", headers: { Authorization: `Bearer ${GROQ_API_KEY}`, "Content-Type": "application/json" },
      body: JSON.stringify({ model: GROQ_MODEL, messages: [{ role: "system", content: systemPrompt }, { role: "user", content: JSON.stringify({ msg: cleanMessage, ctx: context }) }], response_format: { type: "json_object" } })
    });
    const groqData = await groqResponse.json();
    aiResult = JSON.parse(groqData.choices?.[0]?.message?.content || "{}");

    const finalResponse = {
      category: aiResult.category || "general_update",
      team_message: aiResult.team_message || "Noted team, let's keep moving.",
      used_data: aiResult.used_data || {}
    };
    await saveAuditLog({ user_message: message, clean_message: cleanMessage, team_message: finalResponse.team_message });
    return res.json(finalResponse);

  } catch (error) {
    console.error("AI ERROR:", error);
    return res.json(buildTemporaryTapiwaFallback());
  }
});

const port = process.env.PORT || 3001;
app.listen(port, "0.0.0.0", () => { console.log(`Zachangu AI server running on port ${port}`); });
