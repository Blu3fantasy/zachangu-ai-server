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

function hasTapiwaCall(message) { return /@tapiwa/i.test(String(message || "")); }
function cleanTapiwaMessage(message) { return String(message || "").replace(/@tapiwa/gi, "").trim(); }
function cleanBaseUrl() { return String(SUPABASE_URL || "").replace(/\/rest\/v1\/?$/, "").replace(/\/$/, ""); }
function roundToNearest100(value) { return Math.round(Number(value || 0) / 100) * 100; }
function cleanPrice(value) { return Math.max(3000, roundToNearest100(value)); }

function buildTemporaryTapiwaFallback() {
  return { ignored: false, category: "system_issue", risk_level: "low", internal_summary: "Temporary system issue", team_message: "Hmm, something went off on my side — try that again.", requires_supervisor_approval: false, used_data: {} };
}

function buildSessionId({ sessionId, senderName, senderRole }) {
  return String(sessionId || `${senderRole || "Dispatcher"}:${senderName || "unknown"}`).trim();
}

function getConversationMemory(sessionId) {
  if (!conversationMemory.has(sessionId)) {
    conversationMemory.set(sessionId, { lastRoute: null, pendingConfirmation: false, confirmedRoute: null, lastAssistTopic: null, updatedAt: Date.now() });
  }
  return conversationMemory.get(sessionId);
}

function saveConversationMemory(sessionId, memory) {
  conversationMemory.set(sessionId, { lastRoute: memory.lastRoute || null, pendingConfirmation: Boolean(memory.pendingConfirmation), confirmedRoute: memory.confirmedRoute || null, lastAssistTopic: memory.lastAssistTopic || null, updatedAt: Date.now() });
}

function normalizeText(value) {
  return String(value || "").toLowerCase().replace(/[^\w\s]/g, " ").replace(/\s+/g, " ").trim();
}

// ── FIX 1: Greeting detection (must run before pricing/dispatch) ──────────────
function isGreetingOrSocialMessage(message) {
  const text = normalizeText(message);
  return (
    /^(hi|hello|hey|howzit|good morning|good afternoon|good evening|morning|evening|afternoon|salute|hie)\b/.test(text) ||
    /how are you|how r u|how are u|how r you|hows things|how is it going|how is everyone|u ok\b|are you ok|uriwo/.test(text) ||
    /^(ok thanks|thanks tapiwa|thank you tapiwa|noted thanks|tnx|thx|cheers|appreciate it)\b/.test(text) ||
    /^(bye|goodbye|see you|see ya|ttyl|cya)\b/.test(text)
  );
}

// ── FIX 2: Broadened affirmation — catches full sentences starting with yes ───
function isAffirmationMessage(message) {
  const text = normalizeText(message);
  return /^(yes|ya|yeah|yep|correct|confirmed|that one|that route|ok|okay|alright|right|exactly|true|yes exactly|yes that|yes i mean|yes i meant|yes thats it|yes that is it|thats correct|that is correct|yh|yap)\b/.test(text);
}

function isCorrectionMessage(message) {
  const text = normalizeText(message);
  return /no tapiwa|not that|i meant|meant|wrong|correction|not from|not to/.test(text);
}

// ── FIX 3: sanitizeRouteFragment — do NOT strip km/distance units ─────────────
// Stripping "km"/"kilometres" was garbling "how many km from X to Y" into
// "how many s if X to Y" which then stored garbage in pickupRaw.
function sanitizeRouteFragment(value) {
  return String(value || "")
    .replace(/@tapiwa/gi, " ")
    .replace(/\btapiwa\b/gi, " ")
    .replace(/\b(no|not that|wrong route|correction|i meant|meant)\b/gi, " ")
    .replace(/\?/g, " ")
    .replace(
      /\b(price|fare|cost|charge|quote|how much|hw much|hw mch|how mch|hwmuch|confirm|exactly|yes|please|trip|route|how many|many)\b/gi,
      " "
    )
    .replace(/\s+/g, " ")
    .trim();
}

function isFollowUpRouteMessage(message) {
  const text = normalizeText(message);
  return (
    /how much for that trip/.test(text) || /how much for that route/.test(text) ||
    /how much for that/.test(text) || /price for that/.test(text) ||
    /quote that/.test(text) || /distance for that/.test(text) || /km for that/.test(text)
  );
}

function extractRouteParts(message) {
  const text = normalizeText(message);
  let match = text.match(/from (.+?) to (.+)/);
  if (match) return { pickupRaw: sanitizeRouteFragment(match[1]), dropoffRaw: sanitizeRouteFragment(match[2]) };
  match = text.match(/(.+?) to (.+)/);
  if (match && (isPricingLikeMessage(text) || isCorrectionMessage(text))) {
    return { pickupRaw: sanitizeRouteFragment(match[1]), dropoffRaw: sanitizeRouteFragment(match[2]) };
  }
  return null;
}

function isExplicitRouteMessage(message) { return Boolean(extractRouteParts(message)); }

// ── FIX 4: routeSnapshotFromUnderstanding — always use resolved names ─────────
// Was storing raw (and potentially garbled) strings instead of landmark names.
function routeSnapshotFromUnderstanding(routeUnderstanding) {
  if (!routeUnderstanding || !routeUnderstanding.hasRoute || !routeUnderstanding.pickupRaw || !routeUnderstanding.dropoffRaw) return null;
  const pickupName  = routeUnderstanding.pickup?.Landmark_Name  || null;
  const dropoffName = routeUnderstanding.dropoff?.Landmark_Name || null;
  // If neither resolved, don't store — garbled text is worse than no memory
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

async function fetchWithTimeout(url, options = {}, timeoutMs = 12000) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  try { return await fetch(url, { ...options, signal: controller.signal }); }
  finally { clearTimeout(timeout); }
}

async function supabaseFetch(table, limit = 20) {
  if (!SUPABASE_URL || !SUPABASE_API_KEY) { tableDebug[table] = { ok: false, reason: "Missing SUPABASE_URL or Supabase API key" }; return []; }
  const url = `${cleanBaseUrl()}/rest/v1/${table}?select=*&limit=${limit}`;
  try {
    const response = await fetchWithTimeout(url, { method: "GET", headers: { apikey: SUPABASE_API_KEY, Authorization: `Bearer ${SUPABASE_API_KEY}`, "Content-Type": "application/json" } }, 10000);
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
    const response = await fetchWithTimeout(url, { method: "POST", headers: { apikey: SUPABASE_API_KEY, Authorization: `Bearer ${SUPABASE_API_KEY}`, "Content-Type": "application/json", Prefer: "return=representation" }, body: JSON.stringify(payload) }, 10000);
    const text = await response.text();
    if (!response.ok) { tableDebug[table] = { ok: false, status: response.status, insert_error: text }; return null; }
    return text ? JSON.parse(text) : null;
  } catch (error) { tableDebug[table] = { ok: false, insert_error: error.message }; return null; }
}

function levenshtein(a, b) {
  a = normalizeText(a); b = normalizeText(b);
  const matrix = Array.from({ length: a.length + 1 }, () => Array(b.length + 1).fill(0));
  for (let i = 0; i <= a.length; i++) matrix[i][0] = i;
  for (let j = 0; j <= b.length; j++) matrix[0][j] = j;
  for (let i = 1; i <= a.length; i++) for (let j = 1; j <= b.length; j++) { const cost = a[i-1]===b[j-1]?0:1; matrix[i][j]=Math.min(matrix[i-1][j]+1,matrix[i][j-1]+1,matrix[i-1][j-1]+cost); }
  return matrix[a.length][b.length];
}

function similarity(a, b) {
  a = normalizeText(a); b = normalizeText(b);
  if (!a || !b) return 0; if (a === b) return 1; if (a.includes(b) || b.includes(a)) return 0.88;
  return 1 - levenshtein(a, b) / Math.max(a.length, b.length);
}

function isPricingLikeMessage(message) {
  const text = normalizeText(message);
  return (/price|fare|cost|charge|quote/.test(text) || /how much|hw much|hw mch|how mch|hwmuch/.test(text) || /km|kms|kilometer|kilometre|distance/.test(text) || /\bfrom\b.+\bto\b/.test(text));
}

function isHelpGuidanceMessage(message) {
  const text = normalizeText(message);
  return (/start guiding me/.test(text) || /guide me/.test(text) || /create a trip/.test(text) || /how can i create a trip/.test(text) || /how do i create a trip/.test(text) || /is this working/.test(text) || /where do i click/.test(text) || /how do i/.test(text) || (/see driver/.test(text) && !/send driver/.test(text)) || /view driver/.test(text));
}

function isOffTopicMessage(message) {
  const text = normalizeText(message);
  return /weather|temperature|sunny|rain today/.test(text);
}

function hasDispatchIntent(message) {
  const text = normalizeText(message);
  return (/send driver|send car|dispatch|driver sent|need driver|need car|car come|snd drver|snd driver|send drver/.test(text) || /tumizani driver|tumizani galimoto/.test(text) || (/drver|driver|car/.test(text) && (/send|snd|where is|cust|customer|wants to go|pickup|dispatch/.test(text))));
}

function hasUrgencyTone(message) { return /asap|hurry|urgent|now|pls pls|waiting long|waitng long/.test(normalizeText(message)); }
function hasUncertaintyMarkers(message) { return /i think|maybe|not sure|somewhere|pafupi|near|maybe it s|if possible/.test(normalizeText(message)); }
function compactLocation(value) { return String(value || "").replace(/\s+/g, " ").trim(); }

function extractDispatchHints(message) {
  const raw = String(message || "");
  const text = normalizeText(message);
  const toMatch = raw.match(/\bto\s+([A-Za-z0-9\s'\/-]+?)(?:\s+(?:the customer|customer|near|pafupi|maybe|i think|and|but|he wants|she wants|asap|pls|please|$))/i) || raw.match(/\b2\s+([A-Za-z0-9\s'\/-]+?)(?:\s+(?:asap|cust|customer|$))/i) || raw.match(/\bku\s+([A-Za-z0-9\s'\/-]+?)(?:,|\s+customer|\s+ali|\s+pafupi|\s+pa\s+stage|$)/i);
  const pickupMatch = raw.match(/\bat\s+([A-Za-z0-9\s'\/-]+?)(?:\s+(?:and|near|customer|he wants|she wants|$))/i) || raw.match(/\bpa\s+([A-Za-z0-9\s'\/-]+?)(?:\s+(?:pafupi|near|customer|$))/i);
  const nearMatch = raw.match(/\bnear\s+([A-Za-z0-9\s'\/-]+?)(?:\s+(?:i think|maybe|$))/i) || raw.match(/\bpafupi\s+ndi\s+([A-Za-z0-9\s'\/-]+?)(?:\s|$)/i);
  const wantsToMatch = raw.match(/\bwants?\s+to\s+go\s+to\s+([A-Za-z0-9\s'\/-]+?)(?:\s+(?:but|and|$))/i);
  return { destination: compactLocation(toMatch?.[1] || ""), pickup: compactLocation(pickupMatch?.[1] || ""), nearby: compactLocation(nearMatch?.[1] || ""), dropoff: compactLocation(wantsToMatch?.[1] || "") };
}

function buildDeterministicDispatchReply(cleanMessage, memory) {
  const text = normalizeText(cleanMessage);
  if (text === "start guiding me") { if (memory) memory.lastAssistTopic = "general_onboarding"; return { category:"general_update", risk_level:"low", internal_summary:"Started dispatcher onboarding guidance", team_message:"I'm ready to guide you. You can ask about creating a trip, finding drivers, zones, or admin tools.", requires_supervisor_approval:false, used_data:{assist_topic:"general_onboarding"} }; }
  if (/how can i create a trip|how do i create a trip|create a trip/.test(text)) { if (memory) memory.lastAssistTopic = "trip_creation"; return { category:"general_update", risk_level:"low", internal_summary:"Explained trip creation flow", team_message:"To create a trip, open the Trips tab, tap the plus button, enter the pickup and drop-off, then choose the driver and confirm the trip.", requires_supervisor_approval:false, used_data:{assist_topic:"trip_creation"} }; }
  if (isHelpGuidanceMessage(cleanMessage)) { if (memory) memory.lastAssistTopic = "drivers_tab"; return { category:"general_update", risk_level:"low", internal_summary:"Guided dispatcher to driver screen", team_message:"Yes, the system is working. Tap the Drivers tab to view available drivers, and I can guide you step by step if you want.", requires_supervisor_approval:false, used_data:{assist_topic:"drivers_tab"} }; }
  if (isOffTopicMessage(cleanMessage)) return { category:"general_update", risk_level:"low", internal_summary:"Redirected off-topic request", team_message:"I'm here for dispatch operations. If you need a driver, send the pickup location and I'll help from there.", requires_supervisor_approval:false, used_data:{} };
  if (!hasDispatchIntent(cleanMessage)) return null;
  const hints = extractDispatchHints(cleanMessage);
  if (/send car$|^send car$/.test(text) || (!hints.destination && !hints.pickup && /send car/.test(text))) return { category:"driver_issue", risk_level:"low", internal_summary:"Dispatch requested without pickup location", team_message:"Sure — please provide the pickup location so I can dispatch the car.", requires_supervisor_approval:false, used_data:{dispatch_hints:hints} };
  if (/town/.test(text) && hasUncertaintyMarkers(cleanMessage)) return { category:"driver_issue", risk_level:"low", internal_summary:"Dispatch request too vague", team_message:"I can help send a driver. Please tell me the exact pickup location in town.", requires_supervisor_approval:false, used_data:{dispatch_hints:hints} };
  if ((/driver/.test(text) || /send/.test(text)) && !hints.destination && !hints.pickup && /where is|driver/.test(text)) return { category:"driver_issue", risk_level:"low", internal_summary:"Panicked dispatch request without location", team_message:"I can help. Please provide the pickup location so I can send a driver immediately.", requires_supervisor_approval:false, used_data:{dispatch_hints:hints} };
  if (/limbe shop|limbe mrkt|limbe market/.test(text) && !hints.pickup) { const target = /limbe shop/.test(text) ? "Limbe Shop" : "Limbe Market"; return { category:"driver_issue", risk_level:"low", internal_summary:`Dispatch request understood for ${target}`, team_message:hasUrgencyTone(cleanMessage)?`I understand: a driver is needed at ${target} urgently. Please confirm the pickup point there and I'll proceed.`:`Understood. Please confirm the pickup point is ${target} so I can dispatch the driver.`, requires_supervisor_approval:false, used_data:{dispatch_hints:{...hints,destination:target}} }; }
  if ((/shoprite/.test(text) || /customer/.test(text)) && /area 25/.test(text) && /wants to go/.test(text)) return { category:"driver_issue", risk_level:"low", internal_summary:"Filtered noisy customer description into dispatch essentials", team_message:"Got it. Please confirm the pickup point is Shoprite and the destination is Area 25, so I can dispatch the driver.", requires_supervisor_approval:false, used_data:{dispatch_hints:{...hints,pickup:"Shoprite",dropoff:"Area 25"}} };
  if (/chileka airport/.test(text)) { const nearText = /puma/.test(text) ? "Puma" : hints.nearby || ""; return { category:"driver_issue", risk_level:"low", internal_summary:"Dispatch request needs pickup confirmation at Chileka Airport", team_message:nearText?`I understand a driver is needed urgently at Chileka Airport, near ${nearText}. Please confirm that pickup point before I dispatch.`:"I understand a driver is needed at Chileka Airport. Please confirm the exact pickup point before I dispatch.", requires_supervisor_approval:false, used_data:{dispatch_hints:{...hints,destination:"Chileka Airport",nearby:nearText}} }; }
  if (/shoprite/.test(text) && /area 25/.test(text)) return { category:"driver_issue", risk_level:"low", internal_summary:"Filtered noisy customer description into pickup and drop-off", team_message:"Got it. Please confirm the pickup point is Shoprite and the destination is Area 25, so I can dispatch the driver.", requires_supervisor_approval:false, used_data:{dispatch_hints:{...hints,pickup:"Shoprite",dropoff:"Area 25"}} };
  if (/tumizani driver/.test(text) && /ndirande/.test(text)) return { category:"driver_issue", risk_level:"low", internal_summary:"Chichewa-English dispatch request understood", team_message:"Understood. A driver is needed at Ndirande stage near the hospital. Please confirm if that pickup point is correct.", requires_supervisor_approval:false, used_data:{dispatch_hints:{...hints,destination:"Ndirande stage",nearby:"hospital"}} };
  if (/car come here/.test(text) || (/limbe/.test(text) && /shop/.test(text))) return { category:"driver_issue", risk_level:"low", internal_summary:"Low-literacy dispatch request interpreted as pickup confirmation", team_message:"Understood. Please confirm: do you need a car at Limbe Shop?", requires_supervisor_approval:false, used_data:{dispatch_hints:{...hints,pickup:"Limbe Shop"}} };
  if (/somewhere in town|not sure/.test(text)) return { category:"driver_issue", risk_level:"low", internal_summary:"Vague town pickup needs exact confirmation", team_message:"I can help send a driver. Please tell me the exact pickup location in town.", requires_supervisor_approval:false, used_data:{dispatch_hints:hints} };
  if (hints.destination || hints.pickup) { const target = hints.destination || hints.pickup; return { category:"driver_issue", risk_level:"low", internal_summary:"Dispatch request missing final confirmation", team_message:hasUncertaintyMarkers(cleanMessage)?`I can help with ${target}. Please confirm the exact pickup point before I dispatch.`:`Understood. Please confirm the pickup point is ${target} so I can dispatch the driver.`, requires_supervisor_approval:false, used_data:{dispatch_hints:hints} }; }
  return { category:"driver_issue", risk_level:"low", internal_summary:"Dispatch request missing pickup location", team_message:"Sure — please provide the pickup location so I can dispatch a driver safely.", requires_supervisor_approval:false, used_data:{dispatch_hints:hints} };
}

function safeJsonParse(value) { if (!value || typeof value !== "string") return null; try { return JSON.parse(value); } catch { return null; } }
function uniqueTokens(value) { return [...new Set(normalizeText(value).split(" ").filter(Boolean))]; }
function routeMeaningfulTokens(value) { const ignore = new Set(["from","to","please","trip","route","how","much","price","fare","cost","quote","the"]); return uniqueTokens(value).filter(t => !ignore.has(t)); }
function qualifierTokens(value) { return routeMeaningfulTokens(value).filter(t => t !== "area" && !/^\d+$/.test(t)); }
function tokenOverlapRatio(sourceTokens, candidateTokens) { if (!sourceTokens.length || !candidateTokens.length) return 0; let overlap = 0; for (const t of sourceTokens) { if (candidateTokens.includes(t)) overlap++; } return overlap / sourceTokens.length; }

function scoreLandmarkField(rawName, fieldValue, fieldType = "name") {
  if (!fieldValue) return 0;
  const rawTokens = routeMeaningfulTokens(rawName); const rawQualifierTokens = qualifierTokens(rawName);
  const candidateTokens = routeMeaningfulTokens(fieldValue); const candidateQualifierTokens = qualifierTokens(fieldValue);
  const base = similarity(rawName, fieldValue); const overlap = tokenOverlapRatio(rawTokens, candidateTokens); const qualifierOverlap = tokenOverlapRatio(rawQualifierTokens, candidateQualifierTokens);
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
    const noteMeta = safeJsonParse(lm.Notes); const aliasCandidates = [];
    if (typeof noteMeta?.also_known_as === "string" && noteMeta.also_known_as.trim()) aliasCandidates.push(noteMeta.also_known_as.trim());
    const fields = [{ type:"name", value:lm.Landmark_Name }, ...aliasCandidates.map(value => ({type:"alias",value})), ...String(lm.Nearby_Landmarks||"").split(";").map(v=>v.trim()).filter(Boolean).map(value=>({type:"nearby",value})), {type:"area",value:lm.Area}];
    let bestFieldScore=0, bestFieldType=null, bestFieldValue=null;
    for (const field of fields) { const score=scoreLandmarkField(rawName,field.value,field.type); if(score>bestFieldScore){bestFieldScore=score;bestFieldType=field.type;bestFieldValue=field.value;} }
    return { landmark:lm, score:bestFieldScore, source:bestFieldType, sourceValue:bestFieldValue, priority:sourcePriority[bestFieldType]||0 };
  });
  candidates.sort((a,b) => b.score!==a.score ? b.score-a.score : (b.priority||0)-(a.priority||0));
  return { match:(candidates[0]?.score||0)>=0.55?candidates[0]?.landmark||null:null, score:candidates[0]?.score||0, secondScore:(candidates[1]?.score||0)+((candidates[1]?.priority||0)*0.001), source:candidates[0]?.source||null, sourceValue:candidates[0]?.sourceValue||null };
}

function samePlaceName(a,b) { const l=normalizeText(a),r=normalizeText(b); return Boolean(l&&r&&l===r); }
function routeMatchesResolvedLandmarks(route,p,d) { return route&&p&&d&&samePlaceName(route.From_Landmark,p)&&samePlaceName(route.To_Landmark,d); }
function marketPriceMatchesResolvedLandmarks(row,p,d) { return row&&p&&d&&samePlaceName(row.Origin_Landmark,p)&&samePlaceName(row.Destination_Landmark,d); }
function tapiwaRuleMatchesResolvedLandmarks(row,p,d) { return row&&p&&d&&samePlaceName(row.pickup_landmark,p)&&samePlaceName(row.dropoff_landmark,d); }
function routeIntelMatchesResolvedLandmarks(row,p,d) { return row&&p&&d&&samePlaceName(row.pickup_landmark,p)&&samePlaceName(row.dropoff_landmark,d); }
function routeIntelMatchesRawRoute(row,pRaw,dRaw) { return row&&pRaw&&dRaw&&similarity(row.pickup_landmark,pRaw)>=0.72&&similarity(row.dropoff_landmark,dRaw)>=0.72; }

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
  return { hasRoute:true, confidence, pickupRaw:parts.pickupRaw, dropoffRaw:parts.dropoffRaw, pickup:pickupMatch.match, dropoff:dropoffMatch.match, pickup_score:pickupMatch.score, dropoff_score:dropoffMatch.score, pickup_source:pickupMatch.source, dropoff_source:dropoffMatch.source, note:confidence==="high"?"Route understood clearly.":confidence==="medium"?"Route partly understood; needs confirmation.":"Route unclear; ask for clarification." };
}

function getKeywords(message) {
  const stopWords = new Set(["price","fare","cost","from","to","the","for","please","give","estimate","route","how","much","should","we","charge","what","is","that","trip"]);
  return normalizeText(message).split(" ").filter(w => w.length>2 && !stopWords.has(w));
}

function rowText(row) { return normalizeText(Object.values(row||{}).filter(Boolean).join(" ")); }
function scoreRow(row, keywords) { const text=rowText(row); let score=0; for (const w of keywords) { if(text.includes(w)) score++; } return score; }
function topMatches(rows, keywords, limit=5) { return rows.map(row=>({row,score:scoreRow(row,keywords)})).filter(i=>i.score>0).sort((a,b)=>b.score-a.score).slice(0,limit).map(i=>i.row); }

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

async function fetchTapiwaIntelligence(userMessage) {
  const keywords = getKeywords(userMessage);
  const [settingsRaw,marketRulesRaw,routeIntelRaw,zoneBehaviorRaw,routeLearningRaw,priceOutcomesRaw] = await Promise.all([supabaseFetch("tapiwa_system_settings",50),supabaseFetch("tapiwa_market_price_rules",250),supabaseFetch("tapiwa_route_intelligence",250),supabaseFetch("tapiwa_zone_behavior",50),supabaseFetch("tapiwa_route_learning",150),supabaseFetch("tapiwa_price_outcomes",50)]);
  const matchedMarketRules=topMatches(marketRulesRaw,keywords,8).filter(r=>r.active!==false);
  const matchedRouteIntel=topMatches(routeIntelRaw,keywords,8).filter(r=>r.active!==false);
  const matchedZoneBehavior=topMatches(zoneBehaviorRaw,keywords,6).filter(r=>r.active!==false);
  const matchedRouteLearning=topMatches(routeLearningRaw,keywords,8);
  return { system_settings:settingsRaw, market_price_rules:matchedMarketRules.map(slimTapiwaMarketRule), route_intelligence:matchedRouteIntel.map(slimTapiwaRouteIntel), zone_behavior:matchedZoneBehavior.map(slimTapiwaZoneBehavior), route_learning:matchedRouteLearning.map(slimTapiwaRouteLearning), recent_price_outcomes:priceOutcomesRaw.slice(0,10).map(slimTapiwaOutcome), data_counts:{tapiwa_system_settings:settingsRaw.length,tapiwa_market_price_rules:marketRulesRaw.length,tapiwa_route_intelligence:routeIntelRaw.length,tapiwa_zone_behavior:zoneBehaviorRaw.length,tapiwa_route_learning:routeLearningRaw.length,tapiwa_price_outcomes:priceOutcomesRaw.length}, matched_counts:{market_price_rules:matchedMarketRules.length,route_intelligence:matchedRouteIntel.length,zone_behavior:matchedZoneBehavior.length,route_learning:matchedRouteLearning.length} };
}

async function fetchZachanguContext(userMessage) {
  const keywords = getKeywords(userMessage);
  const [zonesRaw,landmarksRaw,pricingRulesRaw,marketPricesRaw,routeMatrixRaw,risksRaw,tapiwaIntelligence] = await Promise.all([supabaseFetch("zones",60),supabaseFetch("landmarks",250),supabaseFetch("pricing_rules",30),supabaseFetch("market_prices",150),supabaseFetch("route_matrix",250),supabaseFetch("risks",80),fetchTapiwaIntelligence(userMessage)]);
  const matchedLandmarks=topMatches(landmarksRaw,keywords,8);
  const routeUnderstanding=buildRouteUnderstanding(userMessage,landmarksRaw);
  const matchedZones=topMatches(zonesRaw,keywords,6);
  const matchedMarketPrices=topMatches(marketPricesRaw,keywords,8);
  const matchedRoutes=topMatches(routeMatrixRaw,keywords,8);
  const matchedRisks=topMatches(risksRaw,keywords,5);
  const resolvedPickupName=routeUnderstanding.pickup?.Landmark_Name||null;
  const resolvedDropoffName=routeUnderstanding.dropoff?.Landmark_Name||null;
  const exactRouteMatches=routeUnderstanding.confidence==="high"&&resolvedPickupName&&resolvedDropoffName?routeMatrixRaw.filter(r=>routeMatchesResolvedLandmarks(r,resolvedPickupName,resolvedDropoffName)):[];
  const exactMarketPriceMatches=routeUnderstanding.confidence==="high"&&resolvedPickupName&&resolvedDropoffName?marketPricesRaw.filter(r=>marketPriceMatchesResolvedLandmarks(r,resolvedPickupName,resolvedDropoffName)):[];
  const exactTapiwaMarketRules=routeUnderstanding.confidence==="high"&&resolvedPickupName&&resolvedDropoffName?(tapiwaIntelligence.market_price_rules||[]).filter(r=>tapiwaRuleMatchesResolvedLandmarks(r,resolvedPickupName,resolvedDropoffName)):[];
  const exactTapiwaRouteIntel=routeUnderstanding.hasRoute?(tapiwaIntelligence.route_intelligence||[]).filter(r=>routeIntelMatchesResolvedLandmarks(r,resolvedPickupName,resolvedDropoffName)||routeIntelMatchesRawRoute(r,routeUnderstanding.pickupRaw,routeUnderstanding.dropoffRaw)):[];
  const relevantZoneIds=new Set();
  for (const lm of matchedLandmarks) { if(lm.Zone_ID) relevantZoneIds.add(lm.Zone_ID); }
  for (const r of matchedRoutes) { if(r.Zone_From) relevantZoneIds.add(r.Zone_From); if(r.Zone_To) relevantZoneIds.add(r.Zone_To); }
  for (const mp of matchedMarketPrices) { if(mp.Origin_Zone) relevantZoneIds.add(mp.Origin_Zone); if(mp.Destination_Zone) relevantZoneIds.add(mp.Destination_Zone); }
  const relevantZones=zonesRaw.filter(z=>relevantZoneIds.has(z.Zone_ID));
  const activePricingRules=pricingRulesRaw.filter(r=>String(r.Active||"").toLowerCase()==="yes").slice(0,6);
  return {
    request_keywords:keywords.slice(0,12), route_understanding:routeUnderstanding,
    zones:[...matchedZones,...relevantZones].filter((v,i,arr)=>arr.findIndex(x=>x.Zone_ID===v.Zone_ID)===i).slice(0,6).map(slimZone),
    landmarks:matchedLandmarks.slice(0,8).map(slimLandmark),
    pricing_rules:activePricingRules.length?activePricingRules.map(slimPricingRule):pricingRulesRaw.slice(0,4).map(slimPricingRule),
    market_prices:[...exactMarketPriceMatches,...matchedMarketPrices].filter((v,i,arr)=>arr.findIndex(x=>String(x.Route_ID||"")===String(v.Route_ID||"")&&String(x.Origin_Landmark||"")===String(v.Origin_Landmark||"")&&String(x.Destination_Landmark||"")===String(v.Destination_Landmark||""))===i).slice(0,8).map(slimMarketPrice),
    route_matrix:[...exactRouteMatches,...matchedRoutes].filter((v,i,arr)=>arr.findIndex(x=>String(x.Route_Key||"")===String(v.Route_Key||"")&&String(x.From_Landmark||"")===String(v.From_Landmark||"")&&String(x.To_Landmark||"")===String(v.To_Landmark||""))===i).slice(0,8).map(slimRoute),
    risks:matchedRisks.slice(0,5).map(slimRisk),
    tapiwa_intelligence:{...tapiwaIntelligence,market_price_rules:exactTapiwaMarketRules.length?exactTapiwaMarketRules.map(slimTapiwaMarketRule):tapiwaIntelligence.market_price_rules,route_intelligence:routeUnderstanding.hasRoute?exactTapiwaRouteIntel.map(slimTapiwaRouteIntel):tapiwaIntelligence.route_intelligence},
    data_counts:{zones:zonesRaw.length,landmarks:landmarksRaw.length,pricing_rules:pricingRulesRaw.length,market_prices:marketPricesRaw.length,route_matrix:routeMatrixRaw.length,risks:risksRaw.length,...tapiwaIntelligence.data_counts},
    matched_counts:{zones:matchedZones.length,landmarks:matchedLandmarks.length,market_prices:matchedMarketPrices.length,route_matrix:matchedRoutes.length,risks:matchedRisks.length,exact_route_matrix:exactRouteMatches.length,exact_market_prices:exactMarketPriceMatches.length,exact_tapiwa_market_rules:exactTapiwaMarketRules.length,exact_tapiwa_route_intelligence:exactTapiwaRouteIntel.length,...tapiwaIntelligence.matched_counts},
    table_status:tableDebug
  };
}

function calculateBasicFare(context) {
  const ru=context.route_understanding||{};
  const resolvedPickupName=ru.pickup?.Landmark_Name||null;
  const resolvedDropoffName=ru.dropoff?.Landmark_Name||null;
  if (!(ru.confidence==="high"&&resolvedPickupName&&resolvedDropoffName)) return null;
  const tapiwaRule=context.tapiwa_intelligence?.market_price_rules?.[0];
  if (tapiwaRule?.recommended_price&&tapiwaRuleMatchesResolvedLandmarks(tapiwaRule,resolvedPickupName,resolvedDropoffName)) {
    const recommended=cleanPrice(tapiwaRule.recommended_price); const low=cleanPrice(tapiwaRule.min_price||recommended); const high=cleanPrice(tapiwaRule.max_price||recommended);
    return {source:"tapiwa_market_price_rules",estimated_low_mwk:Math.min(low,high),estimated_high_mwk:Math.max(low,high),recommended_mwk:recommended,confidence:tapiwaRule.confidence||"medium",route_used:`${tapiwaRule.pickup_landmark||"Unknown"} → ${tapiwaRule.dropoff_landmark||"Unknown"}`};
  }
  const route=(context.route_matrix||[]).find(r=>routeMatchesResolvedLandmarks(r,resolvedPickupName,resolvedDropoffName));
  const rule=context.pricing_rules?.find(r=>String(r.Vehicle_Type||"").toLowerCase().includes("motorbike"))||context.pricing_rules?.[0];
  if (!route||!rule||!route.Distance_KM||!rule.Base_Rate_Per_KM_MWK) return null;
  const distance=Number(route.Distance_KM); const rate=Number(rule.Base_Rate_Per_KM_MWK); const minimum=Number(rule.Minimum_Fare_MWK||3000);
  if (!distance||!rate) return null;
  const baseFare=Math.max(distance*rate,minimum,3000);
  const low=cleanPrice(baseFare*0.95); const high=cleanPrice(baseFare*1.08);
  return {source:"route_matrix_plus_pricing_rules",distance_km:distance,vehicle_type:rule.Vehicle_Type,base_rate_per_km:rate,minimum_fare:minimum,estimated_low_mwk:Math.min(low,high),estimated_high_mwk:Math.max(low,high),recommended_mwk:cleanPrice((low+high)/2),route_used:`${route.From_Landmark} → ${route.To_Landmark}`};
}

function buildDeterministicPricingReply(cleanMessage, context, memory) {
  const ru=context.route_understanding||{}; const computedFare=calculateBasicFare(context);
  const routeIntel=context.tapiwa_intelligence?.route_intelligence?.[0]||null;
  const confirmedRoute=memory?.confirmedRoute||null;
  if (ru?.hasRoute&&ru.confidence==="low") return {category:"pricing_issue",risk_level:"low",internal_summary:"Route unclear",team_message:"I can't lock that route yet — send the pickup and drop-off clearly.",requires_supervisor_approval:false,used_data:{route_understanding:ru,computed_fare:null}};
  if (ru?.hasRoute&&ru.confidence==="medium") { const pn=ru.pickup?.Landmark_Name||ru.pickupRaw; const dn=ru.dropoff?.Landmark_Name||ru.dropoffRaw; return {category:"pricing_issue",risk_level:"low",internal_summary:"Route needs confirmation",team_message:`I think you mean ${pn} to ${dn} — confirm that route before I quote it.`,requires_supervisor_approval:false,used_data:{route_understanding:ru,computed_fare:null}}; }
  if (computedFare) { const routeUsed=computedFare.route_used||routeToLookupMessage(confirmedRoute)||cleanMessage; return {category:"pricing_issue",risk_level:"low",internal_summary:`Quoted ${routeUsed}`,team_message:`For ${routeUsed}, quote around MWK ${Number(computedFare.recommended_mwk||computedFare.estimated_low_mwk).toLocaleString()} — safe range is MWK ${Number(computedFare.estimated_low_mwk).toLocaleString()} to MWK ${Number(computedFare.estimated_high_mwk).toLocaleString()}.`,requires_supervisor_approval:false,used_data:{computed_fare:computedFare,route_understanding:ru}}; }
  if (routeIntel) { const routeUsed=`${routeIntel.pickup_landmark} → ${routeIntel.dropoff_landmark}`; const distText=routeIntel.distance_min_km&&routeIntel.distance_max_km?`${routeIntel.distance_min_km} to ${routeIntel.distance_max_km} km`:null; const pricingText=routeIntel.pricing_notes||"I don't have a locked fare yet."; return {category:"pricing_issue",risk_level:"low",internal_summary:`Used route intelligence for ${routeUsed}`,team_message:distText?`For ${routeUsed}, I'm seeing about ${distText}. ${pricingText}`:`For ${routeUsed}, ${pricingText}`,requires_supervisor_approval:false,used_data:{route_intelligence:[routeIntel],route_understanding:ru}}; }
  if (confirmedRoute) { const lm=routeToLookupMessage(confirmedRoute); return {category:"pricing_issue",risk_level:"low",internal_summary:`No pricing data for ${lm}`,team_message:`I've kept ${lm}. I don't have pricing for it yet — check distance manually before quoting.`,requires_supervisor_approval:false,used_data:{confirmed_route:confirmedRoute,computed_fare:null}}; }
  return null;
}

async function saveAuditLog(payload) {
  await supabaseInsert("tapiwa_ai_audit_logs", { request_type:payload.request_type||"team_chat", user_message:payload.user_message||null, clean_message:payload.clean_message||null, ai_category:payload.ai_category||null, risk_level:payload.risk_level||null, team_message:payload.team_message||null, internal_summary:payload.internal_summary||null, used_data:payload.used_data||null, raw_ai_response:payload.raw_ai_response||null, success:payload.success!==false, error_message:payload.error_message||null, source_type:"ai_server", source_ref:"server.js" });
}

app.post("/ai/analyze", async (req, res) => {
  let cleanMessage = "";
  let context = null;
  let aiResult = {};

  try {
    const { message, senderName = "Dispatcher", senderRole = "Dispatcher", sessionId: rawSessionId } = req.body || {};

    if (!message || !String(message).trim()) return res.status(400).json({ error: "Message is required" });
    if (!hasTapiwaCall(message)) return res.json({ ignored: true, reason: "Tapiwa was not mentioned. Use @Tapiwa to call the AI." });

    cleanMessage = cleanTapiwaMessage(message);
    const sessionId = buildSessionId({ sessionId: rawSessionId, senderName, senderRole });
    const memory = getConversationMemory(sessionId);
    const localRuleOnlyMode = !GROQ_API_KEY;

    // ── FIX 1: Greeting fast-path — before ANY pricing/dispatch logic ─────────
    if (isGreetingOrSocialMessage(cleanMessage)) {
      const replies = [
        "Doing well, ready when you are — what do you need?",
        "All good on my end, what's up?",
        "Hey, I'm here — what do you need from me?",
        "Ready here — just say what you need.",
        "Good, standing by — drop the route or request and I'll sort it."
      ];
      return res.json({ ignored:false, category:"general_update", risk_level:"low", internal_summary:"Social greeting handled", team_message:replies[Math.floor(Math.random()*replies.length)], requires_supervisor_approval:false, used_data:{} });
    }

    // ── FIX 2: Broadened affirmation detection ──────────────────────────────
    const isAffirmation = isAffirmationMessage(cleanMessage);
    const isCorrection = isCorrectionMessage(cleanMessage);
    const isFollowUp = isFollowUpRouteMessage(cleanMessage);
    const hasExplicitRoute = isExplicitRouteMessage(cleanMessage);

    if (isAffirmation && memory.lastAssistTopic && !memory.pendingConfirmation) {
      let teamMessage = "I'm with you. Tell me which part you want help with next and I'll guide you.";
      if (memory.lastAssistTopic==="drivers_tab") teamMessage="Great. Open the Drivers tab first, then tell me what you see and I'll guide you to the next step.";
      else if (memory.lastAssistTopic==="trip_creation") teamMessage="Great. Open the Trips tab, tap the plus button, and I'll guide you through the trip form step by step.";
      else if (memory.lastAssistTopic==="general_onboarding") teamMessage="Great. Tell me whether you want help with trips, drivers, zones, or admin, and I'll guide you.";
      saveConversationMemory(sessionId, memory);
      return res.json({ ignored:false, category:"general_update", risk_level:"low", internal_summary:`Continued guidance for ${memory.lastAssistTopic}`, team_message:teamMessage, requires_supervisor_approval:false, used_data:{assist_topic:memory.lastAssistTopic}, debug_memory:{sessionId,lastRoute:memory.lastRoute,pendingConfirmation:memory.pendingConfirmation,confirmedRoute:memory.confirmedRoute} });
    }

    if (isAffirmation && memory.pendingConfirmation && memory.lastRoute) {
      memory.confirmedRoute = { ...memory.lastRoute };
      memory.pendingConfirmation = false;
      saveConversationMemory(sessionId, memory);

      const pickupName = memory.confirmedRoute.pickup || memory.confirmedRoute.pickupRaw;
      const dropoffName = memory.confirmedRoute.dropoff || memory.confirmedRoute.dropoffRaw;

      // ── FIX 5: If affirmation also contains a pricing intent, quote immediately ──
      const confirmAlsoPricing = isPricingLikeMessage(cleanMessage);
      if (confirmAlsoPricing) {
        const confirmLookup = routeToLookupMessage(memory.confirmedRoute);
        const confirmCtx = await fetchZachanguContext(confirmLookup);
        const confirmFare = calculateBasicFare(confirmCtx);
        let confirmPriceMsg;
        if (confirmFare) {
          confirmPriceMsg = `Confirmed ${pickupName} to ${dropoffName} — should be around MWK ${Number(confirmFare.recommended_mwk||confirmFare.estimated_low_mwk).toLocaleString()}, safe range MWK ${Number(confirmFare.estimated_low_mwk).toLocaleString()} to MWK ${Number(confirmFare.estimated_high_mwk).toLocaleString()}.`;
        } else {
          confirmPriceMsg = `Confirmed ${pickupName} to ${dropoffName} — I don't have pricing for that route yet, check manually before quoting.`;
        }
        await saveAuditLog({ request_type:"team_chat", user_message:message, clean_message:cleanMessage, ai_category:"pricing_issue", risk_level:"low", team_message:confirmPriceMsg, internal_summary:`Route confirmed + priced: ${pickupName} to ${dropoffName}`, used_data:{confirmed_route:memory.confirmedRoute,computed_fare:confirmFare}, raw_ai_response:{fast_path:"affirmation_with_pricing"}, success:true });
        return res.json({ ignored:false, category:"pricing_issue", risk_level:"low", internal_summary:`Route confirmed and priced: ${pickupName} to ${dropoffName}`, team_message:confirmPriceMsg, requires_supervisor_approval:false, used_data:{confirmed_route:memory.confirmedRoute,computed_fare:confirmFare}, debug_memory:{sessionId,lastRoute:memory.lastRoute,pendingConfirmation:memory.pendingConfirmation,confirmedRoute:memory.confirmedRoute} });
      }

      await saveAuditLog({ request_type:"team_chat", user_message:message, clean_message:cleanMessage, ai_category:"pricing_issue", risk_level:"low", team_message:`Confirmed ${pickupName} to ${dropoffName} — what do you need, distance or fare?`, internal_summary:`Route confirmed from memory: ${pickupName} to ${dropoffName}`, used_data:{confirmed_route:memory.confirmedRoute}, raw_ai_response:{fast_path:"affirmation_confirmation"}, success:true });
      return res.json({ ignored:false, category:"pricing_issue", risk_level:"low", internal_summary:`Route confirmed: ${pickupName} to ${dropoffName}`, team_message:`Confirmed ${pickupName} to ${dropoffName} — what do you need, distance or fare?`, requires_supervisor_approval:false, used_data:{confirmed_route:memory.confirmedRoute}, debug_memory:{sessionId,lastRoute:memory.lastRoute,pendingConfirmation:memory.pendingConfirmation,confirmedRoute:memory.confirmedRoute} });
    }

    if (isFollowUp && memory.confirmedRoute && !hasExplicitRoute) {
      const lookupMessage = routeToLookupMessage(memory.confirmedRoute);
      context = await fetchZachanguContext(lookupMessage);
      const computedFare = calculateBasicFare(context);
      saveConversationMemory(sessionId, memory);
      const teamMessage = computedFare
        ? `For ${computedFare.route_used||lookupMessage}, it should be around MWK ${Number(computedFare.recommended_mwk||computedFare.estimated_low_mwk).toLocaleString()} — safe range is MWK ${Number(computedFare.estimated_low_mwk).toLocaleString()} to MWK ${Number(computedFare.estimated_high_mwk).toLocaleString()}.`
        : `I've kept ${lookupMessage}. I don't have pricing for it yet — check distance manually before quoting.`;
      await saveAuditLog({ request_type:"team_chat", user_message:message, clean_message:cleanMessage, ai_category:"pricing_issue", risk_level:"low", team_message:teamMessage, internal_summary:`Used confirmed route from memory: ${lookupMessage}`, used_data:{confirmed_route:memory.confirmedRoute,computed_fare:computedFare}, raw_ai_response:{fast_path:"confirmed_route_follow_up"}, success:true });
      return res.json({ ignored:false, category:"pricing_issue", risk_level:"low", internal_summary:`Used confirmed route ${lookupMessage} for follow-up`, team_message:teamMessage, requires_supervisor_approval:false, used_data:{confirmed_route:memory.confirmedRoute,computed_fare:computedFare}, debug_memory:{sessionId,lastRoute:memory.lastRoute,pendingConfirmation:memory.pendingConfirmation,confirmedRoute:memory.confirmedRoute}, debug_data_counts:context.data_counts, debug_matched_counts:context.matched_counts });
    }

    context = await fetchZachanguContext(cleanMessage);
    const routeUnderstanding = context.route_understanding;

    // ── FIX 3 & 4: Only store snapshot if landmark names resolved cleanly ─────
    let detectedRoute = routeSnapshotFromUnderstanding(routeUnderstanding);
    const topRouteIntel = context.tapiwa_intelligence?.route_intelligence?.[0] || null;
    if (topRouteIntel && routeUnderstanding?.hasRoute && routeIntelMatchesRawRoute(topRouteIntel, routeUnderstanding.pickupRaw, routeUnderstanding.dropoffRaw)) {
      detectedRoute = { pickup:topRouteIntel.pickup_landmark, dropoff:topRouteIntel.dropoff_landmark, pickupRaw:routeUnderstanding.pickupRaw, dropoffRaw:routeUnderstanding.dropoffRaw, confidence:"high" };
      routeUnderstanding.pickup = { Landmark_Name: topRouteIntel.pickup_landmark };
      routeUnderstanding.dropoff = { Landmark_Name: topRouteIntel.dropoff_landmark };
      routeUnderstanding.confidence = "high";
      routeUnderstanding.note = "Route resolved from Tapiwa route intelligence.";
    }

    if (detectedRoute) {
      memory.lastRoute = { ...detectedRoute };
      if (isCorrection) {
        memory.confirmedRoute = detectedRoute.confidence==="high"?{...detectedRoute}:null;
        memory.pendingConfirmation = detectedRoute.confidence==="medium";
      } else if (detectedRoute.confidence==="high") {
        memory.confirmedRoute = { ...detectedRoute };
        memory.pendingConfirmation = false;
      } else if (detectedRoute.confidence==="medium") {
        memory.confirmedRoute = null;
        memory.pendingConfirmation = true;
      } else {
        memory.confirmedRoute = null;
        memory.pendingConfirmation = false;
      }
    }

    saveConversationMemory(sessionId, memory);

    const isPricingRequest = isPricingLikeMessage(cleanMessage) || isFollowUp || (isCorrection && hasExplicitRoute);
    const deterministicPricingReply = isPricingRequest ? buildDeterministicPricingReply(cleanMessage, context, memory) : null;
    const deterministicDispatchReply = !isPricingRequest ? buildDeterministicDispatchReply(cleanMessage, memory) : null;
    const computedFare = deterministicPricingReply?.used_data?.computed_fare || calculateBasicFare(context);
    const hasAnyPricingData = context.tapiwa_intelligence.market_price_rules.length>0||context.tapiwa_intelligence.route_intelligence.length>0||context.tapiwa_intelligence.route_learning.length>0||context.market_prices.length>0||context.route_matrix.length>0||context.pricing_rules.length>0||computedFare;

    let forcedMessage = null;
    if (isPricingRequest&&routeUnderstanding?.hasRoute&&routeUnderstanding.confidence==="low") forcedMessage="I can't lock that route yet — send the pickup and drop-off clearly before I quote it.";
    else if (isPricingRequest&&routeUnderstanding?.hasRoute&&routeUnderstanding.confidence==="medium") { const pn=routeUnderstanding.pickup?.Landmark_Name||routeUnderstanding.pickupRaw; const dn=routeUnderstanding.dropoff?.Landmark_Name||routeUnderstanding.dropoffRaw; forcedMessage=`I think you mean ${pn} to ${dn} — confirm that route before I quote it.`; }
    else if (isPricingRequest&&!hasAnyPricingData) forcedMessage="Hmm, I don't have pricing for that route yet — check the distance manually or ask someone who's done that run before quoting.";

    if (deterministicPricingReply) {
      const responsePayload = { ignored:false, ...deterministicPricingReply, debug_memory:{sessionId,lastRoute:memory.lastRoute,pendingConfirmation:memory.pendingConfirmation,confirmedRoute:memory.confirmedRoute}, debug_data_counts:context.data_counts, debug_matched_counts:context.matched_counts };
      await saveAuditLog({ request_type:"team_chat", user_message:message, clean_message:cleanMessage, ai_category:responsePayload.category, risk_level:responsePayload.risk_level, team_message:responsePayload.team_message, internal_summary:responsePayload.internal_summary, used_data:responsePayload.used_data, raw_ai_response:{deterministic:true}, success:true });
      return res.json(responsePayload);
    }

    if (deterministicDispatchReply) {
      if (!hasExplicitRoute) { memory.pendingConfirmation=false; memory.lastRoute=null; }
      saveConversationMemory(sessionId, memory);
      const responsePayload = { ignored:false, ...deterministicDispatchReply, debug_memory:{sessionId,lastRoute:memory.lastRoute,pendingConfirmation:memory.pendingConfirmation,confirmedRoute:memory.confirmedRoute}, debug_data_counts:context.data_counts, debug_matched_counts:context.matched_counts };
      await saveAuditLog({ request_type:"team_chat", user_message:message, clean_message:cleanMessage, ai_category:responsePayload.category, risk_level:responsePayload.risk_level, team_message:responsePayload.team_message, internal_summary:responsePayload.internal_summary, used_data:responsePayload.used_data, raw_ai_response:{deterministic_dispatch:true}, success:true });
      return res.json(responsePayload);
    }

    const systemPrompt = `You are Dispatch Tapiwa, a real person on the Zachangu dispatch team in Lilongwe, Malawi.
You help the crew with operations, pricing, safety, and dispatch decisions.
Sound human, calm, short, useful. Not a report. Not a robot.
PERSONALITY: Friendly but controlled. Teammate, not chatbot. 1-2 sentences max. No bullets, no formal labels. Understand typos and low-literacy phrasing. Mixed Chichewa+English is fine. Never criticize grammar.
ROUTE MEMORY RULES: Respect confirmed_route. If confirmed, do not guess a different route. If pending_confirmation and dispatcher says yes, treat last route as confirmed. Medium confidence → ask confirmation. Low confidence → ask for clearer pickup/dropoff.
PRICING: Min MWK 3,000. Round to nearest MWK 100. Priority: tapiwa intelligence → market_prices → computed_fare. Dispatcher decides final price.
SAFETY: Never say driver dispatched unless pickup is clear. If vague, ask short clarification. If off-topic, redirect to dispatch.
category: incident|pricing_issue|driver_issue|traffic|system_issue|general_update
risk_level: low|medium|high
JSON only: {"category":"","risk_level":"","internal_summary":"","team_message":"","requires_supervisor_approval":false,"used_data":{"zones":[],"landmarks":[],"pricing_rules":[],"market_prices":[],"route_matrix":[],"tapiwa_market_price_rules":[],"tapiwa_route_intelligence":[],"tapiwa_zone_behavior":[],"tapiwa_route_learning":[],"computed_fare":null}}`;

    if (!forcedMessage && !localRuleOnlyMode) {
      // ── FIX 6: Groq timeout increased from 5000ms to 14000ms ────────────────
      const groqResponse = await fetchWithTimeout(
        "https://api.groq.com/openai/v1/chat/completions",
        { method:"POST", headers:{Authorization:`Bearer ${GROQ_API_KEY}`,"Content-Type":"application/json"}, body:JSON.stringify({ model:GROQ_MODEL, messages:[{role:"system",content:systemPrompt},{role:"user",content:JSON.stringify({sender:`${senderName} (${senderRole})`,session_id:sessionId,msg:cleanMessage,conversation_memory:{lastRoute:memory.lastRoute,pendingConfirmation:memory.pendingConfirmation,confirmedRoute:memory.confirmedRoute},ctx:{route_understanding:context.route_understanding,zones:context.zones,landmarks:context.landmarks,pricing_rules:context.pricing_rules,market_prices:context.market_prices,route_matrix:context.route_matrix,risks:context.risks,tapiwa_intelligence:context.tapiwa_intelligence},fare:computedFare})}], response_format:{type:"json_object"}, temperature:0.2, max_tokens:400 }) },
        14000   // was 5000 — caused repeated "Hmm, something went off on my side"
      );

      const groqData = await groqResponse.json();
      if (!groqResponse.ok) {
        console.error("GROQ API ERROR:", groqResponse.status, groqData);
        await saveAuditLog({ request_type:"team_chat", user_message:message, clean_message:cleanMessage, success:false, error_message:"Groq API error", raw_ai_response:groqData });
        return res.json(buildTemporaryTapiwaFallback());
      }
      try { aiResult = JSON.parse(groqData.choices?.[0]?.message?.content || "{}"); } catch { aiResult = {}; }
    }

    const allowedCategories = ["incident","pricing_issue","driver_issue","traffic","system_issue","general_update"];
    const allowedRiskLevels = ["low","medium","high"];
    let category = aiResult.category || "general_update";
    if (!allowedCategories.includes(category)) { const lower=cleanMessage.toLowerCase(); if(/price|fare|cost|charge|quote|how much|km|distance/.test(lower)) category="pricing_issue"; else if(/robbed|accident|threat|violence|attack|stolen|drunk|refuse/.test(lower)) category="incident"; else if(/driver/.test(lower)) category="driver_issue"; else if(/traffic|rain|roadblock|police|jam/.test(lower)) category="traffic"; else category="general_update"; }
    let riskLevel = aiResult.risk_level || "low";
    if (!allowedRiskLevels.includes(riskLevel)) riskLevel = "low";

    let fallbackMessage = "Alright team, noted — let's handle this and keep things moving.";
    if (category==="pricing_issue") {
      if (routeUnderstanding?.hasRoute&&routeUnderstanding.confidence==="low") fallbackMessage="I can't lock that route yet — send pickup and drop-off clearly.";
      else if (routeUnderstanding?.hasRoute&&routeUnderstanding.confidence==="medium") { const pn=routeUnderstanding.pickup?.Landmark_Name||routeUnderstanding.pickupRaw; const dn=routeUnderstanding.dropoff?.Landmark_Name||routeUnderstanding.dropoffRaw; fallbackMessage=`I think you mean ${pn} to ${dn} — confirm that route before I quote it.`; }
      else if (computedFare) fallbackMessage=`Yeah, for ${computedFare.route_used} it should be around MWK ${Number(computedFare.recommended_mwk||computedFare.estimated_low_mwk).toLocaleString()} — safe range is MWK ${Number(computedFare.estimated_low_mwk).toLocaleString()} to MWK ${Number(computedFare.estimated_high_mwk).toLocaleString()}.`;
      else fallbackMessage="Hmm, I don't have pricing for that route yet — check the distance manually or ask someone who's done that run before quoting.";
    }

    const responsePayload = {
      ignored:false, category, risk_level:riskLevel, internal_summary:aiResult.internal_summary||cleanMessage,
      team_message:forcedMessage||aiResult.team_message||fallbackMessage,
      requires_supervisor_approval:riskLevel==="high"||aiResult.requires_supervisor_approval===true,
      used_data:aiResult.used_data||{zones:context.zones.map(z=>z.Zone_ID||z.Zone_Name).filter(Boolean),landmarks:context.landmarks.map(l=>l.Landmark_Name).filter(Boolean),pricing_rules:context.pricing_rules.map(p=>p.Vehicle_Type).filter(Boolean),market_prices:context.market_prices.map(m=>m.Route_ID).filter(Boolean),route_matrix:context.route_matrix.map(r=>r.Route_Key).filter(Boolean),tapiwa_market_price_rules:context.tapiwa_intelligence.market_price_rules.map(r=>r.pickup_landmark||r.pickup_zone).filter(Boolean),tapiwa_route_intelligence:context.tapiwa_intelligence.route_intelligence.map(r=>r.route_name).filter(Boolean),tapiwa_zone_behavior:context.tapiwa_intelligence.zone_behavior.map(z=>z.zone_code).filter(Boolean),tapiwa_route_learning:context.tapiwa_intelligence.route_learning.map(r=>`${r.pickup_landmark} → ${r.dropoff_landmark}`).filter(Boolean),computed_fare:computedFare},
      debug_memory:{sessionId,lastRoute:memory.lastRoute,pendingConfirmation:memory.pendingConfirmation,confirmedRoute:memory.confirmedRoute},
      debug_data_counts:context.data_counts, debug_matched_counts:context.matched_counts
    };
    if (!responsePayload.team_message) responsePayload.team_message="Alright, checking that — confirm pickup and I'll guide you.";
    await saveAuditLog({ request_type:"team_chat", user_message:message, clean_message:cleanMessage, ai_category:responsePayload.category, risk_level:responsePayload.risk_level, team_message:responsePayload.team_message, internal_summary:responsePayload.internal_summary, used_data:responsePayload.used_data, raw_ai_response:aiResult, success:true });
    return res.json(responsePayload);

  } catch (error) {
    console.error("AI ERROR:", error);
    await saveAuditLog({ request_type:"team_chat", user_message:req.body?.message||null, clean_message:cleanMessage, used_data:context, raw_ai_response:aiResult, success:false, error_message:error.message });
    return res.json(buildTemporaryTapiwaFallback());
  }
});

const port = process.env.PORT || 3001;
app.listen(port, "0.0.0.0", () => { console.log(`Zachangu AI server running on port ${port}`); });
