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

const ORS_API_KEY = process.env.ORS_API_KEY;
const PORT = process.env.PORT || 3001;

const tableDebug = {};
const conversationMemory = {};

// =========================
// BASIC HELPERS
// =========================

function cleanBaseUrl() {
  return String(SUPABASE_URL || "")
    .replace(/\/rest\/v1\/?$/, "")
    .replace(/\/$/, "");
}

function normalizeText(value) {
  return String(value || "")
    .toLowerCase()
    .replace(/[^\w\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function roundToNearest100(value) {
  return Math.round(Number(value || 0) / 100) * 100;
}

function cleanPrice(value) {
  return Math.max(3000, roundToNearest100(value));
}

function money(value) {
  return `MWK ${Number(cleanPrice(value)).toLocaleString("en-US")}`;
}

function hasTapiwaCall(message) {
  return /@tapiwa/i.test(String(message || ""));
}

function cleanTapiwaMessage(message) {
  return String(message || "").replace(/@tapiwa/gi, "").trim();
}

function isPricingIntent(message) {
  const text = normalizeText(message);

  return (
    /price|fare|cost|charge|quote/.test(text) ||
    /how much|hw much|hw mch|how mch|hwmuch/.test(text) ||
    /km|kms|kilometer|kilometre|distance/.test(text) ||
    /\bfrom\b.+\bto\b/.test(text) ||
    /\bto\b/.test(text)
  );
}

function isIncidentIntent(message) {
  const text = normalizeText(message);

  return /drunk|refus|pay|accident|fight|robbery|stolen|police|danger|threat|violence|emergency|attack/.test(
    text
  );
}

function isConfirmation(message) {
  const text = normalizeText(message);
  return /yes|yeah|yep|correct|that one|go ahead|confirmed|confirm/.test(text);
}

function isCorrection(message) {
  const text = normalizeText(message);
  return /no|wrong|i meant|meant|not that|correction|correct it/.test(text);
}

// =========================
// SUPABASE REST HELPERS
// =========================

async function supabaseFetch(table, limit = 50, query = "") {
  if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
    tableDebug[table] = {
      ok: false,
      reason: "Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY"
    };
    return [];
  }

  const url = `${cleanBaseUrl()}/rest/v1/${table}?select=*&limit=${limit}${query}`;

  try {
    const response = await fetch(url, {
      method: "GET",
      headers: {
        apikey: SUPABASE_SERVICE_ROLE_KEY,
        Authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
        "Content-Type": "application/json"
      }
    });

    const text = await response.text();

    if (!response.ok) {
      tableDebug[table] = {
        ok: false,
        status: response.status,
        error: text
      };
      return [];
    }

    const json = JSON.parse(text);

    tableDebug[table] = {
      ok: true,
      status: response.status,
      rows_loaded: Array.isArray(json) ? json.length : 0
    };

    return Array.isArray(json) ? json : [];
  } catch (error) {
    tableDebug[table] = {
      ok: false,
      error: error.message
    };
    return [];
  }
}

async function supabaseInsert(table, payload) {
  if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) return null;

  const url = `${cleanBaseUrl()}/rest/v1/${table}`;

  try {
    const response = await fetch(url, {
      method: "POST",
      headers: {
        apikey: SUPABASE_SERVICE_ROLE_KEY,
        Authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
        "Content-Type": "application/json",
        Prefer: "return=representation"
      },
      body: JSON.stringify(payload)
    });

    const text = await response.text();

    if (!response.ok) {
      tableDebug[table] = {
        ok: false,
        status: response.status,
        insert_error: text
      };
      return null;
    }

    return text ? JSON.parse(text) : null;
  } catch (error) {
    tableDebug[table] = {
      ok: false,
      insert_error: error.message
    };
    return null;
  }
}

// =========================
// FUZZY MATCHING
// =========================

function levenshtein(a, b) {
  a = normalizeText(a);
  b = normalizeText(b);

  const matrix = Array.from({ length: a.length + 1 }, () =>
    Array(b.length + 1).fill(0)
  );

  for (let i = 0; i <= a.length; i++) matrix[i][0] = i;
  for (let j = 0; j <= b.length; j++) matrix[0][j] = j;

  for (let i = 1; i <= a.length; i++) {
    for (let j = 1; j <= b.length; j++) {
      const cost = a[i - 1] === b[j - 1] ? 0 : 1;
      matrix[i][j] = Math.min(
        matrix[i - 1][j] + 1,
        matrix[i][j - 1] + 1,
        matrix[i - 1][j - 1] + cost
      );
    }
  }

  return matrix[a.length][b.length];
}

function similarity(a, b) {
  a = normalizeText(a);
  b = normalizeText(b);

  if (!a || !b) return 0;
  if (a === b) return 1;
  if (a.includes(b) || b.includes(a)) return 0.9;

  const distance = levenshtein(a, b);
  const maxLength = Math.max(a.length, b.length);

  return 1 - distance / maxLength;
}

function getKeywords(message) {
  const stopWords = new Set([
    "price",
    "fare",
    "cost",
    "from",
    "to",
    "the",
    "for",
    "please",
    "give",
    "estimate",
    "route",
    "how",
    "much",
    "should",
    "we",
    "charge",
    "what",
    "is",
    "it",
    "that",
    "trip"
  ]);

  return normalizeText(message)
    .split(" ")
    .filter((word) => word.length > 1 && !stopWords.has(word));
}

function rowText(row) {
  return normalizeText(Object.values(row || {}).filter(Boolean).join(" "));
}

function scoreRow(row, keywords) {
  const text = rowText(row);
  let score = 0;

  for (const word of keywords) {
    if (text.includes(word)) score += 1;
  }

  return score;
}

function topMatches(rows, keywords, limit = 5) {
  return rows
    .map((row) => ({ row, score: scoreRow(row, keywords) }))
    .filter((item) => item.score > 0)
    .sort((a, b) => b.score - a.score)
    .slice(0, limit)
    .map((item) => item.row);
}

// =========================
// ROUTE UNDERSTANDING
// =========================

function extractRouteParts(message) {
  const text = normalizeText(message);

  let match = text.match(/from (.+?) to (.+)/);
  if (match) {
    return {
      pickupRaw: match[1].trim(),
      dropoffRaw: match[2].trim()
    };
  }

  match = text.match(/(.+?) to (.+)/);
  if (match && isPricingIntent(text)) {
    return {
      pickupRaw: match[1]
        .replace(
          /price|fare|cost|charge|quote|how much|hw much|hw mch|how mch|km|kms|kilometer|kilometre|distance/g,
          ""
        )
        .trim(),
      dropoffRaw: match[2].trim()
    };
  }

  return null;
}

function getLandmarkName(row) {
  return (
    row?.Landmark_Name ||
    row?.landmark_name ||
    row?.name ||
    row?.pickup_landmark ||
    row?.dropoff_landmark ||
    ""
  );
}

function getZoneId(row) {
  return row?.Zone_ID || row?.zone_id || row?.zone_code || row?.Zone || null;
}

function getLatitude(row) {
  return Number(
    row?.latitude ??
      row?.Latitude ??
      row?.lat ??
      row?.Lat ??
      row?.Pickup_Latitude ??
      row?.Dropoff_Latitude
  );
}

function getLongitude(row) {
  return Number(
    row?.longitude ??
      row?.Longitude ??
      row?.lng ??
      row?.Lon ??
      row?.long ??
      row?.Pickup_Longitude ??
      row?.Dropoff_Longitude
  );
}

function hasCoords(row) {
  return Number.isFinite(getLatitude(row)) && Number.isFinite(getLongitude(row));
}

function bestLandmarkMatch(rawName, landmarks) {
  if (!rawName || !Array.isArray(landmarks)) return { match: null, score: 0 };

  const candidates = landmarks.map((lm) => {
    const names = [
      lm.Landmark_Name,
      lm.landmark_name,
      lm.name,
      lm.Area,
      lm.area,
      lm.Nearby_Landmarks,
      lm.nearby_landmarks
    ].filter(Boolean);

    let bestScore = 0;

    for (const name of names) {
      const score = similarity(rawName, name);
      if (score > bestScore) bestScore = score;
    }

    return { landmark: lm, score: bestScore };
  });

  candidates.sort((a, b) => b.score - a.score);

  return {
    match: candidates[0]?.landmark || null,
    score: candidates[0]?.score || 0
  };
}

function buildRouteUnderstanding(message, landmarksRaw, memory = {}) {
  const text = normalizeText(message);

  if (
    memory.confirmedRoute &&
    /that route|that trip|same route|it|how much|price|fare|quote/.test(text) &&
    !extractRouteParts(message)
  ) {
    return {
      hasRoute: true,
      confidence: "high",
      pickup: memory.confirmedRoute.pickup,
      dropoff: memory.confirmedRoute.dropoff,
      pickupRaw: memory.confirmedRoute.pickupRaw,
      dropoffRaw: memory.confirmedRoute.dropoffRaw,
      note: "Using confirmed route from memory"
    };
  }

  const parts = extractRouteParts(message);

  if (!parts) {
    return {
      hasRoute: false,
      confidence: "low",
      pickup: null,
      dropoff: null,
      note: "No clear pickup/dropoff detected."
    };
  }

  const pickupMatch = bestLandmarkMatch(parts.pickupRaw, landmarksRaw);
  const dropoffMatch = bestLandmarkMatch(parts.dropoffRaw, landmarksRaw);
  const avgScore = (pickupMatch.score + dropoffMatch.score) / 2;

  let confidence = "low";
  if (avgScore >= 0.78) confidence = "high";
  else if (avgScore >= 0.52) confidence = "medium";

  return {
    hasRoute: true,
    confidence,
    pickupRaw: parts.pickupRaw,
    dropoffRaw: parts.dropoffRaw,
    pickup: pickupMatch.match,
    dropoff: dropoffMatch.match,
    pickup_score: pickupMatch.score,
    dropoff_score: dropoffMatch.score,
    note:
      confidence === "high"
        ? "Route understood clearly."
        : confidence === "medium"
        ? "Route partly understood; needs confirmation."
        : "Route unclear; ask for clarification."
  };
}

// =========================
// ORS DISTANCE
// =========================

async function getOrsDistanceKm(pickup, dropoff) {
  if (!ORS_API_KEY || !hasCoords(pickup) || !hasCoords(dropoff)) {
    return null;
  }

  const body = {
    coordinates: [
      [getLongitude(pickup), getLatitude(pickup)],
      [getLongitude(dropoff), getLatitude(dropoff)]
    ]
  };

  try {
    const response = await fetch(
      "https://api.openrouteservice.org/v2/directions/driving-car",
      {
        method: "POST",
        headers: {
          Authorization: ORS_API_KEY,
          "Content-Type": "application/json"
        },
        body: JSON.stringify(body)
      }
    );

    const json = await response.json();

    if (!response.ok) return null;

    const meters = json?.routes?.[0]?.summary?.distance;
    const seconds = json?.routes?.[0]?.summary?.duration;

    if (!Number.isFinite(meters)) return null;

    return {
      distance_km: Number((meters / 1000).toFixed(2)),
      duration_min: Number((seconds / 60).toFixed(0)),
      source: "ORS"
    };
  } catch {
    return null;
  }
}

// =========================
// DATA FETCHING FOR QUOTES
// =========================

async function fetchQuoteData(message, memory = {}) {
  const landmarksRaw = await supabaseFetch("landmarks", 500);
  const routeUnderstanding = buildRouteUnderstanding(message, landmarksRaw, memory);

  const [
    systemSettings,
    pricingRules,
    marketRules,
    routeIntel,
    routeLearning,
    zoneBehavior
  ] = await Promise.all([
    supabaseFetch("tapiwa_system_settings", 50),
    supabaseFetch("pricing_rules", 50),
    supabaseFetch("tapiwa_market_price_rules", 250),
    supabaseFetch("tapiwa_route_intelligence", 250),
    supabaseFetch("tapiwa_route_learning", 250),
    supabaseFetch("tapiwa_zone_behavior", 50)
  ]);

  return {
    landmarksRaw,
    routeUnderstanding,
    systemSettings,
    pricingRules,
    marketRules,
    routeIntel,
    routeLearning,
    zoneBehavior
  };
}

function findMatchingMarketRule(route, rules) {
  if (!route?.pickup || !route?.dropoff) return null;

  const pickupName = normalizeText(getLandmarkName(route.pickup));
  const dropName = normalizeText(getLandmarkName(route.dropoff));
  const pickupZone = normalizeText(getZoneId(route.pickup));
  const dropZone = normalizeText(getZoneId(route.dropoff));

  let best = null;
  let bestScore = 0;

  for (const rule of rules || []) {
    if (rule.active === false) continue;

    let score = 0;

    if (rule.pickup_landmark && similarity(pickupName, rule.pickup_landmark) > 0.75)
      score += 3;
    if (rule.dropoff_landmark && similarity(dropName, rule.dropoff_landmark) > 0.75)
      score += 3;
    if (rule.pickup_zone && normalizeText(rule.pickup_zone) === pickupZone) score += 1;
    if (rule.dropoff_zone && normalizeText(rule.dropoff_zone) === dropZone) score += 1;

    if (!rule.pickup_landmark && !rule.dropoff_landmark && rule.source_type === "system_default") {
      score += 0.2;
    }

    if (score > bestScore) {
      best = rule;
      bestScore = score;
    }
  }

  return bestScore >= 3 ? best : null;
}

function findPricingRule(pricingRules, vehicleType = "motorbike") {
  const vt = normalizeText(vehicleType);

  return (
    pricingRules.find((r) =>
      normalizeText(r.Vehicle_Type || r.vehicle_type).includes(vt)
    ) ||
    pricingRules.find((r) =>
      normalizeText(r.Vehicle_Type || r.vehicle_type).includes("motorbike")
    ) ||
    pricingRules[0] ||
    null
  );
}

function getRateFromRule(rule) {
  return Number(
    rule?.Base_Rate_Per_KM_MWK ??
      rule?.base_rate_per_km_mwk ??
      rule?.rate_per_km ??
      1500
  );
}

function getMinimumFareFromRule(rule) {
  return Number(
    rule?.Minimum_Fare_MWK ??
      rule?.minimum_fare_mwk ??
      rule?.minimum_fare ??
      3000
  );
}

function getMultiplier({ peak, night, rain, event }) {
  let multiplier = 1;

  if (peak) multiplier *= 1.2;
  if (night) multiplier *= 1.2;
  if (rain) multiplier *= 1.25;
  if (event) multiplier *= 1.15;

  return Number(multiplier.toFixed(2));
}

function localFallbackDistanceEstimate(route) {
  const pz = normalizeText(getZoneId(route.pickup));
  const dz = normalizeText(getZoneId(route.dropoff));

  if (!route?.pickup || !route?.dropoff) return null;

  if (pz && dz && pz === dz) return 4;
  if ((pz === "z1" && dz === "z2") || (pz === "z2" && dz === "z1")) return 8;
  if ((pz === "z1" && dz === "z3") || (pz === "z3" && dz === "z1")) return 10;
  if ((pz === "z3" && dz === "z4") || (pz === "z4" && dz === "z3")) return 9;
  if ((pz === "z3" && dz === "z5") || (pz === "z5" && dz === "z3")) return 14;

  return 8;
}

async function calculateTripQuote({
  message,
  vehicleType = "motorbike",
  peak = false,
  night = false,
  rain = false,
  event = false,
  manualDistanceKm = null,
  memory = {}
}) {
  const data = await fetchQuoteData(message, memory);
  const route = data.routeUnderstanding;

  if (!route.hasRoute) {
    return {
      ok: false,
      mode: "clarify",
      reason: "No route detected",
      team_message: "Send pickup and drop-off clearly so I can quote it."
    };
  }

  if (route.confidence === "low") {
    return {
      ok: false,
      mode: "clarify",
      reason: "Low route confidence",
      route,
      team_message: "I can’t lock that route yet — send pickup and drop-off clearly."
    };
  }

  if (route.confidence === "medium") {
    return {
      ok: false,
      mode: "confirm",
      route,
      team_message: `I think you mean ${getLandmarkName(route.pickup) || route.pickupRaw} to ${
        getLandmarkName(route.dropoff) || route.dropoffRaw
      }, confirm?`
    };
  }

  const pricingRule = findPricingRule(data.pricingRules, vehicleType);
  const marketRule = findMatchingMarketRule(route, data.marketRules);

  let distanceData = null;

  if (manualDistanceKm) {
    distanceData = {
      distance_km: Number(manualDistanceKm),
      duration_min: null,
      source: "manual"
    };
  } else {
    distanceData = await getOrsDistanceKm(route.pickup, route.dropoff);
  }

  if (!distanceData) {
    distanceData = {
      distance_km: localFallbackDistanceEstimate(route),
      duration_min: null,
      source: "zone_estimate"
    };
  }

  const distanceKm = Number(distanceData.distance_km);
  const rate = getRateFromRule(pricingRule);
  const minimumFare = Math.max(getMinimumFareFromRule(pricingRule), 3000);
  const multiplier = getMultiplier({ peak, night, rain, event });

  const rawBase = Math.max(distanceKm * rate, minimumFare);
  const systemPrice = cleanPrice(rawBase * multiplier);

  let rangeMin = cleanPrice(systemPrice * 0.92);
  let rangeMax = cleanPrice(systemPrice * 1.08);
  let recommended = systemPrice;

  let marketNote = null;

  if (marketRule) {
    const marketMin = cleanPrice(marketRule.min_price);
    const marketMax = cleanPrice(marketRule.max_price);
    const marketRecommended = cleanPrice(
      marketRule.recommended_price || (marketMin + marketMax) / 2
    );

    const marketTooLow =
      marketMax < systemPrice * 0.65 && distanceKm >= 10;

    if (!marketTooLow) {
      recommended = cleanPrice((systemPrice * 0.75 + marketRecommended * 0.25));
      rangeMin = cleanPrice(Math.min(rangeMin, marketMin));
      rangeMax = cleanPrice(Math.max(rangeMax, marketMax));
      marketNote = "Market data considered.";
    } else {
      marketNote =
        "Market data looks too low for this distance/condition, system economics used.";
    }
  }

  const conditionLabels = [];
  if (peak) conditionLabels.push("peak");
  if (night) conditionLabels.push("night");
  if (rain) conditionLabels.push("rain");
  if (event) conditionLabels.push("event");

  const confidence = distanceData.source === "ORS" ? "high" : "medium";

  const result = {
    ok: true,
    mode: "priced",
    pickup: getLandmarkName(route.pickup) || route.pickupRaw,
    dropoff: getLandmarkName(route.dropoff) || route.dropoffRaw,
    pickup_zone: getZoneId(route.pickup),
    dropoff_zone: getZoneId(route.dropoff),
    distance_km: distanceKm,
    duration_min: distanceData.duration_min,
    distance_source: distanceData.source,
    vehicle_type: vehicleType,
    rate_per_km: rate,
    minimum_fare: minimumFare,
    multiplier,
    conditions: conditionLabels,
    raw_system_price: rawBase,
    tapiwa_recommended_price: recommended,
    final_price_default: recommended,
    range_min: rangeMin,
    range_max: rangeMax,
    confidence,
    market_note: marketNote,
    team_message: `Should be around ${money(recommended)} — safe range ${money(
      rangeMin
    )} to ${money(rangeMax)}${
      night ? ", night rate included" : ""
    }.`
  };

  const inserted = await supabaseInsert("tapiwa_pricing_decisions", {
    pickup_landmark: result.pickup,
    dropoff_landmark: result.dropoff,
    pickup_zone: result.pickup_zone,
    dropoff_zone: result.dropoff_zone,
    distance_km: result.distance_km,
    vehicle_type: result.vehicle_type,
    time_mode: night ? "night" : peak ? "peak" : "normal",
    weather_mode: rain ? "rain" : "normal",
    raw_ai_price: result.raw_system_price,
    tapiwa_recommended_price: result.tapiwa_recommended_price,
    range_min: result.range_min,
    range_max: result.range_max,
    confidence: result.confidence,
    reason: result.market_note || "System pricing engine used.",
    dispatcher_script: result.team_message,
    source_type: "system_calculated",
    source_ref: "server.js v3"
  });

  result.pricing_decision_id = inserted?.[0]?.id || null;

  return result;
}

// =========================
// GROQ HUMAN VOICE
// =========================

async function groqHumanize(summary) {
  if (!GROQ_API_KEY) return summary.team_message;

  try {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 5000);

    const response = await fetch("https://api.groq.com/openai/v1/chat/completions", {
      method: "POST",
      signal: controller.signal,
      headers: {
        Authorization: `Bearer ${GROQ_API_KEY}`,
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        model: GROQ_MODEL,
        messages: [
          {
            role: "system",
            content: `
You are Dispatch Tapiwa in Zachangu Ops Room.

Rewrite the provided answer into one short human team-chat message.

Rules:
- Keep the exact price numbers.
- Do not calculate new prices.
- Do not add new facts.
- Do not repeat "confirm pickup" unless necessary.
- Sound calm, human, and operational.
- Maximum 2 sentences.
            `.trim()
          },
          {
            role: "user",
            content: JSON.stringify(summary)
          }
        ],
        temperature: 0.35,
        max_tokens: 120
      })
    });

    clearTimeout(timeout);

    const json = await response.json();
    if (!response.ok) return summary.team_message;

    return json.choices?.[0]?.message?.content?.trim() || summary.team_message;
  } catch {
    return summary.team_message;
  }
}

// =========================
// ROUTES
// =========================

app.get("/", (req, res) => {
  res.json({
    status: "Zachangu AI server v3 is running",
    groq_ready: Boolean(GROQ_API_KEY),
    supabase_ready: Boolean(SUPABASE_URL && SUPABASE_SERVICE_ROLE_KEY),
    ors_ready: Boolean(ORS_API_KEY),
    port: PORT
  });
});

app.get("/debug-tables", async (req, res) => {
  const tables = [
    "zones",
    "landmarks",
    "pricing_rules",
    "market_prices",
    "route_matrix",
    "risks",
    "drivers",
    "trip_requests",
    "tapiwa_system_settings",
    "tapiwa_market_price_rules",
    "tapiwa_route_intelligence",
    "tapiwa_zone_behavior",
    "tapiwa_route_learning",
    "tapiwa_price_outcomes",
    "tapiwa_ai_audit_logs",
    "tapiwa_pricing_decisions"
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

app.post("/api/quote", async (req, res) => {
  try {
    const quote = await calculateTripQuote({
      message: `${req.body.pickup || ""} to ${req.body.dropoff || ""}`,
      vehicleType: req.body.vehicleType || req.body.vehicle_type || "motorbike",
      peak: Boolean(req.body.peak),
      night: Boolean(req.body.night),
      rain: Boolean(req.body.rain),
      event: Boolean(req.body.event),
      manualDistanceKm: req.body.distanceKm || req.body.distance_km || null,
      memory: {}
    });

    return res.json(quote);
  } catch (error) {
    return res.json({
      ok: false,
      mode: "system_issue",
      team_message: "Something went off while pricing — try again.",
      error: error.message
    });
  }
});

app.post("/ai/analyze", async (req, res) => {
  let cleanMessage = "";

  try {
    const {
      message,
      senderName = "Dispatcher",
      senderRole = "Dispatcher",
      sessionId = "default",
      peak = false,
      night = false,
      rain = false,
      event = false,
      vehicleType = "motorbike"
    } = req.body || {};

    if (!message || !String(message).trim()) {
      return res.status(400).json({ error: "Message is required" });
    }

    if (!hasTapiwaCall(message)) {
      return res.json({
        ignored: true,
        reason: "Tapiwa was not mentioned. Use @Tapiwa to call the AI."
      });
    }

    cleanMessage = cleanTapiwaMessage(message);
    const memory = conversationMemory[sessionId] || {};

    if (isIncidentIntent(cleanMessage)) {
      const team_message =
        "Hold on that one, involve the supervisor before sending anyone.";

      await supabaseInsert("tapiwa_ai_audit_logs", {
        request_type: "team_chat",
        user_message: message,
        clean_message: cleanMessage,
        ai_category: "incident",
        risk_level: "high",
        team_message,
        internal_summary: cleanMessage,
        used_data: {},
        raw_ai_response: {},
        success: true,
        source_type: "ai_server",
        source_ref: "server.js v3"
      });

      return res.json({
        ignored: false,
        category: "incident",
        risk_level: "high",
        internal_summary: cleanMessage,
        team_message,
        requires_supervisor_approval: true,
        used_data: {}
      });
    }

    if (isPricingIntent(cleanMessage) || isConfirmation(cleanMessage)) {
      const quote = await calculateTripQuote({
        message: cleanMessage,
        vehicleType,
        peak,
        night,
        rain,
        event,
        memory
      });

      if (quote.mode === "confirm") {
        memory.pendingRoute = {
          pickup: quote.route.pickup,
          dropoff: quote.route.dropoff,
          pickupRaw: quote.route.pickupRaw,
          dropoffRaw: quote.route.dropoffRaw
        };
        conversationMemory[sessionId] = memory;
      }

      if (quote.mode === "priced") {
        memory.confirmedRoute = {
          pickup: quote.pickup,
          dropoff: quote.dropoff,
          pickupRaw: quote.pickup,
          dropoffRaw: quote.dropoff
        };
        conversationMemory[sessionId] = memory;
      }

      if (isConfirmation(cleanMessage) && memory.pendingRoute) {
        memory.confirmedRoute = memory.pendingRoute;
        memory.pendingRoute = null;
        conversationMemory[sessionId] = memory;

        return res.json({
          ignored: false,
          category: "pricing_issue",
          risk_level: "low",
          internal_summary: "Route confirmed",
          team_message: `${getLandmarkName(memory.confirmedRoute.pickup) || memory.confirmedRoute.pickupRaw} to ${
            getLandmarkName(memory.confirmedRoute.dropoff) || memory.confirmedRoute.dropoffRaw
          }, confirmed route.`,
          requires_supervisor_approval: false,
          used_data: { memory: memory.confirmedRoute }
        });
      }

      let teamMessage = quote.team_message;

      if (quote.ok) {
        teamMessage = await groqHumanize({
          intent: "pricing",
          pickup: quote.pickup,
          dropoff: quote.dropoff,
          distance_km: quote.distance_km,
          recommended_price: quote.tapiwa_recommended_price,
          range_min: quote.range_min,
          range_max: quote.range_max,
          conditions: quote.conditions,
          confidence: quote.confidence,
          team_message: quote.team_message
        });
      }

      await supabaseInsert("tapiwa_ai_audit_logs", {
        request_type: "team_chat",
        user_message: message,
        clean_message: cleanMessage,
        ai_category: "pricing_issue",
        risk_level: "low",
        team_message: teamMessage,
        internal_summary: cleanMessage,
        used_data: quote,
        raw_ai_response: {},
        success: true,
        source_type: "ai_server",
        source_ref: "server.js v3"
      });

      return res.json({
        ignored: false,
        category: "pricing_issue",
        risk_level: "low",
        internal_summary: cleanMessage,
        team_message: teamMessage,
        requires_supervisor_approval: false,
        used_data: quote
      });
    }

    const defaultMessage = "Alright team, noted — let’s keep things moving.";

    await supabaseInsert("tapiwa_ai_audit_logs", {
      request_type: "team_chat",
      user_message: message,
      clean_message: cleanMessage,
      ai_category: "general_update",
      risk_level: "low",
      team_message: defaultMessage,
      internal_summary: cleanMessage,
      used_data: {},
      raw_ai_response: {},
      success: true,
      source_type: "ai_server",
      source_ref: "server.js v3"
    });

    return res.json({
      ignored: false,
      category: "general_update",
      risk_level: "low",
      internal_summary: cleanMessage,
      team_message: defaultMessage,
      requires_supervisor_approval: false,
      used_data: {}
    });
  } catch (error) {
    console.error("AI ERROR:", error);

    await supabaseInsert("tapiwa_ai_audit_logs", {
      request_type: "team_chat",
      clean_message: cleanMessage,
      ai_category: "system_issue",
      risk_level: "low",
      team_message: "Something went off on my side — try that again.",
      internal_summary: cleanMessage,
      used_data: {},
      raw_ai_response: {},
      success: false,
      error_message: error.message,
      source_type: "ai_server",
      source_ref: "server.js v3"
    });

    return res.json({
      ignored: false,
      category: "system_issue",
      risk_level: "low",
      internal_summary: cleanMessage || "System issue",
      team_message: "Something went off on my side — try that again.",
      requires_supervisor_approval: false,
      used_data: {}
    });
  }
});

app.post("/api/final-price", async (req, res) => {
  const {
    trip_request_id,
    pricing_decision_id,
    tapiwa_recommended_price,
    final_price,
    override_reason,
    customer_accepted = null,
    driver_accepted = null,
    trip_completed = false
  } = req.body || {};

  const recommended = cleanPrice(tapiwa_recommended_price);
  const finalPrice = cleanPrice(final_price || tapiwa_recommended_price);

  const inserted = await supabaseInsert("tapiwa_price_outcomes", {
    trip_request_id: trip_request_id || null,
    pricing_decision_id: pricing_decision_id || null,
    tapiwa_recommended_price: recommended,
    final_price: finalPrice,
    override_reason: override_reason || null,
    customer_accepted,
    driver_accepted,
    trip_completed,
    source_type: "dispatcher_final_action",
    source_ref: "server.js v3"
  });

  return res.json({
    success: true,
    outcome: inserted?.[0] || null,
    tapiwa_recommended_price: recommended,
    final_price: finalPrice,
    price_difference: finalPrice - recommended,
    price_overridden: finalPrice !== recommended
  });
});

app.listen(PORT, "0.0.0.0", () => {
  console.log(`Zachangu AI server v3 running on port ${PORT}`);
});
