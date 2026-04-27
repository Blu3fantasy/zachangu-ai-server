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

const tableDebug = {};
const conversationMemory = new Map();

app.get("/", (req, res) => {
  res.json({
    status: "Zachangu AI server is running",
    groq_ready: Boolean(GROQ_API_KEY),
    supabase_ready: Boolean(SUPABASE_URL && SUPABASE_SERVICE_ROLE_KEY),
    supabase_url_loaded: SUPABASE_URL || null,
    tapiwa_intelligence_ready: true
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
    "tapiwa_ai_audit_logs"
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
  return String(SUPABASE_URL || "")
    .replace(/\/rest\/v1\/?$/, "")
    .replace(/\/$/, "");
}

function roundToNearest100(value) {
  return Math.round(Number(value || 0) / 100) * 100;
}

function cleanPrice(value) {
  return Math.max(3000, roundToNearest100(value));
}

function buildSessionId({ sessionId, senderName, senderRole }) {
  return String(
    sessionId || `${senderRole || "Dispatcher"}:${senderName || "unknown"}`
  ).trim();
}

function getConversationMemory(sessionId) {
  if (!conversationMemory.has(sessionId)) {
    conversationMemory.set(sessionId, {
      lastRoute: null,
      pendingConfirmation: false,
      confirmedRoute: null,
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
    updatedAt: Date.now()
  });
}

function normalizeText(value) {
  return String(value || "")
    .toLowerCase()
    .replace(/[^\w\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function isAffirmationMessage(message) {
  const text = normalizeText(message);
  return /^(yes|ya|yeah|yep|correct|confirmed|that one|that route|ok|okay|alright|right)$/i.test(
    text
  );
}

function isFollowUpRouteMessage(message) {
  const text = normalizeText(message);

  return (
    /how much for that trip/.test(text) ||
    /how much for that route/.test(text) ||
    /how much for that/.test(text) ||
    /price for that/.test(text) ||
    /quote that/.test(text) ||
    /distance for that/.test(text) ||
    /km for that/.test(text)
  );
}

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
  if (match && isPricingLikeMessage(text)) {
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

function isExplicitRouteMessage(message) {
  return Boolean(extractRouteParts(message));
}

function routeSnapshotFromUnderstanding(routeUnderstanding) {
  if (
    !routeUnderstanding ||
    !routeUnderstanding.hasRoute ||
    !routeUnderstanding.pickupRaw ||
    !routeUnderstanding.dropoffRaw
  ) {
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
  return `${route.pickupRaw} to ${route.dropoffRaw}`;
}

async function fetchWithTimeout(url, options = {}, timeoutMs = 12000) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);

  try {
    return await fetch(url, {
      ...options,
      signal: controller.signal
    });
  } finally {
    clearTimeout(timeout);
  }
}

async function supabaseFetch(table, limit = 20) {
  if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
    tableDebug[table] = {
      ok: false,
      reason: "Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY"
    };
    return [];
  }

  const url = `${cleanBaseUrl()}/rest/v1/${table}?select=*&limit=${limit}`;

  try {
    const response = await fetchWithTimeout(
      url,
      {
        method: "GET",
        headers: {
          apikey: SUPABASE_SERVICE_ROLE_KEY,
          Authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
          "Content-Type": "application/json"
        }
      },
      10000
    );

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
    const response = await fetchWithTimeout(
      url,
      {
        method: "POST",
        headers: {
          apikey: SUPABASE_SERVICE_ROLE_KEY,
          Authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
          "Content-Type": "application/json",
          Prefer: "return=representation"
        },
        body: JSON.stringify(payload)
      },
      10000
    );

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
  if (a.includes(b) || b.includes(a)) return 0.88;

  const distance = levenshtein(a, b);
  const maxLength = Math.max(a.length, b.length);

  return 1 - distance / maxLength;
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

function isCorrectionMessage(message) {
  const text = normalizeText(message);

  return /no tapiwa|not that|i meant|meant|wrong|correction|not from|not to/.test(
    text
  );
}

function bestLandmarkMatch(rawName, landmarks) {
  if (!rawName || !Array.isArray(landmarks)) {
    return { match: null, score: 0 };
  }

  const candidates = landmarks.map((lm) => {
    const names = [lm.Landmark_Name, lm.Area, lm.Nearby_Landmarks].filter(Boolean);

    let bestScore = 0;

    for (const name of names) {
      const score = similarity(rawName, name);
      if (score > bestScore) bestScore = score;
    }

    return {
      landmark: lm,
      score: bestScore
    };
  });

  candidates.sort((a, b) => b.score - a.score);

  return {
    match: (candidates[0]?.score || 0) >= 0.55 ? candidates[0]?.landmark || null : null,
    score: candidates[0]?.score || 0
  };
}

function samePlaceName(a, b) {
  const left = normalizeText(a);
  const right = normalizeText(b);
  return Boolean(left && right && left === right);
}

function routeMatchesResolvedLandmarks(route, pickupName, dropoffName) {
  if (!route || !pickupName || !dropoffName) return false;
  return (
    samePlaceName(route.From_Landmark, pickupName) &&
    samePlaceName(route.To_Landmark, dropoffName)
  );
}

function marketPriceMatchesResolvedLandmarks(row, pickupName, dropoffName) {
  if (!row || !pickupName || !dropoffName) return false;
  return (
    samePlaceName(row.Origin_Landmark, pickupName) &&
    samePlaceName(row.Destination_Landmark, dropoffName)
  );
}

function tapiwaRuleMatchesResolvedLandmarks(row, pickupName, dropoffName) {
  if (!row || !pickupName || !dropoffName) return false;
  return (
    samePlaceName(row.pickup_landmark, pickupName) &&
    samePlaceName(row.dropoff_landmark, dropoffName)
  );
}

function buildRouteUnderstanding(message, landmarksRaw) {
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
  else if (avgScore >= 0.55) confidence = "medium";

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
    "that",
    "trip"
  ]);

  return normalizeText(message)
    .split(" ")
    .filter((word) => word.length > 2 && !stopWords.has(word));
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
    .map((row) => ({
      row,
      score: scoreRow(row, keywords)
    }))
    .filter((item) => item.score > 0)
    .sort((a, b) => b.score - a.score)
    .slice(0, limit)
    .map((item) => item.row);
}

function slimLandmark(row) {
  return {
    Landmark_ID: row.Landmark_ID,
    Landmark_Name: row.Landmark_Name,
    Area: row.Area,
    Zone_ID: row.Zone_ID,
    Nearby_Landmarks: row.Nearby_Landmarks
  };
}

function slimZone(row) {
  return {
    Zone_ID: row.Zone_ID,
    Zone_Name: row.Zone_Name,
    Areas_Covered: row.Areas_Covered,
    Zone_Type: row.Zone_Type,
    Demand_Level: row.Demand_Level,
    Strategic_Role: row.Strategic_Role
  };
}

function slimPricingRule(row) {
  return {
    Vehicle_Type: row.Vehicle_Type,
    Base_Rate_Per_KM_MWK: row.Base_Rate_Per_KM_MWK,
    Minimum_Fare_MWK: row.Minimum_Fare_MWK,
    Peak_Multiplier: row.Peak_Multiplier,
    Night_Multiplier: row.Night_Multiplier,
    Rain_Multiplier: row.Rain_Multiplier,
    Commission_Percent: row.Commission_Percent,
    Active: row.Active
  };
}

function slimMarketPrice(row) {
  return {
    Route_ID: row.Route_ID,
    Origin_Landmark: row.Origin_Landmark,
    Destination_Landmark: row.Destination_Landmark,
    Origin_Zone: row.Origin_Zone,
    Destination_Zone: row.Destination_Zone,
    Min_Price: row.Min_Price,
    Max_Price: row.Max_Price,
    Avg_Price: row.Avg_Price,
    Last_Updated: row.Last_Updated
  };
}

function slimRoute(row) {
  return {
    Route_Key: row.Route_Key,
    From_Landmark: row.From_Landmark,
    To_Landmark: row.To_Landmark,
    Distance_KM: row.Distance_KM,
    Time_Normal_Min: row.Time_Normal_Min,
    Time_Peak_Min: row.Time_Peak_Min,
    Zone_From: row.Zone_From,
    Zone_To: row.Zone_To
  };
}

function slimRisk(row) {
  return {
    Zone_ID: row.Zone_ID,
    Rank_Name: row.Rank_Name,
    Problem_Description: row.Problem_Description,
    Category: row.Category,
    Risk_Points: row.Risk_Points,
    Universal_5_Step_Solution: row.Universal_5_Step_Solution
  };
}

function slimTapiwaMarketRule(row) {
  return {
    pickup_zone: row.pickup_zone,
    dropoff_zone: row.dropoff_zone,
    pickup_landmark: row.pickup_landmark,
    dropoff_landmark: row.dropoff_landmark,
    vehicle_type: row.vehicle_type,
    min_price: row.min_price,
    recommended_price: row.recommended_price,
    max_price: row.max_price,
    confidence: row.confidence,
    source_type: row.source_type,
    notes: row.notes
  };
}

function slimTapiwaRouteIntel(row) {
  return {
    route_name: row.route_name,
    pickup_landmark: row.pickup_landmark,
    dropoff_landmark: row.dropoff_landmark,
    pickup_zone: row.pickup_zone,
    dropoff_zone: row.dropoff_zone,
    distance_min_km: row.distance_min_km,
    distance_max_km: row.distance_max_km,
    typical_customer_type: row.typical_customer_type,
    peak_time: row.peak_time,
    key_concern: row.key_concern,
    route_behavior: row.route_behavior,
    pricing_notes: row.pricing_notes,
    driver_notes: row.driver_notes,
    customer_notes: row.customer_notes
  };
}

function slimTapiwaZoneBehavior(row) {
  return {
    zone_code: row.zone_code,
    zone_name: row.zone_name,
    zone_type: row.zone_type,
    demand_level: row.demand_level,
    strategic_role: row.strategic_role,
    demand_time: row.demand_time,
    pricing_behavior: row.pricing_behavior,
    customer_behavior: row.customer_behavior,
    driver_behavior: row.driver_behavior,
    risk_notes: row.risk_notes,
    dispatcher_notes: row.dispatcher_notes
  };
}

function slimTapiwaRouteLearning(row) {
  return {
    pickup_zone: row.pickup_zone,
    dropoff_zone: row.dropoff_zone,
    pickup_landmark: row.pickup_landmark,
    dropoff_landmark: row.dropoff_landmark,
    vehicle_type: row.vehicle_type,
    trip_count: row.trip_count,
    avg_final_price: row.avg_final_price,
    min_final_price: row.min_final_price,
    max_final_price: row.max_final_price,
    most_common_price: row.most_common_price,
    avg_tapiwa_price: row.avg_tapiwa_price,
    avg_override_difference: row.avg_override_difference,
    acceptance_rate: row.acceptance_rate
  };
}

function slimTapiwaOutcome(row) {
  return {
    trip_request_id: row.trip_request_id,
    tapiwa_recommended_price: row.tapiwa_recommended_price,
    final_price: row.final_price,
    price_difference: row.price_difference,
    price_overridden: row.price_overridden,
    override_reason: row.override_reason,
    customer_accepted: row.customer_accepted,
    driver_accepted: row.driver_accepted,
    trip_completed: row.trip_completed,
    created_at: row.created_at
  };
}

async function fetchTapiwaIntelligence(userMessage) {
  const keywords = getKeywords(userMessage);

  const [
    settingsRaw,
    marketRulesRaw,
    routeIntelRaw,
    zoneBehaviorRaw,
    routeLearningRaw,
    priceOutcomesRaw
  ] = await Promise.all([
    supabaseFetch("tapiwa_system_settings", 50),
    supabaseFetch("tapiwa_market_price_rules", 250),
    supabaseFetch("tapiwa_route_intelligence", 250),
    supabaseFetch("tapiwa_zone_behavior", 50),
    supabaseFetch("tapiwa_route_learning", 150),
    supabaseFetch("tapiwa_price_outcomes", 50)
  ]);

  const matchedMarketRules = topMatches(marketRulesRaw, keywords, 8).filter(
    (r) => r.active !== false
  );

  const matchedRouteIntel = topMatches(routeIntelRaw, keywords, 8).filter(
    (r) => r.active !== false
  );

  const matchedZoneBehavior = topMatches(zoneBehaviorRaw, keywords, 6).filter(
    (r) => r.active !== false
  );

  const matchedRouteLearning = topMatches(routeLearningRaw, keywords, 8);

  return {
    system_settings: settingsRaw,
    market_price_rules: matchedMarketRules.map(slimTapiwaMarketRule),
    route_intelligence: matchedRouteIntel.map(slimTapiwaRouteIntel),
    zone_behavior: matchedZoneBehavior.map(slimTapiwaZoneBehavior),
    route_learning: matchedRouteLearning.map(slimTapiwaRouteLearning),
    recent_price_outcomes: priceOutcomesRaw.slice(0, 10).map(slimTapiwaOutcome),
    data_counts: {
      tapiwa_system_settings: settingsRaw.length,
      tapiwa_market_price_rules: marketRulesRaw.length,
      tapiwa_route_intelligence: routeIntelRaw.length,
      tapiwa_zone_behavior: zoneBehaviorRaw.length,
      tapiwa_route_learning: routeLearningRaw.length,
      tapiwa_price_outcomes: priceOutcomesRaw.length
    },
    matched_counts: {
      market_price_rules: matchedMarketRules.length,
      route_intelligence: matchedRouteIntel.length,
      zone_behavior: matchedZoneBehavior.length,
      route_learning: matchedRouteLearning.length
    }
  };
}

async function fetchZachanguContext(userMessage) {
  const keywords = getKeywords(userMessage);

  const [
    zonesRaw,
    landmarksRaw,
    pricingRulesRaw,
    marketPricesRaw,
    routeMatrixRaw,
    risksRaw,
    tapiwaIntelligence
  ] = await Promise.all([
    supabaseFetch("zones", 60),
    supabaseFetch("landmarks", 250),
    supabaseFetch("pricing_rules", 30),
    supabaseFetch("market_prices", 150),
    supabaseFetch("route_matrix", 250),
    supabaseFetch("risks", 80),
    fetchTapiwaIntelligence(userMessage)
  ]);

  const matchedLandmarks = topMatches(landmarksRaw, keywords, 8);
  const routeUnderstanding = buildRouteUnderstanding(userMessage, landmarksRaw);
  const matchedZones = topMatches(zonesRaw, keywords, 6);
  const matchedMarketPrices = topMatches(marketPricesRaw, keywords, 8);
  const matchedRoutes = topMatches(routeMatrixRaw, keywords, 8);
  const matchedRisks = topMatches(risksRaw, keywords, 5);
  const resolvedPickupName = routeUnderstanding.pickup?.Landmark_Name || null;
  const resolvedDropoffName = routeUnderstanding.dropoff?.Landmark_Name || null;
  const exactRouteMatches =
    routeUnderstanding.confidence === "high" && resolvedPickupName && resolvedDropoffName
      ? routeMatrixRaw.filter((route) =>
          routeMatchesResolvedLandmarks(route, resolvedPickupName, resolvedDropoffName)
        )
      : [];
  const exactMarketPriceMatches =
    routeUnderstanding.confidence === "high" && resolvedPickupName && resolvedDropoffName
      ? marketPricesRaw.filter((row) =>
          marketPriceMatchesResolvedLandmarks(row, resolvedPickupName, resolvedDropoffName)
        )
      : [];
  const exactTapiwaMarketRules =
    routeUnderstanding.confidence === "high" && resolvedPickupName && resolvedDropoffName
      ? (tapiwaIntelligence.market_price_rules || []).filter((row) =>
          tapiwaRuleMatchesResolvedLandmarks(row, resolvedPickupName, resolvedDropoffName)
        )
      : [];

  const relevantZoneIds = new Set();

  for (const lm of matchedLandmarks) {
    if (lm.Zone_ID) relevantZoneIds.add(lm.Zone_ID);
  }

  for (const route of matchedRoutes) {
    if (route.Zone_From) relevantZoneIds.add(route.Zone_From);
    if (route.Zone_To) relevantZoneIds.add(route.Zone_To);
  }

  for (const mp of matchedMarketPrices) {
    if (mp.Origin_Zone) relevantZoneIds.add(mp.Origin_Zone);
    if (mp.Destination_Zone) relevantZoneIds.add(mp.Destination_Zone);
  }

  const relevantZones = zonesRaw.filter((z) => relevantZoneIds.has(z.Zone_ID));

  const activePricingRules = pricingRulesRaw
    .filter((rule) => String(rule.Active || "").toLowerCase() === "yes")
    .slice(0, 6);

  return {
    request_keywords: keywords.slice(0, 12),
    route_understanding: routeUnderstanding,

    zones: [...matchedZones, ...relevantZones]
      .filter((v, i, arr) => arr.findIndex((x) => x.Zone_ID === v.Zone_ID) === i)
      .slice(0, 6)
      .map(slimZone),

    landmarks: matchedLandmarks.slice(0, 8).map(slimLandmark),

    pricing_rules: activePricingRules.length
      ? activePricingRules.map(slimPricingRule)
      : pricingRulesRaw.slice(0, 4).map(slimPricingRule),

    market_prices: [...exactMarketPriceMatches, ...matchedMarketPrices]
      .filter(
        (v, i, arr) =>
          arr.findIndex(
            (x) =>
              String(x.Route_ID || "") === String(v.Route_ID || "") &&
              String(x.Origin_Landmark || "") === String(v.Origin_Landmark || "") &&
              String(x.Destination_Landmark || "") === String(v.Destination_Landmark || "")
          ) === i
      )
      .slice(0, 8)
      .map(slimMarketPrice),

    route_matrix: [...exactRouteMatches, ...matchedRoutes]
      .filter(
        (v, i, arr) =>
          arr.findIndex(
            (x) =>
              String(x.Route_Key || "") === String(v.Route_Key || "") &&
              String(x.From_Landmark || "") === String(v.From_Landmark || "") &&
              String(x.To_Landmark || "") === String(v.To_Landmark || "")
          ) === i
      )
      .slice(0, 8)
      .map(slimRoute),

    risks: matchedRisks.slice(0, 5).map(slimRisk),

    tapiwa_intelligence: {
      ...tapiwaIntelligence,
      market_price_rules: exactTapiwaMarketRules.length
        ? exactTapiwaMarketRules.map(slimTapiwaMarketRule)
        : tapiwaIntelligence.market_price_rules
    },

    data_counts: {
      zones: zonesRaw.length,
      landmarks: landmarksRaw.length,
      pricing_rules: pricingRulesRaw.length,
      market_prices: marketPricesRaw.length,
      route_matrix: routeMatrixRaw.length,
      risks: risksRaw.length,
      ...tapiwaIntelligence.data_counts
    },

    matched_counts: {
      zones: matchedZones.length,
      landmarks: matchedLandmarks.length,
      market_prices: matchedMarketPrices.length,
      route_matrix: matchedRoutes.length,
      risks: matchedRisks.length,
      exact_route_matrix: exactRouteMatches.length,
      exact_market_prices: exactMarketPriceMatches.length,
      exact_tapiwa_market_rules: exactTapiwaMarketRules.length,
      ...tapiwaIntelligence.matched_counts
    },

    table_status: tableDebug
  };
}

function calculateBasicFare(context) {
  const routeUnderstanding = context.route_understanding || {};
  const resolvedPickupName = routeUnderstanding.pickup?.Landmark_Name || null;
  const resolvedDropoffName = routeUnderstanding.dropoff?.Landmark_Name || null;
  const hasConfirmedExactRoute =
    routeUnderstanding.confidence === "high" &&
    resolvedPickupName &&
    resolvedDropoffName;

  if (!hasConfirmedExactRoute) {
    return null;
  }

  const tapiwaRule = context.tapiwa_intelligence?.market_price_rules?.[0];

  if (
    tapiwaRule?.recommended_price &&
    tapiwaRuleMatchesResolvedLandmarks(
      tapiwaRule,
      resolvedPickupName,
      resolvedDropoffName
    )
  ) {
    const recommended = cleanPrice(tapiwaRule.recommended_price);
    const low = cleanPrice(tapiwaRule.min_price || recommended);
    const high = cleanPrice(tapiwaRule.max_price || recommended);

    return {
      source: "tapiwa_market_price_rules",
      estimated_low_mwk: Math.min(low, high),
      estimated_high_mwk: Math.max(low, high),
      recommended_mwk: recommended,
      confidence: tapiwaRule.confidence || "medium",
      route_used: `${tapiwaRule.pickup_landmark || "Unknown"} → ${tapiwaRule.dropoff_landmark || "Unknown"}`
    };
  }

  const route = (context.route_matrix || []).find((row) =>
    routeMatchesResolvedLandmarks(row, resolvedPickupName, resolvedDropoffName)
  );
  const rule =
    context.pricing_rules?.find((r) =>
      String(r.Vehicle_Type || "").toLowerCase().includes("motorbike")
    ) || context.pricing_rules?.[0];

  if (!route || !rule || !route.Distance_KM || !rule.Base_Rate_Per_KM_MWK) {
    return null;
  }

  const distance = Number(route.Distance_KM);
  const rate = Number(rule.Base_Rate_Per_KM_MWK);
  const minimum = Number(rule.Minimum_Fare_MWK || 3000);

  if (!distance || !rate) return null;

  const rawFare = distance * rate;
  const baseFare = Math.max(rawFare, minimum, 3000);

  const low = cleanPrice(baseFare * 0.95);
  const high = cleanPrice(baseFare * 1.08);
  const recommended = cleanPrice((low + high) / 2);

  return {
    source: "route_matrix_plus_pricing_rules",
    distance_km: distance,
    vehicle_type: rule.Vehicle_Type,
    base_rate_per_km: rate,
    minimum_fare: minimum,
    estimated_low_mwk: Math.min(low, high),
    estimated_high_mwk: Math.max(low, high),
    recommended_mwk: recommended,
    route_used: `${route.From_Landmark} → ${route.To_Landmark}`
  };
}

async function saveAuditLog(payload) {
  await supabaseInsert("tapiwa_ai_audit_logs", {
    request_type: payload.request_type || "team_chat",
    user_message: payload.user_message || null,
    clean_message: payload.clean_message || null,
    ai_category: payload.ai_category || null,
    risk_level: payload.risk_level || null,
    team_message: payload.team_message || null,
    internal_summary: payload.internal_summary || null,
    used_data: payload.used_data || null,
    raw_ai_response: payload.raw_ai_response || null,
    success: payload.success !== false,
    error_message: payload.error_message || null,
    source_type: "ai_server",
    source_ref: "server.js"
  });
}

app.post("/ai/analyze", async (req, res) => {
  let cleanMessage = "";
  let context = null;
  let aiResult = {};

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

    if (!GROQ_API_KEY) {
      return res.status(500).json({ error: "Missing GROQ_API_KEY" });
    }

    if (!hasTapiwaCall(message)) {
      return res.json({
        ignored: true,
        reason: "Tapiwa was not mentioned. Use @Tapiwa to call the AI."
      });
    }

    cleanMessage = cleanTapiwaMessage(message);

    const sessionId = buildSessionId({
      sessionId: rawSessionId,
      senderName,
      senderRole
    });

    const memory = getConversationMemory(sessionId);
    const isAffirmation = isAffirmationMessage(cleanMessage);
    const isCorrection = isCorrectionMessage(cleanMessage);
    const isFollowUp = isFollowUpRouteMessage(cleanMessage);
    const hasExplicitRoute = isExplicitRouteMessage(cleanMessage);

    if (isAffirmation && memory.pendingConfirmation && memory.lastRoute) {
      memory.confirmedRoute = { ...memory.lastRoute };
      memory.pendingConfirmation = false;
      saveConversationMemory(sessionId, memory);

      const pickupName = memory.confirmedRoute.pickup || memory.confirmedRoute.pickupRaw;
      const dropoffName = memory.confirmedRoute.dropoff || memory.confirmedRoute.dropoffRaw;

      await saveAuditLog({
        request_type: "team_chat",
        user_message: message,
        clean_message: cleanMessage,
        ai_category: "pricing_issue",
        risk_level: "low",
        team_message: `Confirmed ${pickupName} to ${dropoffName}.`,
        internal_summary: `Route confirmed from memory: ${pickupName} to ${dropoffName}`,
        used_data: {
          confirmed_route: memory.confirmedRoute
        },
        raw_ai_response: { fast_path: "affirmation_confirmation" },
        success: true
      });

      return res.json({
        ignored: false,
        category: "pricing_issue",
        risk_level: "low",
        internal_summary: `Route confirmed: ${pickupName} to ${dropoffName}`,
        team_message: `Confirmed ${pickupName} to ${dropoffName}.`,
        requires_supervisor_approval: false,
        used_data: {
          confirmed_route: memory.confirmedRoute
        },
        debug_memory: {
          sessionId,
          lastRoute: memory.lastRoute,
          pendingConfirmation: memory.pendingConfirmation,
          confirmedRoute: memory.confirmedRoute
        }
      });
    }

    if (isFollowUp && memory.confirmedRoute && !hasExplicitRoute) {
      const lookupMessage = routeToLookupMessage(memory.confirmedRoute);
      context = await fetchZachanguContext(lookupMessage);
      const computedFare = calculateBasicFare(context);

      saveConversationMemory(sessionId, memory);

      const teamMessage = computedFare
        ? `For ${lookupMessage}, it should be around MWK ${Number(
            computedFare.recommended_mwk || computedFare.estimated_low_mwk
          ).toLocaleString()} — safe range is MWK ${Number(
            computedFare.estimated_low_mwk
          ).toLocaleString()} to MWK ${Number(
            computedFare.estimated_high_mwk
          ).toLocaleString()}.`
        : `I’ve kept ${lookupMessage}. I don’t have pricing for it yet — check distance manually before quoting.`;

      await saveAuditLog({
        request_type: "team_chat",
        user_message: message,
        clean_message: cleanMessage,
        ai_category: "pricing_issue",
        risk_level: "low",
        team_message: teamMessage,
        internal_summary: `Used confirmed route from memory: ${lookupMessage}`,
        used_data: {
          confirmed_route: memory.confirmedRoute,
          computed_fare: computedFare
        },
        raw_ai_response: { fast_path: "confirmed_route_follow_up" },
        success: true
      });

      return res.json({
        ignored: false,
        category: "pricing_issue",
        risk_level: "low",
        internal_summary: `Used confirmed route ${lookupMessage} for follow-up`,
        team_message: teamMessage,
        requires_supervisor_approval: false,
        used_data: {
          confirmed_route: memory.confirmedRoute,
          computed_fare: computedFare
        },
        debug_memory: {
          sessionId,
          lastRoute: memory.lastRoute,
          pendingConfirmation: memory.pendingConfirmation,
          confirmedRoute: memory.confirmedRoute
        },
        debug_data_counts: context.data_counts,
        debug_matched_counts: context.matched_counts
      });
    }

    context = await fetchZachanguContext(cleanMessage);
    const routeUnderstanding = context.route_understanding;
    const detectedRoute = routeSnapshotFromUnderstanding(routeUnderstanding);

    if (detectedRoute) {
      memory.lastRoute = { ...detectedRoute };

      if (isCorrection) {
        memory.confirmedRoute =
          detectedRoute.confidence === "high" ? { ...detectedRoute } : null;
        memory.pendingConfirmation = detectedRoute.confidence === "medium";
      } else if (detectedRoute.confidence === "high") {
        memory.confirmedRoute = { ...detectedRoute };
        memory.pendingConfirmation = false;
      } else if (detectedRoute.confidence === "medium") {
        memory.confirmedRoute = null;
        memory.pendingConfirmation = true;
      } else {
        memory.confirmedRoute = null;
        memory.pendingConfirmation = false;
      }
    }

    saveConversationMemory(sessionId, memory);

    const computedFare = calculateBasicFare(context);

    const isPricingRequest = isPricingLikeMessage(cleanMessage) || isFollowUp;
    const hasAnyPricingData =
      context.tapiwa_intelligence.market_price_rules.length > 0 ||
      context.tapiwa_intelligence.route_intelligence.length > 0 ||
      context.tapiwa_intelligence.route_learning.length > 0 ||
      context.market_prices.length > 0 ||
      context.route_matrix.length > 0 ||
      context.pricing_rules.length > 0 ||
      computedFare;

    let forcedMessage = null;

    if (
      isPricingRequest &&
      routeUnderstanding?.hasRoute &&
      routeUnderstanding.confidence === "low"
    ) {
      forcedMessage =
        "I can’t lock that route yet — send the pickup and drop-off clearly before I quote it.";
    } else if (
      isPricingRequest &&
      routeUnderstanding?.hasRoute &&
      routeUnderstanding.confidence === "medium"
    ) {
      const pickupName =
        routeUnderstanding.pickup?.Landmark_Name || routeUnderstanding.pickupRaw;
      const dropoffName =
        routeUnderstanding.dropoff?.Landmark_Name || routeUnderstanding.dropoffRaw;
      forcedMessage = `I think you mean ${pickupName} to ${dropoffName} — confirm that route before I quote it.`;
    } else if (isPricingRequest && !hasAnyPricingData) {
      forcedMessage =
        "Hmm, I don't have pricing for that route yet — check the distance manually or ask someone who's done that run before quoting.";
    }

    const systemPrompt = `You are Dispatch Tapiwa, a real person on the Zachangu dispatch team in Lilongwe, Malawi.

You help the crew with operations, pricing, safety, and dispatch decisions.

You must sound human, calm, short, and useful. Do not sound like a report. Do not sound like a robot.

IMPORTANT PERSONALITY:
- You are friendly but controlled.
- You are a teammate, not a chatbot.
- Keep team_message short.
- 1 to 2 sentences max.
- No bullets in team_message.
- No formal labels in team_message.

ROUTE MEMORY RULES:
- Respect confirmed_route from conversation_memory.
- If confirmed_route exists, do not guess a different route.
- If dispatcher already confirmed a route, keep using that route.
- If pending_confirmation exists and dispatcher says yes/correct/that one, treat the last route as confirmed.
- If confidence is medium, ask for confirmation.
- If confidence is low, ask for clearer pickup and drop-off.

PRICING RULES:
- Minimum fare is MWK 3,000.
- Never recommend below MWK 3,000.
- Round prices to nearest MWK 100.
- Prefer Tapiwa intelligence tables when available.

JSON only:
{
  "category":"",
  "risk_level":"",
  "internal_summary":"",
  "team_message":"",
  "requires_supervisor_approval":false,
  "used_data":{
    "zones":[],
    "landmarks":[],
    "pricing_rules":[],
    "market_prices":[],
    "route_matrix":[],
    "tapiwa_market_price_rules":[],
    "tapiwa_route_intelligence":[],
    "tapiwa_zone_behavior":[],
    "tapiwa_route_learning":[],
    "computed_fare":null
  }
}`;

    if (!forcedMessage) {
      const groqResponse = await fetchWithTimeout(
        "https://api.groq.com/openai/v1/chat/completions",
        {
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
                  sender: `${senderName} (${senderRole})`,
                  session_id: sessionId,
                  msg: cleanMessage,
                  conversation_memory: {
                    lastRoute: memory.lastRoute,
                    pendingConfirmation: memory.pendingConfirmation,
                    confirmedRoute: memory.confirmedRoute
                  },
                  ctx: {
                    route_understanding: context.route_understanding,
                    zones: context.zones,
                    landmarks: context.landmarks,
                    pricing_rules: context.pricing_rules,
                    market_prices: context.market_prices,
                    route_matrix: context.route_matrix,
                    risks: context.risks,
                    tapiwa_intelligence: context.tapiwa_intelligence
                  },
                  fare: computedFare
                })
              }
            ],
            response_format: { type: "json_object" },
            temperature: 0.2,
            max_tokens: 500
          })
        },
        12000
      );

      const groqData = await groqResponse.json();

      if (!groqResponse.ok) {
        await saveAuditLog({
          request_type: "team_chat",
          user_message: message,
          clean_message: cleanMessage,
          success: false,
          error_message: "Groq API error",
          raw_ai_response: groqData
        });

        return res.status(groqResponse.status).json({
          error: "Groq API error",
          details: groqData
        });
      }

      try {
        aiResult = JSON.parse(groqData.choices?.[0]?.message?.content || "{}");
      } catch {
        aiResult = {};
      }
    }

    const allowedCategories = [
      "incident",
      "pricing_issue",
      "driver_issue",
      "traffic",
      "system_issue",
      "general_update"
    ];

    const allowedRiskLevels = ["low", "medium", "high"];

    let category = aiResult.category || "general_update";

    if (!allowedCategories.includes(category)) {
      const lower = cleanMessage.toLowerCase();
      if (/price|fare|cost|charge|quote|how much|km|distance/.test(lower)) {
        category = "pricing_issue";
      } else if (
        /robbed|accident|threat|violence|attack|stolen|drunk|refuse/.test(lower)
      ) {
        category = "incident";
      } else if (/driver/.test(lower)) {
        category = "driver_issue";
      } else if (/traffic|rain|roadblock|police|jam/.test(lower)) {
        category = "traffic";
      } else {
        category = "general_update";
      }
    }

    let riskLevel = aiResult.risk_level || "low";
    if (!allowedRiskLevels.includes(riskLevel)) riskLevel = "low";

    let fallbackMessage =
      "Alright team, noted — let's handle this and keep things moving.";

    if (category === "pricing_issue") {
      if (routeUnderstanding?.hasRoute && routeUnderstanding.confidence === "low") {
        fallbackMessage =
          "I can’t lock that route yet — send pickup and drop-off clearly.";
      } else if (
        routeUnderstanding?.hasRoute &&
        routeUnderstanding.confidence === "medium"
      ) {
        const pickupName =
          routeUnderstanding.pickup?.Landmark_Name || routeUnderstanding.pickupRaw;
        const dropoffName =
          routeUnderstanding.dropoff?.Landmark_Name || routeUnderstanding.dropoffRaw;

        fallbackMessage = `I think you mean ${pickupName} to ${dropoffName} — confirm that route before I quote it.`;
      } else if (computedFare) {
        fallbackMessage = `Yeah, for ${computedFare.route_used} it should be around MWK ${Number(
          computedFare.recommended_mwk || computedFare.estimated_low_mwk
        ).toLocaleString()} — safe range is MWK ${Number(
          computedFare.estimated_low_mwk
        ).toLocaleString()} to MWK ${Number(
          computedFare.estimated_high_mwk
        ).toLocaleString()}.`;
      } else {
        fallbackMessage =
          "Hmm, I don't have pricing for that route yet — check the distance manually or ask someone who's done that run before quoting.";
      }
    }

    const responsePayload = {
      ignored: false,
      category,
      risk_level: riskLevel,
      internal_summary: aiResult.internal_summary || cleanMessage,
      team_message: forcedMessage || aiResult.team_message || fallbackMessage,
      requires_supervisor_approval:
        riskLevel === "high" || aiResult.requires_supervisor_approval === true,
      used_data: aiResult.used_data || {
        zones: context.zones.map((z) => z.Zone_ID || z.Zone_Name).filter(Boolean),
        landmarks: context.landmarks.map((l) => l.Landmark_Name).filter(Boolean),
        pricing_rules: context.pricing_rules.map((p) => p.Vehicle_Type).filter(Boolean),
        market_prices: context.market_prices.map((m) => m.Route_ID).filter(Boolean),
        route_matrix: context.route_matrix.map((r) => r.Route_Key).filter(Boolean),
        tapiwa_market_price_rules: context.tapiwa_intelligence.market_price_rules
          .map((r) => r.pickup_landmark || r.pickup_zone)
          .filter(Boolean),
        tapiwa_route_intelligence: context.tapiwa_intelligence.route_intelligence
          .map((r) => r.route_name)
          .filter(Boolean),
        tapiwa_zone_behavior: context.tapiwa_intelligence.zone_behavior
          .map((z) => z.zone_code)
          .filter(Boolean),
        tapiwa_route_learning: context.tapiwa_intelligence.route_learning
          .map((r) => `${r.pickup_landmark} → ${r.dropoff_landmark}`)
          .filter(Boolean),
        computed_fare: computedFare
      },
      debug_memory: {
        sessionId,
        lastRoute: memory.lastRoute,
        pendingConfirmation: memory.pendingConfirmation,
        confirmedRoute: memory.confirmedRoute
      },
      debug_data_counts: context.data_counts,
      debug_matched_counts: context.matched_counts
    };

    await saveAuditLog({
      request_type: "team_chat",
      user_message: message,
      clean_message: cleanMessage,
      ai_category: responsePayload.category,
      risk_level: responsePayload.risk_level,
      team_message: responsePayload.team_message,
      internal_summary: responsePayload.internal_summary,
      used_data: responsePayload.used_data,
      raw_ai_response: aiResult,
      success: true
    });

    return res.json(responsePayload);
  } catch (error) {
    await saveAuditLog({
      request_type: "team_chat",
      clean_message: cleanMessage,
      used_data: context,
      raw_ai_response: aiResult,
      success: false,
      error_message: error.message
    });

    return res.status(500).json({
      error: "AI server failed",
      details: error.message
    });
  }
});

const port = process.env.PORT || 3001;

app.listen(port, "0.0.0.0", () => {
  console.log(`Zachangu AI server running on port ${port}`);
});
