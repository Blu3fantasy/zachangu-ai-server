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

app.get("/", (req, res) => {
  res.json({
    status: "Zachangu AI server is running",
    groq_ready: Boolean(GROQ_API_KEY),
    supabase_ready: Boolean(SUPABASE_URL && SUPABASE_SERVICE_ROLE_KEY),
    supabase_url_loaded: SUPABASE_URL || null
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
    "trip_requests"
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

function normalizeText(value) {
  return String(value || "")
    .toLowerCase()
    .replace(/[^\w\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
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
    "is"
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

async function fetchZachanguContext(userMessage) {
  const keywords = getKeywords(userMessage);

  const [
    zonesRaw,
    landmarksRaw,
    pricingRulesRaw,
    marketPricesRaw,
    routeMatrixRaw,
    risksRaw
  ] = await Promise.all([
    supabaseFetch("zones", 60),
    supabaseFetch("landmarks", 250),
    supabaseFetch("pricing_rules", 30),
    supabaseFetch("market_prices", 150),
    supabaseFetch("route_matrix", 250),
    supabaseFetch("risks", 80)
  ]);

  const matchedLandmarks = topMatches(landmarksRaw, keywords, 8);
  const matchedZones = topMatches(zonesRaw, keywords, 6);
  const matchedMarketPrices = topMatches(marketPricesRaw, keywords, 8);
  const matchedRoutes = topMatches(routeMatrixRaw, keywords, 8);
  const matchedRisks = topMatches(risksRaw, keywords, 5);

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

    zones: [...matchedZones, ...relevantZones]
      .filter((v, i, arr) => arr.findIndex((x) => x.Zone_ID === v.Zone_ID) === i)
      .slice(0, 6)
      .map(slimZone),

    landmarks: matchedLandmarks.slice(0, 8).map(slimLandmark),

    pricing_rules: activePricingRules.length
      ? activePricingRules.map(slimPricingRule)
      : pricingRulesRaw.slice(0, 4).map(slimPricingRule),

    market_prices: matchedMarketPrices.slice(0, 8).map(slimMarketPrice),

    route_matrix: matchedRoutes.slice(0, 8).map(slimRoute),

    risks: matchedRisks.slice(0, 5).map(slimRisk),

    data_counts: {
      zones: zonesRaw.length,
      landmarks: landmarksRaw.length,
      pricing_rules: pricingRulesRaw.length,
      market_prices: marketPricesRaw.length,
      route_matrix: routeMatrixRaw.length,
      risks: risksRaw.length
    },

    matched_counts: {
      zones: matchedZones.length,
      landmarks: matchedLandmarks.length,
      market_prices: matchedMarketPrices.length,
      route_matrix: matchedRoutes.length,
      risks: matchedRisks.length
    },

    table_status: tableDebug
  };
}

function calculateBasicFare(context) {
  const route = context.route_matrix?.[0];
  const rule =
    context.pricing_rules?.find((r) =>
      String(r.Vehicle_Type || "").toLowerCase().includes("motorbike")
    ) || context.pricing_rules?.[0];

  if (!route || !rule || !route.Distance_KM || !rule.Base_Rate_Per_KM_MWK) {
    return null;
  }

  const distance = Number(route.Distance_KM);
  const rate = Number(rule.Base_Rate_Per_KM_MWK);
  const minimum = Number(rule.Minimum_Fare_MWK || 0);

  if (!distance || !rate) return null;

  const rawFare = distance * rate;
  const baseFare = Math.max(rawFare, minimum);

  const low = Math.round((baseFare * 0.95) / 500) * 500;
  const high = Math.round((baseFare * 1.08) / 500) * 500;

  return {
    distance_km: distance,
    vehicle_type: rule.Vehicle_Type,
    base_rate_per_km: rate,
    minimum_fare: minimum,
    estimated_low_mwk: low,
    estimated_high_mwk: high,
    route_used: `${route.From_Landmark} → ${route.To_Landmark}`
  };
}

app.post("/ai/analyze", async (req, res) => {
  try {
    const {
      message,
      senderName = "Dispatcher",
      senderRole = "Dispatcher"
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

    const cleanMessage = cleanTapiwaMessage(message);
    const context = await fetchZachanguContext(cleanMessage);
    const computedFare = calculateBasicFare(context);

    const isPricingRequest = /price|fare|cost|charge|quote/i.test(cleanMessage);
    const hasAnyPricingData =
      context.market_prices.length > 0 ||
      context.route_matrix.length > 0 ||
      context.pricing_rules.length > 0 ||
      computedFare;

    let forcedMessage = null;

    if (isPricingRequest && !hasAnyPricingData) {
      forcedMessage =
        "Hmm, I don't have pricing for that route yet — check the distance manually or ask someone who's done that run before quoting.";
    }

    const systemPrompt = `You are Tapiwa, a real person on the Zachangu dispatch team in Lilongwe, Malawi. You help the crew with ops, pricing, and keeping things running.

team_message is how you talk in the group chat. Write it like you are texting your workmates — not like a report, not like a robot. Short, real, human. Say things like yeah, alright, good catch, let's sort this, hold on that one. React naturally to what is happening.

BAD: Incident detected. Risk level: medium. Action required: notify supervisor.
GOOD: Heeh, that does not sound right — let us pause that area and loop in the supervisor before sending anyone.

BAD: Estimated fare: MWK 3,000-3,500. Confirm route before dispatch.
GOOD: Should be somewhere around 3k to 3,500 — just confirm the pickup spot first before you quote them.

1-2 sentences max. No bullets. No labels. No formal tone.
Safety: never send drivers into trouble spots. Escalate high-risk to supervisor.
Pricing: only use numbers from computed_fare or market_prices. Never guess.
Never take direct action — you advise, the dispatcher decides.
category: incident|pricing_issue|driver_issue|traffic|system_issue|general_update
risk_level: low|medium|high

JSON only: {"category":"","risk_level":"","internal_summary":"","team_message":"","requires_supervisor_approval":false,"used_data":{"zones":[],"landmarks":[],"pricing_rules":[],"market_prices":[],"route_matrix":[],"computed_fare":null}}`;

    let aiResult = {};

    if (!forcedMessage) {
      const groqResponse = await fetch(
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
                // Only send what the model actually needs — drop debug counts
                content: JSON.stringify({
                  sender: `${senderName} (${senderRole})`,
                  msg: cleanMessage,
                  ctx: {
                    zones: context.zones,
                    landmarks: context.landmarks,
                    pricing_rules: context.pricing_rules,
                    market_prices: context.market_prices,
                    route_matrix: context.route_matrix,
                    risks: context.risks
                  },
                  fare: computedFare
                })
              }
            ],
            response_format: { type: "json_object" },
            temperature: 0.2,
            max_tokens: 350  // reduced from 500; output is small JSON
          })
        }
      );

      const groqData = await groqResponse.json();

      if (!groqResponse.ok) {
        return res.status(groqResponse.status).json({
          error: "Groq API error",
          details: groqData
        });
      }

      try {
        aiResult = JSON.parse(
          groqData.choices?.[0]?.message?.content || "{}"
        );
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
      if (/price|fare|cost|charge|quote/.test(lower)) category = "pricing_issue";
      else if (/robbed|accident|threat|violence|attack|stolen/.test(lower)) category = "incident";
      else if (/driver/.test(lower)) category = "driver_issue";
      else if (/traffic|rain|roadblock|police|jam/.test(lower)) category = "traffic";
      else category = "general_update";
    }

    let riskLevel = aiResult.risk_level || "low";
    if (!allowedRiskLevels.includes(riskLevel)) riskLevel = "low";

    let fallbackMessage =
"Alright team, noted — let's handle this and keep things moving.";

    if (category === "pricing_issue") {
      if (computedFare) {
        fallbackMessage = `Yeah should be around MWK ${computedFare.estimated_low_mwk.toLocaleString()} to ${computedFare.estimated_high_mwk.toLocaleString()} — just double-check the pickup and traffic before you tell them.`;
      } else if (context.market_prices.length > 0) {
        const mp = context.market_prices[0];
        fallbackMessage = `Market rate for that is around MWK ${Number(mp.Min_Price).toLocaleString()} to ${Number(mp.Max_Price).toLocaleString()} — confirm with the rider before you dispatch.`;
      } else {
        fallbackMessage =
          "Hmm, I don't have pricing for that route yet — check the distance manually or ask someone who's done that run before quoting.";
      }
    }

    return res.json({
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
        computed_fare: computedFare
      },
      debug_data_counts: context.data_counts,
      debug_matched_counts: context.matched_counts
    });
  } catch (error) {
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
