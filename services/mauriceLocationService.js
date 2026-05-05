// services/mauriceLocationService.js
// Zachangu Maurice Location Discovery Service
// Purpose: Fetch landmarks from Supabase, score rider clues, ask one question at a time,
// and return estimated km + bike fare range only when confidence is enough.

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

const MIN_CONFIDENCE_TO_PRICE = Number(process.env.MAURICE_LOCATION_MIN_CONFIDENCE || 0.65);
const DEFAULT_MIN_FARE = Number(process.env.ZACHANGU_BIKE_MIN_FARE || 3000);
const DEFAULT_LOW_RATE_PER_KM = Number(process.env.ZACHANGU_BIKE_LOW_RATE_PER_KM || 1400);
const DEFAULT_HIGH_RATE_PER_KM = Number(process.env.ZACHANGU_BIKE_HIGH_RATE_PER_KM || 1600);

function normalizeText(text = "") {
  return String(text).toLowerCase().trim();
}

function detectArea(text = "") {
  const clean = normalizeText(text);
  const match = clean.match(/(?:area|ma)\s?\d+/i);
  if (!match) return null;

  return match[0]
    .replace(/^ma/i, "Area")
    .replace(/^area/i, "Area")
    .replace(/\s+/, " ");
}

async function supabaseFetch(path) {
  if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
    throw new Error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY");
  }

  const baseUrl = SUPABASE_URL.replace(/\/$/, "");
  const res = await fetch(`${baseUrl}/rest/v1/${path}`, {
    headers: {
      apikey: SUPABASE_SERVICE_ROLE_KEY,
      Authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
      "Content-Type": "application/json"
    }
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Supabase fetch failed: ${res.status} ${text}`);
  }

  return res.json();
}

function getLandmarkName(landmark) {
  return landmark.name || landmark.landmark_name || landmark.Landmark_Name || landmark.Landmark || "";
}

function getLandmarkArea(landmark) {
  return landmark.area || landmark.Area || landmark.area_name || landmark.Area_Name || "";
}

function getZoneId(landmark) {
  return landmark.zone_id || landmark.Zone_ID || landmark.zone || landmark.Zone || null;
}

function getAliases(landmark) {
  const raw = landmark.aliases || landmark.Aliases || landmark.alias || landmark.Alias || [];

  if (Array.isArray(raw)) return raw.map(String);

  if (typeof raw === "string") {
    try {
      const parsed = JSON.parse(raw);
      if (Array.isArray(parsed)) return parsed.map(String);
    } catch (_) {
      // Not JSON. Treat it as comma-separated aliases.
    }

    return raw.split(",").map((x) => x.trim()).filter(Boolean);
  }

  return [];
}

function scoreLandmark(message, landmark) {
  const text = normalizeText(message);
  const name = normalizeText(getLandmarkName(landmark));
  const area = normalizeText(getLandmarkArea(landmark));
  const zone = normalizeText(getZoneId(landmark) || "");

  let score = 0;

  if (name && text.includes(name)) score += 0.7;
  if (area && text.includes(area)) score += 0.25;
  if (zone && text.includes(zone)) score += 0.1;

  for (const alias of getAliases(landmark)) {
    if (alias && text.includes(normalizeText(alias))) score += 0.55;
  }

  if (text.includes("market") && name.includes("market")) score += 0.1;
  if ((text.includes("fuel") || text.includes("filling")) && (name.includes("fuel") || name.includes("puma"))) score += 0.1;
  if (text.includes("hospital") && name.includes("hospital")) score += 0.1;
  if (text.includes("school") && name.includes("school")) score += 0.1;
  if ((text.includes("church") || text.includes("ccap") || text.includes("parish")) && (name.includes("church") || name.includes("ccap") || name.includes("parish"))) score += 0.1;
  if ((text.includes("roundabout") || text.includes("round about")) && name.includes("roundabout")) score += 0.1;

  return Math.min(Number(score.toFixed(2)), 1);
}

function roundFare(value) {
  return Math.ceil(value / 500) * 500;
}

function estimateDistanceRange(confidence) {
  if (confidence >= 0.85) return [2, 3.5];
  if (confidence >= 0.7) return [3, 5];
  return [4, 7];
}

function estimateBikeFare(distanceRangeKm) {
  const [minKm, maxKm] = distanceRangeKm;

  const minFare = Math.max(DEFAULT_MIN_FARE, roundFare(minKm * DEFAULT_LOW_RATE_PER_KM));
  const maxFare = Math.max(minFare + 1000, roundFare(maxKm * DEFAULT_HIGH_RATE_PER_KM));

  return [minFare, maxFare];
}

function buildQuestion({ detectedArea, candidates }) {
  if (!detectedArea) {
    return "Which area is that place in? For example Area 25, Area 49, Area 36, Kanengo, City Centre, or another area?";
  }

  if (!candidates.length) {
    return "What is it close to — a market, school, filling station, church, hospital, police station, or main road?";
  }

  const names = candidates
    .slice(0, 4)
    .map(getLandmarkName)
    .filter(Boolean);

  if (!names.length) {
    return "What is it close to — a market, school, filling station, church, hospital, police station, or main road?";
  }

  return `Is it closer to ${names.join(", ")}, or another known place?`;
}

async function fetchLandmarksForMessage(message) {
  const detectedArea = detectArea(message);
  let query = "landmarks?select=*";

  // If the rider mentioned an area, try to reduce Supabase payload.
  // This assumes your landmarks table has an 'area' column. If it doesn't, the fallback below fetches all.
  if (detectedArea) {
    const encodedArea = encodeURIComponent(`*${detectedArea}*`);
    const areaQuery = `${query}&area=ilike.${encodedArea}`;

    try {
      const rows = await supabaseFetch(areaQuery);
      if (Array.isArray(rows) && rows.length > 0) return { rows, detectedArea };
    } catch (_) {
      // Fall back to all landmarks if column names differ.
    }
  }

  const rows = await supabaseFetch(query);
  return { rows, detectedArea };
}

export async function mauriceFindLocation(message) {
  if (!message || typeof message !== "string") {
    return {
      status: "needs_more_info",
      detectedArea: null,
      confidence: 0,
      candidates: [],
      question: "Please type the rider's pickup or destination description first."
    };
  }

  const { rows: landmarks, detectedArea } = await fetchLandmarksForMessage(message);

  const scored = (Array.isArray(landmarks) ? landmarks : [])
    .map((landmark) => ({
      ...landmark,
      name: getLandmarkName(landmark),
      area: getLandmarkArea(landmark),
      zone_id: getZoneId(landmark),
      confidence: scoreLandmark(message, landmark)
    }))
    .filter((landmark) => landmark.name)
    .sort((a, b) => b.confidence - a.confidence)
    .slice(0, 5);

  const best = scored[0] || null;

  if (!best || best.confidence < MIN_CONFIDENCE_TO_PRICE) {
    return {
      status: "needs_more_info",
      detectedArea,
      confidence: best?.confidence || 0,
      candidates: scored,
      question: buildQuestion({ detectedArea, candidates: scored })
    };
  }

  const estimatedDistanceKm = estimateDistanceRange(best.confidence);
  const estimatedFareMwk = estimateBikeFare(estimatedDistanceKm);

  return {
    status: "ready_to_price",
    closestLandmark: best.name,
    area: best.area || detectedArea,
    zone: best.zone_id || null,
    confidence: best.confidence,
    estimatedDistanceKm,
    estimatedFareMwk,
    candidates: scored,
    driverNote: `Customer appears closest to ${best.name}. Driver should call rider when nearby to confirm exact pickup point.`
  };
}
