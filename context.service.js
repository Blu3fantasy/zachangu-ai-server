import { supabaseFetch, tableDebug } from "./supabase.service.js";
import { normalizeText } from "../utils/text.js";

export function sanitizeRouteFragment(value) {
  return String(value || "")
    .replace(/@tapiwa/gi, " ").replace(/\btapiwa\b/gi, " ")
    .replace(/\b(no|not that|wrong route|correction|i meant|meant)\b/gi, " ")
    .replace(/\?/g, " ")
    .replace(/\b(price|fare|cost|charge|quote|how much|hw much|hw mch|how mch|hwmuch|confirm|exactly|yes|please|trip|route|how many|many)\b/gi, " ")
    .replace(/\s+/g, " ").trim();
}

export function isPricingLikeMessage(message) {
  const text = normalizeText(message);
  return /price|fare|cost|charge|quote/.test(text) || /how much|hw much|hw mch|how mch|hwmuch/.test(text) || /km|kms|kilometer|kilometre|distance/.test(text) || /\bfrom\b.+\bto\b/.test(text);
}
export function isFollowUpRouteMessage(message) {
  const text = normalizeText(message);
  return /how much for that (trip|route|one|it)/.test(text) || /price for that/.test(text) || /quote that/.test(text) || /distance for that/.test(text) || /km for that/.test(text);
}
export function extractRouteParts(message) {
  const text = normalizeText(message);
  let match = text.match(/from (.+?) to (.+)/);
  if (match) return { pickupRaw: sanitizeRouteFragment(match[1]), dropoffRaw: sanitizeRouteFragment(match[2]) };
  match = text.match(/(.+?) to (.+)/);
  if (match && isPricingLikeMessage(text)) return { pickupRaw: sanitizeRouteFragment(match[1]), dropoffRaw: sanitizeRouteFragment(match[2]) };
  return null;
}
export function isExplicitRouteMessage(message) { return Boolean(extractRouteParts(message)); }
export function isCorrectionMessage(message) { return /no tapiwa|not that|i meant|meant|wrong|correction|not from|not to/.test(normalizeText(message)); }

function levenshtein(a, b) {
  a = normalizeText(a); b = normalizeText(b);
  const matrix = Array.from({ length: a.length + 1 }, () => Array(b.length + 1).fill(0));
  for (let i = 0; i <= a.length; i++) matrix[i][0] = i;
  for (let j = 0; j <= b.length; j++) matrix[0][j] = j;
  for (let i = 1; i <= a.length; i++) for (let j = 1; j <= b.length; j++) {
    const cost = a[i-1] === b[j-1] ? 0 : 1;
    matrix[i][j] = Math.min(matrix[i-1][j]+1, matrix[i][j-1]+1, matrix[i-1][j-1]+cost);
  }
  return matrix[a.length][b.length];
}
export function similarity(a, b) {
  a = normalizeText(a); b = normalizeText(b);
  if (!a || !b) return 0;
  if (a === b) return 1;
  if (a.includes(b) || b.includes(a)) return 0.88;
  return 1 - levenshtein(a, b) / Math.max(a.length, b.length);
}
function safeJsonParse(value) { if (!value || typeof value !== "string") return null; try { return JSON.parse(value); } catch { return null; } }
function uniqueTokens(value) { return [...new Set(normalizeText(value).split(" ").filter(Boolean))]; }
function routeMeaningfulTokens(value) { const ignore = new Set(["from","to","please","trip","route","how","much","price","fare","cost","quote","the"]); return uniqueTokens(value).filter(t => !ignore.has(t)); }
function qualifierTokens(value) { return routeMeaningfulTokens(value).filter(t => t !== "area" && !/^\d+$/.test(t)); }
function tokenOverlapRatio(sourceTokens, candidateTokens) { if (!sourceTokens.length || !candidateTokens.length) return 0; let overlap = 0; for (const t of sourceTokens) if (candidateTokens.includes(t)) overlap++; return overlap / sourceTokens.length; }
function scoreLandmarkField(rawName, fieldValue, fieldType = "name") {
  if (!fieldValue) return 0;
  const rawTokens = routeMeaningfulTokens(rawName), rawQualifierTokens = qualifierTokens(rawName);
  const candidateTokens = routeMeaningfulTokens(fieldValue), candidateQualifierTokens = qualifierTokens(fieldValue);
  const base = similarity(rawName, fieldValue), overlap = tokenOverlapRatio(rawTokens, candidateTokens), qualifierOverlap = tokenOverlapRatio(rawQualifierTokens, candidateQualifierTokens);
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
    const fields = [{ type:"name", value:lm.Landmark_Name }, ...aliasCandidates.map(value => ({ type:"alias", value })), ...String(lm.Nearby_Landmarks||"").split(";").map(v=>v.trim()).filter(Boolean).map(value => ({ type:"nearby", value })), { type:"area", value:lm.Area }];
    let bestFieldScore=0, bestFieldType=null, bestFieldValue=null;
    for (const field of fields) { const score = scoreLandmarkField(rawName, field.value, field.type); if (score > bestFieldScore) { bestFieldScore=score; bestFieldType=field.type; bestFieldValue=field.value; } }
    return { landmark:lm, score:bestFieldScore, source:bestFieldType, sourceValue:bestFieldValue, priority:sourcePriority[bestFieldType]||0 };
  });
  candidates.sort((a,b) => b.score !== a.score ? b.score-a.score : (b.priority||0)-(a.priority||0));
  return { match: (candidates[0]?.score||0) >= 0.55 ? candidates[0]?.landmark||null : null, score: candidates[0]?.score||0, secondScore: (candidates[1]?.score||0) + ((candidates[1]?.priority||0)*0.001), source: candidates[0]?.source||null, sourceValue: candidates[0]?.sourceValue||null };
}
export function buildRouteUnderstanding(message, landmarksRaw) {
  const parts = extractRouteParts(message);
  if (!parts) return { hasRoute:false, confidence:"low", pickup:null, dropoff:null, note:"No clear pickup/dropoff detected." };
  const pickupMatch = bestLandmarkMatch(parts.pickupRaw, landmarksRaw), dropoffMatch = bestLandmarkMatch(parts.dropoffRaw, landmarksRaw);
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
  return { hasRoute:true, confidence, pickupRaw:parts.pickupRaw, dropoffRaw:parts.dropoffRaw, pickup:pickupMatch.match, dropoff:dropoffMatch.match, pickup_score:pickupMatch.score, dropoff_score:dropoffMatch.score, pickup_source:pickupMatch.source, dropoff_source:dropoffMatch.source, note: confidence==="high" ? "Route understood clearly." : confidence==="medium" ? "Route partly understood; needs confirmation." : "Route unclear; ask for clarification." };
}

export function getKeywords(message) { const stopWords = new Set(["price","fare","cost","from","to","the","for","please","give","estimate","route","how","much","should","we","charge","what","is","that","trip"]); return normalizeText(message).split(" ").filter(w => w.length>2 && !stopWords.has(w)); }
function rowText(row) { return normalizeText(Object.values(row||{}).filter(Boolean).join(" ")); }
function scoreRow(row, keywords) { const text=rowText(row); let score=0; for (const w of keywords) if(text.includes(w)) score++; return score; }
function topMatches(rows, keywords, limit=5) { return rows.map(row=>({row,score:scoreRow(row,keywords)})).filter(i=>i.score>0).sort((a,b)=>b.score-a.score).slice(0,limit).map(i=>i.row); }

const slimLandmark = row => ({Landmark_ID:row.Landmark_ID,Landmark_Name:row.Landmark_Name,Area:row.Area,Zone_ID:row.Zone_ID,Nearby_Landmarks:row.Nearby_Landmarks});
const slimZone = row => ({Zone_ID:row.Zone_ID,Zone_Name:row.Zone_Name,Areas_Covered:row.Areas_Covered,Zone_Type:row.Zone_Type,Demand_Level:row.Demand_Level,Strategic_Role:row.Strategic_Role});
const slimPricingRule = row => ({Vehicle_Type:row.Vehicle_Type,Base_Rate_Per_KM_MWK:row.Base_Rate_Per_KM_MWK,Minimum_Fare_MWK:row.Minimum_Fare_MWK,Peak_Multiplier:row.Peak_Multiplier,Night_Multiplier:row.Night_Multiplier,Rain_Multiplier:row.Rain_Multiplier,Commission_Percent:row.Commission_Percent,Active:row.Active});
const slimMarketPrice = row => ({Route_ID:row.Route_ID,Origin_Landmark:row.Origin_Landmark,Destination_Landmark:row.Destination_Landmark,Origin_Zone:row.Origin_Zone,Destination_Zone:row.Destination_Zone,Min_Price:row.Min_Price,Max_Price:row.Max_Price,Avg_Price:row.Avg_Price,Last_Updated:row.Last_Updated});
const slimRoute = row => ({Route_Key:row.Route_Key,From_Landmark:row.From_Landmark,To_Landmark:row.To_Landmark,Distance_KM:row.Distance_KM,Time_Normal_Min:row.Time_Normal_Min,Time_Peak_Min:row.Time_Peak_Min,Zone_From:row.Zone_From,Zone_To:row.Zone_To});
const slimRisk = row => ({Zone_ID:row.Zone_ID,Rank_Name:row.Rank_Name,Problem_Description:row.Problem_Description,Category:row.Category,Risk_Points:row.Risk_Points,Universal_5_Step_Solution:row.Universal_5_Step_Solution});
export const slimTapiwaMarketRule = row => ({pickup_zone:row.pickup_zone,dropoff_zone:row.dropoff_zone,pickup_landmark:row.pickup_landmark,dropoff_landmark:row.dropoff_landmark,vehicle_type:row.vehicle_type,min_price:row.min_price,recommended_price:row.recommended_price,max_price:row.max_price,confidence:row.confidence,source_type:row.source_type,notes:row.notes});
export const slimTapiwaRouteIntel = row => ({route_name:row.route_name,pickup_landmark:row.pickup_landmark,dropoff_landmark:row.dropoff_landmark,pickup_zone:row.pickup_zone,dropoff_zone:row.dropoff_zone,distance_min_km:row.distance_min_km,distance_max_km:row.distance_max_km,typical_customer_type:row.typical_customer_type,peak_time:row.peak_time,key_concern:row.key_concern,route_behavior:row.route_behavior,pricing_notes:row.pricing_notes,driver_notes:row.driver_notes,customer_notes:row.customer_notes});
const slimTapiwaZoneBehavior = row => ({zone_code:row.zone_code,zone_name:row.zone_name,zone_type:row.zone_type,demand_level:row.demand_level,strategic_role:row.strategic_role,demand_time:row.demand_time,pricing_behavior:row.pricing_behavior,customer_behavior:row.customer_behavior,driver_behavior:row.driver_behavior,risk_notes:row.risk_notes,dispatcher_notes:row.dispatcher_notes});
const slimTapiwaRouteLearning = row => ({pickup_zone:row.pickup_zone,dropoff_zone:row.dropoff_zone,pickup_landmark:row.pickup_landmark,dropoff_landmark:row.dropoff_landmark,vehicle_type:row.vehicle_type,trip_count:row.trip_count,avg_final_price:row.avg_final_price,min_final_price:row.min_final_price,max_final_price:row.max_final_price,most_common_price:row.most_common_price,avg_tapiwa_price:row.avg_tapiwa_price,avg_override_difference:row.avg_override_difference,acceptance_rate:row.acceptance_rate});
const slimTapiwaOutcome = row => ({trip_request_id:row.trip_request_id,tapiwa_recommended_price:row.tapiwa_recommended_price,final_price:row.final_price,price_difference:row.price_difference,price_overridden:row.price_overridden,override_reason:row.override_reason,customer_accepted:row.customer_accepted,driver_accepted:row.driver_accepted,trip_completed:row.trip_completed,created_at:row.created_at});

function samePlaceName(a,b) { const l=normalizeText(a),r=normalizeText(b); return Boolean(l&&r&&l===r); }
export function routeMatchesResolvedLandmarks(route,p,d) { return route&&p&&d&&samePlaceName(route.From_Landmark,p)&&samePlaceName(route.To_Landmark,d); }
export function marketPriceMatchesResolvedLandmarks(row,p,d) { return row&&p&&d&&samePlaceName(row.Origin_Landmark,p)&&samePlaceName(row.Destination_Landmark,d); }
export function tapiwaRuleMatchesResolvedLandmarks(row,p,d) { return row&&p&&d&&samePlaceName(row.pickup_landmark,p)&&samePlaceName(row.dropoff_landmark,d); }
export function routeIntelMatchesResolvedLandmarks(row,p,d) { return row&&p&&d&&samePlaceName(row.pickup_landmark,p)&&samePlaceName(row.dropoff_landmark,d); }
export function routeIntelMatchesRawRoute(row,pRaw,dRaw) { return row&&pRaw&&dRaw&&similarity(row.pickup_landmark,pRaw)>=0.72&&similarity(row.dropoff_landmark,dRaw)>=0.72; }

async function fetchTapiwaIntelligence(userMessage) {
  const keywords = getKeywords(userMessage);
  const [settingsRaw,marketRulesRaw,routeIntelRaw,zoneBehaviorRaw,routeLearningRaw,priceOutcomesRaw] = await Promise.all([
    supabaseFetch("tapiwa_system_settings",50), supabaseFetch("tapiwa_market_price_rules",250), supabaseFetch("tapiwa_route_intelligence",250), supabaseFetch("tapiwa_zone_behavior",50), supabaseFetch("tapiwa_route_learning",150), supabaseFetch("tapiwa_price_outcomes",50)
  ]);
  const matchedMarketRules = topMatches(marketRulesRaw,keywords,8).filter(r=>r.active!==false);
  const matchedRouteIntel  = topMatches(routeIntelRaw,keywords,8).filter(r=>r.active!==false);
  const matchedZoneBehavior = topMatches(zoneBehaviorRaw,keywords,6).filter(r=>r.active!==false);
  const matchedRouteLearning = topMatches(routeLearningRaw,keywords,8);
  return { system_settings: settingsRaw, market_price_rules: matchedMarketRules.map(slimTapiwaMarketRule), route_intelligence: matchedRouteIntel.map(slimTapiwaRouteIntel), zone_behavior: matchedZoneBehavior.map(slimTapiwaZoneBehavior), route_learning: matchedRouteLearning.map(slimTapiwaRouteLearning), recent_price_outcomes: priceOutcomesRaw.slice(0,10).map(slimTapiwaOutcome), data_counts: { tapiwa_system_settings:settingsRaw.length, tapiwa_market_price_rules:marketRulesRaw.length, tapiwa_route_intelligence:routeIntelRaw.length, tapiwa_zone_behavior:zoneBehaviorRaw.length, tapiwa_route_learning:routeLearningRaw.length, tapiwa_price_outcomes:priceOutcomesRaw.length }, matched_counts: { market_price_rules:matchedMarketRules.length, route_intelligence:matchedRouteIntel.length, zone_behavior:matchedZoneBehavior.length, route_learning:matchedRouteLearning.length } };
}

export async function fetchZachanguContext(userMessage) {
  const keywords = getKeywords(userMessage);
  const [zonesRaw,landmarksRaw,pricingRulesRaw,marketPricesRaw,routeMatrixRaw,risksRaw,tapiwaIntelligence] = await Promise.all([
    supabaseFetch("zones",60), supabaseFetch("landmarks",250), supabaseFetch("pricing_rules",30), supabaseFetch("market_prices",150), supabaseFetch("route_matrix",250), supabaseFetch("risks",80), fetchTapiwaIntelligence(userMessage)
  ]);
  const matchedLandmarks = topMatches(landmarksRaw,keywords,8);
  const routeUnderstanding = buildRouteUnderstanding(userMessage, landmarksRaw);
  const matchedZones = topMatches(zonesRaw,keywords,6), matchedMarketPrices = topMatches(marketPricesRaw,keywords,8), matchedRoutes = topMatches(routeMatrixRaw,keywords,8), matchedRisks = topMatches(risksRaw,keywords,5);
  const resolvedPickupName = routeUnderstanding.pickup?.Landmark_Name||null, resolvedDropoffName = routeUnderstanding.dropoff?.Landmark_Name||null;
  const exactRouteMatches = routeUnderstanding.confidence==="high"&&resolvedPickupName&&resolvedDropoffName ? routeMatrixRaw.filter(r=>routeMatchesResolvedLandmarks(r,resolvedPickupName,resolvedDropoffName)) : [];
  const exactMarketPriceMatches = routeUnderstanding.confidence==="high"&&resolvedPickupName&&resolvedDropoffName ? marketPricesRaw.filter(r=>marketPriceMatchesResolvedLandmarks(r,resolvedPickupName,resolvedDropoffName)) : [];
  const exactTapiwaMarketRules = routeUnderstanding.confidence==="high"&&resolvedPickupName&&resolvedDropoffName ? (tapiwaIntelligence.market_price_rules||[]).filter(r=>tapiwaRuleMatchesResolvedLandmarks(r,resolvedPickupName,resolvedDropoffName)) : [];
  const exactTapiwaRouteIntel = routeUnderstanding.hasRoute ? (tapiwaIntelligence.route_intelligence||[]).filter(r=>routeIntelMatchesResolvedLandmarks(r,resolvedPickupName,resolvedDropoffName)||routeIntelMatchesRawRoute(r,routeUnderstanding.pickupRaw,routeUnderstanding.dropoffRaw)) : [];
  const relevantZoneIds = new Set();
  for (const lm of matchedLandmarks) if(lm.Zone_ID) relevantZoneIds.add(lm.Zone_ID);
  for (const r of matchedRoutes) { if(r.Zone_From) relevantZoneIds.add(r.Zone_From); if(r.Zone_To) relevantZoneIds.add(r.Zone_To); }
  for (const mp of matchedMarketPrices) { if(mp.Origin_Zone) relevantZoneIds.add(mp.Origin_Zone); if(mp.Destination_Zone) relevantZoneIds.add(mp.Destination_Zone); }
  const relevantZones = zonesRaw.filter(z=>relevantZoneIds.has(z.Zone_ID));
  const activePricingRules = pricingRulesRaw.filter(r=>String(r.Active||"").toLowerCase()==="yes").slice(0,6);
  return { request_keywords: keywords.slice(0,12), route_understanding: routeUnderstanding, zones: [...matchedZones,...relevantZones].filter((v,i,arr)=>arr.findIndex(x=>x.Zone_ID===v.Zone_ID)===i).slice(0,6).map(slimZone), landmarks: matchedLandmarks.slice(0,8).map(slimLandmark), pricing_rules: activePricingRules.length ? activePricingRules.map(slimPricingRule) : pricingRulesRaw.slice(0,4).map(slimPricingRule), market_prices: [...exactMarketPriceMatches,...matchedMarketPrices].filter((v,i,arr)=>arr.findIndex(x=>String(x.Route_ID||"")===String(v.Route_ID||"")&&String(x.Origin_Landmark||"")===String(v.Origin_Landmark||"")&&String(x.Destination_Landmark||"")===String(v.Destination_Landmark||""))===i).slice(0,8).map(slimMarketPrice), route_matrix: [...exactRouteMatches,...matchedRoutes].filter((v,i,arr)=>arr.findIndex(x=>String(x.Route_Key||"")===String(v.Route_Key||"")&&String(x.From_Landmark||"")===String(v.From_Landmark||"")&&String(x.To_Landmark||"")===String(v.To_Landmark||""))===i).slice(0,8).map(slimRoute), risks: matchedRisks.slice(0,5).map(slimRisk), tapiwa_intelligence: { ...tapiwaIntelligence, market_price_rules: exactTapiwaMarketRules.length ? exactTapiwaMarketRules.map(slimTapiwaMarketRule) : tapiwaIntelligence.market_price_rules, route_intelligence: routeUnderstanding.hasRoute ? exactTapiwaRouteIntel.map(slimTapiwaRouteIntel) : tapiwaIntelligence.route_intelligence }, data_counts: { zones:zonesRaw.length, landmarks:landmarksRaw.length, pricing_rules:pricingRulesRaw.length, market_prices:marketPricesRaw.length, route_matrix:routeMatrixRaw.length, risks:risksRaw.length, ...tapiwaIntelligence.data_counts }, matched_counts: { zones:matchedZones.length, landmarks:matchedLandmarks.length, market_prices:matchedMarketPrices.length, route_matrix:matchedRoutes.length, risks:matchedRisks.length, exact_route_matrix:exactRouteMatches.length, exact_market_prices:exactMarketPriceMatches.length, exact_tapiwa_market_rules:exactTapiwaMarketRules.length, exact_tapiwa_route_intelligence:exactTapiwaRouteIntel.length, ...tapiwaIntelligence.matched_counts }, table_status: tableDebug };
}

export function routeSnapshotFromUnderstanding(routeUnderstanding) {
  if (!routeUnderstanding || !routeUnderstanding.hasRoute || !routeUnderstanding.pickupRaw || !routeUnderstanding.dropoffRaw) return null;
  const pickupName  = routeUnderstanding.pickup?.Landmark_Name  || null;
  const dropoffName = routeUnderstanding.dropoff?.Landmark_Name || null;
  if (!pickupName && !dropoffName) return null;
  return { pickup: pickupName || routeUnderstanding.pickupRaw, dropoff: dropoffName || routeUnderstanding.dropoffRaw, pickupRaw: routeUnderstanding.pickupRaw, dropoffRaw: routeUnderstanding.dropoffRaw, confidence: routeUnderstanding.confidence };
}
export function routeToLookupMessage(route) { if (!route) return ""; return `from ${route.pickupRaw || route.pickup || ""} to ${route.dropoffRaw || route.dropoff || ""}`.trim(); }
export function takeItems(items, limit) { return Array.isArray(items) ? items.slice(0, limit) : []; }
export function buildCompactAssistantContext(context, memory, mauriceData) {
  const routeUnderstanding = context?.route_understanding || {};
  const compact = { route_understanding: { hasRoute: !!routeUnderstanding.hasRoute, confidence: routeUnderstanding.confidence || "low", pickup: routeUnderstanding.pickup?.Landmark_Name || routeUnderstanding.pickupRaw || null, dropoff: routeUnderstanding.dropoff?.Landmark_Name || routeUnderstanding.dropoffRaw || null, note: routeUnderstanding.note || null }, zones: takeItems(context?.zones, 3), landmarks: takeItems(context?.landmarks, 4), pricing_rules: takeItems(context?.pricing_rules, 2), market_prices: takeItems(context?.market_prices, 3), route_matrix: takeItems(context?.route_matrix, 3), risks: takeItems(context?.risks, 2), tapiwa_intelligence: { market_price_rules: takeItems(context?.tapiwa_intelligence?.market_price_rules, 2), route_intelligence: takeItems(context?.tapiwa_intelligence?.route_intelligence, 2), zone_behavior: takeItems(context?.tapiwa_intelligence?.zone_behavior, 2), route_learning: takeItems(context?.tapiwa_intelligence?.route_learning, 2) }, data_counts: context?.data_counts || {}, matched_counts: context?.matched_counts || {}, memory: { lastRoute: memory?.lastRoute || null, pendingConfirmation: !!memory?.pendingConfirmation, confirmedRoute: memory?.confirmedRoute || null } };
  if (mauriceData) compact.maurice = mauriceData;
  return compact;
}
