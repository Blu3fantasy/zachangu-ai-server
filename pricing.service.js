import { cleanPrice, formatMwk, normalizeText } from "../utils/text.js";
import { routeMatchesResolvedLandmarks, tapiwaRuleMatchesResolvedLandmarks, isPricingLikeMessage, isExplicitRouteMessage } from "./context.service.js";

export function calculateBasicFare(context) {
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
  const distance = Number(route.Distance_KM), rate = Number(rule.Base_Rate_Per_KM_MWK), minimum = Number(rule.Minimum_Fare_MWK||3000);
  if (!distance || !rate) return null;
  const baseFare = Math.max(distance*rate, minimum, 3000);
  const low = cleanPrice(baseFare*0.95), high = cleanPrice(baseFare*1.08);
  return { source:"route_matrix_plus_pricing_rules", distance_km:distance, vehicle_type:rule.Vehicle_Type, base_rate_per_km:rate, minimum_fare:minimum, estimated_low_mwk:Math.min(low,high), estimated_high_mwk:Math.max(low,high), recommended_mwk:cleanPrice((low+high)/2), route_used:`${route.From_Landmark} → ${route.To_Landmark}` };
}

export function buildDispatchNote({ computedFare, mauriceData, routeUnderstanding }) {
  if (computedFare?.distance_km) return `Note: about ${computedFare.distance_km} km, so keep bike pricing only.`;
  if (mauriceData?.missing_data?.includes("vehicle_type")) return "Note: confirm vehicle type before you lock the fare.";
  if (routeUnderstanding?.confidence === "medium") return "Note: confirm the exact pickup before you quote finally.";
  return "Note: confirm with the driver if traffic or weather changes the run.";
}
export function buildPriceReply({ amountText, noteText, rangeText = "" }) { const head = rangeText || amountText; return noteText ? `${head}\n${noteText}` : head; }

export function buildLocalTapiwaFallback({ cleanMessage, computedFare, routeUnderstanding, mauriceData, memory }) {
  const text = normalizeText(cleanMessage);
  const greeting = /\b(hello|hi|hey|morning|goodmorning|good morning|afternoon|good afternoon|evening|good evening)\b/.test(text);
  if (greeting && !isPricingLikeMessage(cleanMessage) && !isExplicitRouteMessage(cleanMessage)) {
    return { ignored: false, category: "general_update", risk_level: "low", internal_summary: "Local greeting fallback", team_message: "Morning all! 🌞", requires_supervisor_approval: false };
  }
  if (computedFare?.recommended_mwk) {
    const amountText = formatMwk(computedFare.recommended_mwk);
    return { ignored: false, category: "pricing_issue", risk_level: "low", internal_summary: `Computed fare: ${amountText}`, team_message: buildPriceReply({ amountText, noteText: buildDispatchNote({ computedFare, mauriceData, routeUnderstanding }) }), requires_supervisor_approval: false };
  }
  if (mauriceData?.market_price_min || mauriceData?.market_price_max) {
    const min = Number(mauriceData.market_price_min || 0), max = Number(mauriceData.market_price_max || 0);
    const estimate = max ? cleanPrice((min + max) / 2) : cleanPrice(min);
    const hasClearRoute = routeUnderstanding?.confidence === "high" || Number(mauriceData.confidence || 0) >= 0.75;
    const rangeText = min && max ? `${formatMwk(min)} - ${formatMwk(max)}` : formatMwk(estimate);
    return { ignored: false, category: "pricing_issue", risk_level: hasClearRoute ? "low" : "medium", internal_summary: "Local market-price fallback", team_message: hasClearRoute ? buildPriceReply({ amountText: formatMwk(estimate), rangeText, noteText: buildDispatchNote({ computedFare, mauriceData, routeUnderstanding }) }) : buildPriceReply({ amountText: formatMwk(estimate), noteText: "Note: confirm the exact route before you quote finally." }), requires_supervisor_approval: false };
  }
  if (routeUnderstanding?.confidence === "medium" || memory?.pendingConfirmation) {
    const pickup = mauriceData?.pickup_landmark || routeUnderstanding?.pickup?.Landmark_Name || routeUnderstanding?.pickupRaw || memory?.lastRoute?.pickupRaw || "the pickup";
    const dropoff = mauriceData?.dropoff_landmark || routeUnderstanding?.dropoff?.Landmark_Name || routeUnderstanding?.dropoffRaw || memory?.lastRoute?.dropoffRaw || "the dropoff";
    return { ignored: false, category: "pricing_issue", risk_level: "medium", internal_summary: "Route needs confirmation", team_message: `Confirm for me: ${pickup} to ${dropoff}?`, requires_supervisor_approval: false };
  }
  return { ignored: false, category: "system_issue", risk_level: "low", internal_summary: "Local fallback", team_message: "I need a clearer pickup and dropoff before I quote that.", requires_supervisor_approval: false };
}
