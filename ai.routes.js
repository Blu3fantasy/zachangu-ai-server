import express from "express";
import { hasTapiwaCall, cleanTapiwaMessage } from "../utils/text.js";
import { buildSessionId, getConversationMemory, saveConversationMemory } from "../services/memory.service.js";
import { shouldUseMaurice, callMaurice } from "../services/maurice.service.js";
import { fetchZachanguContext, isFollowUpRouteMessage, isExplicitRouteMessage, isCorrectionMessage, routeToLookupMessage, buildCompactAssistantContext, routeSnapshotFromUnderstanding, routeIntelMatchesRawRoute } from "../services/context.service.js";
import { calculateBasicFare, buildLocalTapiwaFallback } from "../services/pricing.service.js";
import { saveAuditLog } from "../services/audit.service.js";
import { callTapiwaGroq, sanitizeAiResult } from "../services/tapiwa.service.js";

const router = express.Router();

router.post("/ai/analyze", async (req, res) => {
  let cleanMessage = "";
  let context = null;
  let aiResult = {};
  let mauriceData = null;

  try {
    const { message, senderName = "Dispatcher", senderRole = "Dispatcher", sessionId: rawSessionId } = req.body || {};
    if (!message || !String(message).trim()) return res.status(400).json({ error: "Message is required" });
    if (!hasTapiwaCall(message)) return res.json({ ignored: true, reason: "Tapiwa was not mentioned. Use @Tapiwa to call the AI." });

    cleanMessage = cleanTapiwaMessage(message);
    const sessionId = buildSessionId({ sessionId: rawSessionId, senderName, senderRole });
    const memory = getConversationMemory(sessionId);

    const isFollowUp = isFollowUpRouteMessage(cleanMessage);
    const hasExplicitRoute = isExplicitRouteMessage(cleanMessage);
    const isCorrection = isCorrectionMessage(cleanMessage);

    let lookupMessage = cleanMessage;
    if (isFollowUp && memory.confirmedRoute && !hasExplicitRoute) lookupMessage = routeToLookupMessage(memory.confirmedRoute);

    context = await fetchZachanguContext(lookupMessage);
    const routeUnderstanding = context.route_understanding;
    const useMaurice = shouldUseMaurice(cleanMessage);
    const compactContext = buildCompactAssistantContext(context, memory, null);

    if (useMaurice) {
      try {
        mauriceData = await callMaurice(cleanMessage, { sender: `${senderName} (${senderRole})`, context: compactContext });
      } catch (mauriceError) {
        console.warn("Maurice failed:", mauriceError.message);
        mauriceData = { intent: "unknown", missing_data: ["maurice_error"], needs_review: true, confidence: 0 };
      }
    }

    const topRouteIntel = context.tapiwa_intelligence?.route_intelligence?.[0] || null;
    if (topRouteIntel && routeUnderstanding?.hasRoute && routeIntelMatchesRawRoute(topRouteIntel, routeUnderstanding.pickupRaw, routeUnderstanding.dropoffRaw)) {
      routeUnderstanding.pickup = { Landmark_Name: topRouteIntel.pickup_landmark };
      routeUnderstanding.dropoff = { Landmark_Name: topRouteIntel.dropoff_landmark };
      routeUnderstanding.confidence = "high";
      routeUnderstanding.note = "Route resolved from Tapiwa route intelligence.";
    }

    const detectedRoute = routeSnapshotFromUnderstanding(routeUnderstanding);
    if (detectedRoute) {
      memory.lastRoute = { ...detectedRoute };
      if (isCorrection) {
        memory.confirmedRoute = detectedRoute.confidence === "high" ? { ...detectedRoute } : null;
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
    const userPayload = {
      sender: `${senderName} (${senderRole})`,
      session_id: sessionId,
      message: cleanMessage,
      memory: compactContext.memory,
      route_understanding: compactContext.route_understanding,
      computed_fare: computedFare || null,
      pricing_rules: compactContext.pricing_rules,
      market_prices: compactContext.market_prices,
      route_matrix: compactContext.route_matrix,
      tapiwa_intelligence: compactContext.tapiwa_intelligence,
      maurice: mauriceData,
      zones: compactContext.zones,
      landmarks: compactContext.landmarks,
      risks: compactContext.risks,
      data_counts: compactContext.data_counts,
      matched_counts: compactContext.matched_counts
    };

    const { missingKey, groqResponse, groqData } = await callTapiwaGroq(userPayload);
    if (missingKey) {
      const fallback = { ignored:false, category:"system_issue", risk_level:"low", internal_summary:"No Tapiwa API key", team_message:"I'm not connected to the AI engine right now — someone check the server config.", requires_supervisor_approval:false, used_data:{} };
      await saveAuditLog({ user_message:message, clean_message:cleanMessage, ai_category:"system_issue", risk_level:"low", team_message:fallback.team_message, success:false, error_message:"TAPIWA_GROQ_API_KEY not set" });
      return res.json(fallback);
    }

    if (!groqResponse.ok) {
      console.error("GROQ API ERROR:", groqResponse.status, groqData);
      const fallbackPayload = buildLocalTapiwaFallback({ cleanMessage, computedFare, routeUnderstanding, mauriceData, memory });
      const usedData = { computed_fare: computedFare, route_understanding: { confidence: routeUnderstanding.confidence, pickup: routeUnderstanding.pickup?.Landmark_Name, dropoff: routeUnderstanding.dropoff?.Landmark_Name }, maurice: mauriceData };
      await saveAuditLog({ user_message:message, clean_message:cleanMessage, success:false, error_message:"Groq API error", raw_ai_response:groqData, ai_category:fallbackPayload.category, risk_level:fallbackPayload.risk_level, team_message:fallbackPayload.team_message, internal_summary:fallbackPayload.internal_summary, used_data:usedData });
      return res.json({ ...fallbackPayload, used_data: usedData, debug_memory: { sessionId, lastRoute:memory.lastRoute, pendingConfirmation:memory.pendingConfirmation, confirmedRoute:memory.confirmedRoute }, debug_data_counts: context.data_counts, debug_matched_counts: context.matched_counts, routed_to_maurice: useMaurice, maurice: mauriceData, fallback_source: "local_tapiwa" });
    }

    try { aiResult = JSON.parse(groqData.choices?.[0]?.message?.content || "{}"); } catch { aiResult = {}; }

    const { category, riskLevel, teamMessage } = sanitizeAiResult(aiResult, cleanMessage);
    const responsePayload = {
      ignored: false,
      category,
      risk_level: riskLevel,
      internal_summary: aiResult.internal_summary || cleanMessage,
      team_message: teamMessage,
      requires_supervisor_approval: riskLevel === "high" || aiResult.requires_supervisor_approval === true,
      used_data: { computed_fare: computedFare, route_understanding: { confidence: routeUnderstanding.confidence, pickup: routeUnderstanding.pickup?.Landmark_Name, dropoff: routeUnderstanding.dropoff?.Landmark_Name }, maurice: mauriceData },
      debug_memory: { sessionId, lastRoute:memory.lastRoute, pendingConfirmation:memory.pendingConfirmation, confirmedRoute:memory.confirmedRoute },
      debug_data_counts: context.data_counts,
      debug_matched_counts: context.matched_counts,
      routed_to_maurice: useMaurice,
      maurice: mauriceData
    };

    await saveAuditLog({ request_type: "team_chat", user_message: message, clean_message: cleanMessage, ai_category: category, risk_level: riskLevel, team_message: teamMessage, internal_summary: aiResult.internal_summary || cleanMessage, used_data: responsePayload.used_data, raw_ai_response: aiResult, success: true });
    return res.json(responsePayload);
  } catch (error) {
    console.error("AI ERROR:", error);
    await saveAuditLog({ user_message: req.body?.message || null, clean_message: cleanMessage, used_data: context, raw_ai_response: aiResult, success: false, error_message: error.message });
    return res.json({ ignored: false, category: "system_issue", risk_level: "low", internal_summary: "Server error", team_message: "Something went off on my end — try that again.", requires_supervisor_approval: false, used_data: {} });
  }
});

export default router;
