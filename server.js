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

    const sessionId = buildSessionId({
      sessionId: rawSessionId,
      senderName,
      senderRole
    });

    const memory = getConversationMemory(sessionId);

    cleanMessage = cleanTapiwaMessage(message);

    const isAffirmation = isAffirmationMessage(cleanMessage);
    const isCorrection = isCorrectionMessage(cleanMessage);
    const isFollowUp = isFollowUpRouteMessage(cleanMessage);
    const hasExplicitRoute = isExplicitRouteMessage(cleanMessage);

    let effectiveMessage = cleanMessage;

    if (isAffirmation && memory.pendingConfirmation && memory.lastRoute) {
      memory.confirmedRoute = { ...memory.lastRoute };
      memory.pendingConfirmation = false;
      effectiveMessage = routeToMessage(memory.confirmedRoute);
    } else if (isFollowUp && memory.confirmedRoute && !hasExplicitRoute) {
      effectiveMessage = routeToMessage(memory.confirmedRoute);
    }

    context = await fetchZachanguContext(effectiveMessage);
    const routeUnderstanding = context.route_understanding;
    const detectedRoute = routeSnapshotFromUnderstanding(routeUnderstanding);

    if (isCorrection && detectedRoute) {
      memory.lastRoute = { ...detectedRoute };
      memory.pendingConfirmation = detectedRoute.confidence === "medium";
      memory.confirmedRoute =
        detectedRoute.confidence === "high" ? { ...detectedRoute } : null;
    } else if (hasExplicitRoute && detectedRoute) {
      memory.lastRoute = { ...detectedRoute };
      memory.pendingConfirmation = detectedRoute.confidence === "medium";
      memory.confirmedRoute =
        detectedRoute.confidence === "high" ? { ...detectedRoute } : null;
    } else if (isAffirmation && memory.lastRoute) {
      memory.confirmedRoute = { ...memory.lastRoute };
      memory.pendingConfirmation = false;
    }

    saveConversationMemory(sessionId, memory);

    const computedFare = calculateBasicFare(context);

    const isPricingRequest = isPricingLikeMessage(effectiveMessage);
    const hasAnyPricingData =
      context.tapiwa_intelligence.market_price_rules.length > 0 ||
      context.tapiwa_intelligence.route_intelligence.length > 0 ||
      context.tapiwa_intelligence.route_learning.length > 0 ||
      context.market_prices.length > 0 ||
      context.route_matrix.length > 0 ||
      context.pricing_rules.length > 0 ||
      computedFare;

    let forcedMessage = null;

    if (isAffirmation && memory.confirmedRoute) {
      const pickupName = memory.confirmedRoute.pickup || memory.confirmedRoute.pickupRaw;
      const dropoffName = memory.confirmedRoute.dropoff || memory.confirmedRoute.dropoffRaw;

      if (computedFare) {
        forcedMessage = `Confirmed ${pickupName} to ${dropoffName}. That trip should be around MWK ${Number(computedFare.recommended_mwk || computedFare.estimated_low_mwk).toLocaleString()} — safe range is MWK ${Number(computedFare.estimated_low_mwk).toLocaleString()} to MWK ${Number(computedFare.estimated_high_mwk).toLocaleString()}.`;
      } else {
        forcedMessage = `Confirmed ${pickupName} to ${dropoffName}. I’ve locked that route.`;
      }
    } else if (isPricingRequest && !hasAnyPricingData) {
      forcedMessage =
        "Hmm, I don't have pricing for that route yet — check the distance manually or ask someone who's done that run before quoting.";
    }

    const systemPrompt = `You are Dispatch Tapiwa, a real person on the Zachangu dispatch team in Lilongwe, Malawi.

You help the crew with operations, pricing, safety, and dispatch decisions.

You must sound human, calm, short, and useful. Do not sound like a report. Do not sound like a robot.

IMPORTANT:
- Respect confirmed_route from conversation_memory.
- If confirmed_route exists and the current message is vague, use the confirmed route.
- If pending_confirmation exists and dispatcher says yes/correct/that one, treat the last route as confirmed.
- Do not re-guess a different route after confirmation.

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
                content: JSON.stringify({
                  sender: `${senderName} (${senderRole})`,
                  session_id: sessionId,
                  msg: cleanMessage,
                  effective_msg: effectiveMessage,
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
        }
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
      const lower = effectiveMessage.toLowerCase();
      if (/price|fare|cost|charge|quote|how much|km|distance/.test(lower)) category = "pricing_issue";
      else if (/robbed|accident|threat|violence|attack|stolen|drunk|refuse/.test(lower)) category = "incident";
      else if (/driver/.test(lower)) category = "driver_issue";
      else if (/traffic|rain|roadblock|police|jam/.test(lower)) category = "traffic";
      else category = "general_update";
    }

    let riskLevel = aiResult.risk_level || "low";
    if (!allowedRiskLevels.includes(riskLevel)) riskLevel = "low";

    let fallbackMessage =
      "Alright team, noted — let's handle this and keep things moving.";

    if (category === "pricing_issue") {
      if (routeUnderstanding?.hasRoute && routeUnderstanding.confidence === "low") {
        fallbackMessage =
          "I can’t lock that route yet — send pickup and drop-off clearly.";
      } else if (routeUnderstanding?.hasRoute && routeUnderstanding.confidence === "medium") {
        const pickupName =
          routeUnderstanding.pickup?.Landmark_Name || routeUnderstanding.pickupRaw;
        const dropoffName =
          routeUnderstanding.dropoff?.Landmark_Name || routeUnderstanding.dropoffRaw;

        fallbackMessage = `I think you mean ${pickupName} to ${dropoffName} — confirm that route before I quote it.`;
      } else if (computedFare) {
        fallbackMessage = `Yeah, for ${computedFare.route_used} it should be around MWK ${Number(computedFare.recommended_mwk || computedFare.estimated_low_mwk).toLocaleString()} — safe range is MWK ${Number(computedFare.estimated_low_mwk).toLocaleString()} to MWK ${Number(computedFare.estimated_high_mwk).toLocaleString()}.`;
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
        tapiwa_market_price_rules: context.tapiwa_intelligence.market_price_rules.map((r) => r.pickup_landmark || r.pickup_zone).filter(Boolean),
        tapiwa_route_intelligence: context.tapiwa_intelligence.route_intelligence.map((r) => r.route_name).filter(Boolean),
        tapiwa_zone_behavior: context.tapiwa_intelligence.zone_behavior.map((z) => z.zone_code).filter(Boolean),
        tapiwa_route_learning: context.tapiwa_intelligence.route_learning.map((r) => `${r.pickup_landmark} → ${r.dropoff_landmark}`).filter(Boolean),
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
