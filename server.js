  });
}

async function markPushTokensInactive(tokens = []) {
  const uniqueTokens = Array.from(new Set((tokens || []).map((token) => String(token || "").trim()).filter(Boolean)));
  const updated = [];
  for (const token of uniqueTokens) {
    const result = await supabaseWrite("user_push_tokens", "PATCH", {
      active: false,
      updated_at: new Date().toISOString()
    }, {
      filters: [{ column: "device_token", operator: "eq", value: token }]
    });
    if (!result.error) updated.push(token);
  }
  clearTableCache("user_push_tokens");
  return updated;
}

app.post("/notifications/register-token", requireApiAuth, async (req, res) => {
  try {
    const deviceToken = String(req.body?.device_token || req.body?.token || "").trim();
    const userEmail = String(req.body?.user_email || req.body?.email || "").trim().toLowerCase();
    const role = String(req.body?.role || "").trim().toLowerCase();
    const platform = String(req.body?.platform || "android").trim().toLowerCase() || "android";
    if (!deviceToken || !userEmail || !role) {
      return res.status(400).json({ ok: false, error: "device_token, user_email, and role are required." });
    }
    const now = new Date().toISOString();
    const row = {
      user_email: userEmail,
      role,
      device_token: deviceToken,
      platform,
      active: true,
      updated_at: now
    };
    const existing = await supabaseSelect("user_push_tokens", {
      limit: 5,
      filters: [{ column: "device_token", operator: "eq", value: deviceToken }]
    });
    if (existing.error) {
      return res.status(500).json({ ok: false, error: existing.error });
    }
    const rows = Array.isArray(existing.data) ? existing.data : [];
    if (rows.length) {
      const primary = rows[0];
      const updateResult = await supabaseWrite("user_push_tokens", "PATCH", row, {
        filters: [{ column: "id", operator: "eq", value: primary.id }]
      });
      if (updateResult.error) {
        return res.status(500).json({ ok: false, error: updateResult.error });
      }
      if (rows.length > 1) {
        await markPushTokensInactive(rows.slice(1).map((item) => item.device_token).filter(Boolean));
      }
      return res.json({ ok: true, operation: "update", data: Array.isArray(updateResult.data) ? updateResult.data[0] || primary : updateResult.data || primary });
    }
    const insertResult = await supabaseWrite("user_push_tokens", "POST", [{ ...row, created_at: now }]);
    if (insertResult.error) {
      return res.status(500).json({ ok: false, error: insertResult.error });
    }
    clearTableCache("user_push_tokens");
    return res.json({ ok: true, operation: "insert", data: Array.isArray(insertResult.data) ? insertResult.data[0] || null : insertResult.data });
  } catch (error) {
    return res.status(500).json({ ok: false, error: error.message || "Failed to register push token." });
  }
});

app.post("/notifications/deactivate-token", requireApiAuth, async (req, res) => {
  try {
    const deviceToken = String(req.body?.device_token || req.body?.token || "").trim();
    if (!deviceToken) {
      return res.status(400).json({ ok: false, error: "device_token is required." });
    }
    const updated = await markPushTokensInactive([deviceToken]);
    return res.json({ ok: true, deactivated: updated });
  } catch (error) {
    return res.status(500).json({ ok: false, error: error.message || "Failed to deactivate push token." });
  }
});

app.post("/notifications/publish", requireApiAuth, async (req, res) => {
  try {
    const rows = Array.isArray(req.body?.rows) ? req.body.rows : [];
    if (!rows.length) {
      return res.status(400).json({ ok: false, error: "rows are required." });
    }
    const sanitizedRows = rows
      .map((row) => ({
        recipient_id: row?.recipient_id || null,
        recipient_role: row?.recipient_role || null,
        type: String(row?.type || "").trim().toLowerCase() || null,
        title: String(row?.title || "").trim() || null,
        body: String(row?.body || "").trim() || null,
        entity_id: row?.entity_id || null,
        entity_type: row?.entity_type || null,
        priority: String(row?.priority || "normal").trim().toLowerCase() || "normal",
        is_read: !!row?.is_read,
        read_at: row?.read_at || null,
        created_at: row?.created_at || new Date().toISOString(),
        idempotency_key: row?.idempotency_key || null
      }))
      .filter((row) => row.type && row.title && row.body);
    if (!sanitizedRows.length) {
      return res.status(400).json({ ok: false, error: "No valid notification rows were provided." });
    }
    const result = await supabaseWrite("notifications", "POST", sanitizedRows, { onConflict: "idempotency_key" });
    if (result.error) {
      return res.status(500).json({ ok: false, error: result.error });
    }
    return res.json({ ok: true, data: Array.isArray(result.data) ? result.data : [] });
  } catch (error) {
    return res.status(500).json({ ok: false, error: error.message || "Failed to publish notifications." });
  }
});

app.post("/notifications/send", requireApiAuth, async (req, res) => {
  try {
    const title = String(req.body?.title || "").trim();
    const body = String(req.body?.body || "").trim();
    if (!title || !body) {
      return res.status(400).json({ ok: false, error: "title and body are required." });
    }
    const target = normalizeNotificationTarget(req.body);
    const tokensTable = await supabaseFetch("user_push_tokens", 5000, { bypassCache: true });
    let tokenRows = Array.isArray(tokensTable) ? tokensTable.filter((row) => row && row.active !== false && String(row.platform || "android").toLowerCase() === "android") : [];
    if (target.type === "role") {
      tokenRows = tokenRows.filter((row) => String(row.role || "").trim().toLowerCase() === target.role);
    } else if (target.type === "email") {
      tokenRows = tokenRows.filter((row) => String(row.user_email || "").trim().toLowerCase() === target.email);
    }
    const tokens = tokenRows.map((row) => String(row.device_token || "").trim()).filter(Boolean);
    const data = buildPushResponseData(req.body);
    const sendResult = await sendFirebaseNotification({ tokens, title, body, data });
    if (!sendResult.ok) {
      return res.status(503).json({ ok: false, error: sendResult.error, target, token_count: tokens.length });
    }
    const deactivatedTokens = sendResult.invalidTokens?.length ? await markPushTokensInactive(sendResult.invalidTokens) : [];
    return res.json({
      ok: true,
      title,
      body,
      target,
      token_count: tokens.length,
      success_count: sendResult.successCount || 0,
      failure_count: sendResult.failureCount || 0,
      deactivated_tokens: deactivatedTokens,
      results: sendResult.responses || []
    });
  } catch (error) {
    return res.status(500).json({ ok: false, error: error.message || "Failed to send notification." });
  }
});

if (ENABLE_SECURE_DEBUG_ROUTES) {
  app.get("/debug-ai-keys", requireApiAuth, (req, res) => {
    res.json({
      gemini_key_loaded: Boolean(GEMINI_API_KEY),
      maurice_key_loaded: Boolean(GROQ_API_KEY),
      maurice_model: MAURICE_MODEL,
      tapiwa_model: TAPIWA_MODEL,
      maurice_enabled: MAURICE_ENABLED,
      require_api_auth: REQUIRE_API_AUTH,
      strict_output_validation: ENABLE_STRICT_OUTPUT_VALIDATION
    });
  });

  app.get("/env-check", requireApiAuth, (req, res) => {
    res.json({
      gemini_api_key_loaded: Boolean(GEMINI_API_KEY),
      gemini_model: TAPIWA_MODEL,
      maurice_groq_ready: Boolean(GROQ_API_KEY),
      supabase_ready: Boolean(SUPABASE_URL && SUPABASE_API_KEY),
      persona_version: PERSONA_VERSION
    });
  });

  app.get("/debug-tables", requireApiAuth, async (req, res) => {
    const tables = [
      "zones","landmarks","pricing_rules","market_prices","route_matrix","risks",
      "drivers","trip_requests","tapiwa_system_settings","tapiwa_market_price_rules",
      "tapiwa_route_intelligence","tapiwa_zone_behavior","tapiwa_route_learning",
      "tapiwa_price_outcomes","tapiwa_ai_audit_logs"
    ];
    const result = {};
    for (const table of tables) {
      const data = await supabaseFetch(table, 3, { bypassCache: true });
      result[table] = { rows_loaded: data.length, last_status: tableDebug[table] || null, sample: data.slice(0, 1) };
    }
    res.json(result);
  });
}

// â”€â”€ UTILS â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

function hasTapiwaCall(message) { return /@tapiwa/i.test(String(message || "")); }
function cleanTapiwaMessage(message) { return String(message || "").replace(/@tapiwa/gi, "").trim(); }
function cleanBaseUrl() { return String(SUPABASE_URL || "").replace(/\/rest\/v1\/?$/, "").replace(/\/$/, ""); }
function roundToNearest100(value) { return Math.round(Number(value || 0) / 100) * 100; }
function cleanPrice(value) { return Math.max(3000, roundToNearest100(value)); }

function buildSessionId({ sessionId, senderName, senderRole }) {
  return String(sessionId || `${senderRole || "Dispatcher"}:${senderName || "unknown"}`).trim();
}

function pruneConversationMemory() {
  const now = Date.now();
  for (const [sessionId, memory] of conversationMemory.entries()) {
    if (!memory?.updatedAt || now - memory.updatedAt > MEMORY_TTL_MS) {
      conversationMemory.delete(sessionId);
    }
  }

  if (conversationMemory.size <= MEMORY_MAX_SESSIONS) return;

  const oldestFirst = Array.from(conversationMemory.entries())
    .sort((a, b) => Number(a[1]?.updatedAt || 0) - Number(b[1]?.updatedAt || 0));

  while (oldestFirst.length && conversationMemory.size > MEMORY_MAX_SESSIONS) {
    const [sessionId] = oldestFirst.shift();
    conversationMemory.delete(sessionId);
  }
}

async function saveConversationMemoryToDisk() {
  if (memorySaveInFlight) return;
  memorySaveInFlight = true;
  try {
    pruneConversationMemory();
    const payload = Object.fromEntries(conversationMemory.entries());
    await fs.promises.writeFile(MEMORY_FILE, JSON.stringify(payload, null, 2), "utf8");
  } catch (error) {
    console.warn("Conversation memory save failed:", error.message);
  } finally {
    memorySaveInFlight = false;
  }
}

function scheduleConversationMemorySave() {
  if (memorySaveTimer) clearTimeout(memorySaveTimer);
  memorySaveTimer = setTimeout(() => {
    memorySaveTimer = null;
    saveConversationMemoryToDisk().catch(() => {});
  }, 250);
}

function loadConversationMemoryFromDisk() {
  try {
    if (!fs.existsSync(MEMORY_FILE)) return;
