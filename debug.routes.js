import express from "express";
import { env } from "../config/env.js";
import { supabaseFetch, tableDebug } from "../services/supabase.service.js";

const router = express.Router();

router.get("/debug-ai-keys", (req, res) => {
  res.json({
    maurice_key_loaded: Boolean(env.GROQ_API_KEY),
    tapiwa_key_loaded: Boolean(env.TAPIWA_GROQ_API_KEY),
    maurice_model: env.MAURICE_MODEL,
    tapiwa_model: env.TAPIWA_MODEL,
    maurice_enabled: env.MAURICE_ENABLED
  });
});

router.get("/debug-tables", async (req, res) => {
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

export default router;
