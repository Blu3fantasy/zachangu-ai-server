import express from "express";
import cors from "cors";
import dotenv from "dotenv";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { rateLimit } from "express-rate-limit";
import { getFirebaseAdminStatus, normalizeFirebaseDataPayload, sendFirebaseNotification } from "./firebaseAdmin.js";
dotenv.config();
const NODE_ENV = String(process.env.NODE_ENV || "development");
const IS_PROD = NODE_ENV === "production";
const PERSONA_VERSION = "2026-05-05.1";
const ENABLE_STRICT_OUTPUT_VALIDATION = process.env.ENABLE_STRICT_OUTPUT_VALIDATION !== "false";
const ENABLE_SHARED_PERSONA_CONFIG = process.env.ENABLE_SHARED_PERSONA_CONFIG !== "false";
const ENABLE_SECURE_DEBUG_ROUTES = process.env.ENABLE_SECURE_DEBUG_ROUTES === "true";
const EXPOSE_DEBUG_PAYLOADS = process.env.EXPOSE_DEBUG_PAYLOADS === "true" && !IS_PROD;
const REQUIRE_API_AUTH = process.env.REQUIRE_API_AUTH === "true";
const API_SERVER_KEY = String(process.env.AI_SERVER_API_KEY || "");
const CORS_ALLOWED_ORIGINS = String(process.env.CORS_ALLOWED_ORIGINS || "");
const GROQ_API_KEY = process.env.GROQ_API_KEY;
const GEMINI_API_KEY = process.env.GEMINI_API_KEY || process.env.GEMMA_API_KEY || "";
const TAPIWA_PROVIDER = "gemini";
const TAPIWA_MODEL = process.env.GEMINI_MODEL || process.env.GEMMA_MODEL || process.env.TAPIWA_MODEL || "gemini-2.5-flash";
const GROQ_MODEL = process.env.GROQ_MODEL || "llama-3.1-8b-instant";
const MAURICE_ENABLED = process.env.MAURICE_ENABLED !== "false";
const MAURICE_MODEL = process.env.MAURICE_MODEL || GROQ_MODEL;
const MAURICE_MAX_TOKENS = Number(process.env.MAURICE_MAX_TOKENS || 800);
const MAURICE_TEMPERATURE = Number(process.env.MAURICE_TEMPERATURE || 0.1);
const TAPIWA_MAX_OUTPUT_TOKENS = Number(process.env.TAPIWA_MAX_OUTPUT_TOKENS || 900);
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY || process.env.SUPABASE_ANON || "";
const SUPABASE_API_KEY = SUPABASE_SERVICE_ROLE_KEY || SUPABASE_ANON_KEY;
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const MEMORY_FILE = path.join(__dirname, "conversation-memory.json");
const MEMORY_TTL_MS = 1000 * 60 * 60 * 12;
const MEMORY_MAX_SESSIONS = 300;
const TABLE_CACHE_TTL_MS = Number(process.env.TABLE_CACHE_TTL_MS || 60_000);
const STATIC_TABLE_CACHE = new Set([
  "zones",
  "landmarks",
  "pricing_rules",
  "market_prices",
  "route_matrix",
  "risks",
  "tapiwa_system_settings",
  "tapiwa_market_price_rules",
  "tapiwa_route_intelligence",
  "tapiwa_zone_behavior",
  "tapiwa_route_learning"
]);
const TAPIWA_RESPONSE_SCHEMA = {
  type: "OBJECT",
  required: [
    "category",
    "risk_level",
    "internal_summary",
    "team_message",
    "requires_supervisor_approval",
    "opening_used"
  ],
  properties: {
    category: { type: "STRING" },
    risk_level: { type: "STRING" },
    internal_summary: { type: "STRING" },
    team_message: { type: "STRING" },
    requires_supervisor_approval: { type: "BOOLEAN" },
    opening_used: { type: "STRING" }
  }
};
function parseCsvList(value = "") {
  return String(value)
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
}
const allowedCorsOrigins = parseCsvList(CORS_ALLOWED_ORIGINS);
const allowAllCorsOrigins = allowedCorsOrigins.includes("*");
function isOriginAllowed(origin) {
  if (!allowedCorsOrigins.length) return true;
  if (!origin) return true;
  if (allowAllCorsOrigins) return true;
  return allowedCorsOrigins.includes(origin);
}
const app = express();
// Railway proxy fix for express-rate-limit
app.set("trust proxy", 1);
app.use(cors({
  origin(origin, callback) {
    if (isOriginAllowed(origin)) return callback(null, true);
    return callback(new Error("CORS blocked for origin."));
  }
