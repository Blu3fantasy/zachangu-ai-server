import dotenv from "dotenv";
import path from "path";
import { fileURLToPath } from "url";

dotenv.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const rootDir = path.resolve(__dirname, "..");

export const env = {
  GROQ_API_KEY: process.env.GROQ_API_KEY,
  TAPIWA_GROQ_API_KEY: process.env.TAPIWA_GROQ_API_KEY || process.env.GROQ_API_KEY,
  GROQ_MODEL: process.env.GROQ_MODEL || "llama-3.1-8b-instant",
  TAPIWA_MODEL: process.env.TAPIWA_MODEL || process.env.GROQ_MODEL || "llama-3.1-8b-instant",
  MAURICE_ENABLED: process.env.MAURICE_ENABLED !== "false",
  MAURICE_MODEL: process.env.MAURICE_MODEL || process.env.GROQ_MODEL || "llama-3.1-8b-instant",
  MAURICE_MAX_TOKENS: Number(process.env.MAURICE_MAX_TOKENS || 800),
  MAURICE_TEMPERATURE: Number(process.env.MAURICE_TEMPERATURE || 0.1),
  SUPABASE_URL: process.env.SUPABASE_URL,
  SUPABASE_SERVICE_ROLE_KEY: process.env.SUPABASE_SERVICE_ROLE_KEY,
  SUPABASE_ANON_KEY: process.env.SUPABASE_ANON_KEY || process.env.SUPABASE_ANON || "",
  PORT: process.env.PORT || 3001,
  ROOT_DIR: rootDir
};

env.SUPABASE_API_KEY = env.SUPABASE_SERVICE_ROLE_KEY || env.SUPABASE_ANON_KEY;
