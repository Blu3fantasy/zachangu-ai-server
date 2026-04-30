import express from "express";
import cors from "cors";
import { env } from "./config/env.js";
import debugRoutes from "./routes/debug.routes.js";
import aiRoutes from "./routes/ai.routes.js";
import { startMemoryPersistence } from "./services/memory.service.js";

const app = express();
app.use(cors());
app.use(express.json({ limit: "1mb" }));

startMemoryPersistence();

app.get("/", (req, res) => {
  res.json({
    status: "Zachangu AI server is running",
    groq_ready: Boolean(env.GROQ_API_KEY),
    tapiwa_groq_ready: Boolean(env.TAPIWA_GROQ_API_KEY),
    supabase_ready: Boolean(env.SUPABASE_URL && env.SUPABASE_API_KEY),
    supabase_url_loaded: env.SUPABASE_URL || null,
    tapiwa_intelligence_ready: true
  });
});

app.use(debugRoutes);
app.use(aiRoutes);

app.listen(env.PORT, "0.0.0.0", () => {
  console.log(`Zachangu AI server running on port ${env.PORT}`);
});
