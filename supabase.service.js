import { env } from "../config/env.js";
import { fetchWithTimeout } from "../utils/network.js";

export const tableDebug = {};

export function cleanBaseUrl() {
  return String(env.SUPABASE_URL || "").replace(/\/rest\/v1\/?$/, "").replace(/\/$/, "");
}

export async function supabaseFetch(table, limit = 20) {
  if (!env.SUPABASE_URL || !env.SUPABASE_API_KEY) {
    tableDebug[table] = { ok: false, reason: "Missing SUPABASE_URL or Supabase API key" };
    return [];
  }
  const url = `${cleanBaseUrl()}/rest/v1/${table}?select=*&limit=${limit}`;
  try {
    const response = await fetchWithTimeout(url, {
      method: "GET",
      headers: { apikey: env.SUPABASE_API_KEY, Authorization: `Bearer ${env.SUPABASE_API_KEY}`, "Content-Type": "application/json" }
    }, 10000);
    const text = await response.text();
    if (!response.ok) { tableDebug[table] = { ok: false, status: response.status, error: text }; return []; }
    const json = JSON.parse(text);
    tableDebug[table] = { ok: true, status: response.status, rows_loaded: Array.isArray(json) ? json.length : 0 };
    return Array.isArray(json) ? json : [];
  } catch (error) { tableDebug[table] = { ok: false, error: error.message }; return []; }
}

export async function supabaseInsert(table, payload) {
  if (!env.SUPABASE_URL || !env.SUPABASE_API_KEY) return null;
  const url = `${cleanBaseUrl()}/rest/v1/${table}`;
  try {
    const response = await fetchWithTimeout(url, {
      method: "POST",
      headers: { apikey: env.SUPABASE_API_KEY, Authorization: `Bearer ${env.SUPABASE_API_KEY}`, "Content-Type": "application/json", Prefer: "return=representation" },
      body: JSON.stringify(payload)
    }, 10000);
    const text = await response.text();
    if (!response.ok) { tableDebug[table] = { ok: false, status: response.status, insert_error: text }; return null; }
    return text ? JSON.parse(text) : null;
  } catch (error) { tableDebug[table] = { ok: false, insert_error: error.message }; return null; }
}
