import { supabaseInsert } from "./supabase.service.js";
export async function saveAuditLog(payload) {
  await supabaseInsert("tapiwa_ai_audit_logs", {
    request_type: payload.request_type||"team_chat",
    user_message: payload.user_message||null,
    clean_message: payload.clean_message||null,
    ai_category: payload.ai_category||null,
    risk_level: payload.risk_level||null,
    team_message: payload.team_message||null,
    internal_summary: payload.internal_summary||null,
    used_data: payload.used_data||null,
    raw_ai_response: payload.raw_ai_response||null,
    success: payload.success!==false,
    error_message: payload.error_message||null,
    source_type: "ai_server",
    source_ref: "server.js"
  });
}
