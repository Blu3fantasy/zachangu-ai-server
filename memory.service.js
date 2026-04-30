import fs from "fs";
import path from "path";
import { env } from "../config/env.js";

const MEMORY_FILE = path.join(env.ROOT_DIR, "conversation-memory.json");
const MEMORY_TTL_MS = 1000 * 60 * 60 * 12;
const MEMORY_MAX_SESSIONS = 300;
const conversationMemory = new Map();

export function buildSessionId({ sessionId, senderName, senderRole }) {
  return String(sessionId || `${senderRole || "Dispatcher"}:${senderName || "unknown"}`).trim();
}

export function pruneConversationMemory() {
  const now = Date.now();
  for (const [sessionId, memory] of conversationMemory.entries()) {
    if (!memory?.updatedAt || now - memory.updatedAt > MEMORY_TTL_MS) conversationMemory.delete(sessionId);
  }
  if (conversationMemory.size <= MEMORY_MAX_SESSIONS) return;
  const oldestFirst = Array.from(conversationMemory.entries()).sort((a, b) => Number(a[1]?.updatedAt || 0) - Number(b[1]?.updatedAt || 0));
  while (oldestFirst.length && conversationMemory.size > MEMORY_MAX_SESSIONS) {
    const [sessionId] = oldestFirst.shift();
    conversationMemory.delete(sessionId);
  }
}

export function saveConversationMemoryToDisk() {
  try {
    pruneConversationMemory();
    fs.writeFileSync(MEMORY_FILE, JSON.stringify(Object.fromEntries(conversationMemory.entries()), null, 2), "utf8");
  } catch (error) { console.warn("Conversation memory save failed:", error.message); }
}

export function loadConversationMemoryFromDisk() {
  try {
    if (!fs.existsSync(MEMORY_FILE)) return;
    const payload = JSON.parse(fs.readFileSync(MEMORY_FILE, "utf8") || "{}");
    for (const [sessionId, memory] of Object.entries(payload || {})) {
      if (!sessionId || !memory || typeof memory !== "object") continue;
      conversationMemory.set(sessionId, {
        lastRoute: memory.lastRoute || null,
        pendingConfirmation: Boolean(memory.pendingConfirmation),
        confirmedRoute: memory.confirmedRoute || null,
        lastAssistTopic: memory.lastAssistTopic || null,
        updatedAt: Number(memory.updatedAt || Date.now())
      });
    }
    pruneConversationMemory();
  } catch (error) { console.warn("Conversation memory load failed:", error.message); }
}

export function getConversationMemory(sessionId) {
  pruneConversationMemory();
  if (!conversationMemory.has(sessionId)) {
    conversationMemory.set(sessionId, { lastRoute: null, pendingConfirmation: false, confirmedRoute: null, lastAssistTopic: null, updatedAt: Date.now() });
  }
  return conversationMemory.get(sessionId);
}

export function saveConversationMemory(sessionId, memory) {
  conversationMemory.set(sessionId, {
    lastRoute: memory.lastRoute || null,
    pendingConfirmation: Boolean(memory.pendingConfirmation),
    confirmedRoute: memory.confirmedRoute || null,
    lastAssistTopic: memory.lastAssistTopic || null,
    updatedAt: Date.now()
  });
  saveConversationMemoryToDisk();
}

export function startMemoryPersistence() {
  loadConversationMemoryFromDisk();
  setInterval(saveConversationMemoryToDisk, 1000 * 60 * 5).unref?.();
}
