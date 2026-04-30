export function hasTapiwaCall(message) { return /@tapiwa/i.test(String(message || "")); }
export function cleanTapiwaMessage(message) { return String(message || "").replace(/@tapiwa/gi, "").trim(); }
export function roundToNearest100(value) { return Math.round(Number(value || 0) / 100) * 100; }
export function cleanPrice(value) { return Math.max(3000, roundToNearest100(value)); }
export function normalizeText(value) { return String(value || "").toLowerCase().replace(/[^\w\s]/g, " ").replace(/\s+/g, " ").trim(); }
export function formatMwk(value) { const amount = Number(value || 0); if (!amount) return null; return `MWK ${amount.toLocaleString("en-US")}`; }
