// ─── Telegram Service — Debit Neutral ────────────────────────────────────────
// Sends alerts to the configured TELEGRAM_BOT_TOKEN + TELEGRAM_CHAT_ID.
// Can be overridden with DEBIT_NEUTRAL_TELEGRAM_CHAT_ID for a separate channel.
// ─────────────────────────────────────────────────────────────────────────────

import "dotenv/config";

const BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN;
const CHAT_ID   = process.env.DEBIT_NEUTRAL_TELEGRAM_CHAT_ID
               || process.env.TELEGRAM_CHAT_ID;

export const sendDebitNeutralAlert = async (message) => {
  if (!BOT_TOKEN || !CHAT_ID) {
    console.warn("⚠️ Telegram not configured for debit neutral alerts (TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID missing)");
    return;
  }
  try {
    const res = await fetch(
      `https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`,
      {
        method:  "POST",
        headers: { "Content-Type": "application/json" },
        body:    JSON.stringify({
          chat_id:    CHAT_ID,
          text:       message,
          parse_mode: "HTML",
        }),
      }
    );
    if (!res.ok) {
      const body = await res.text();
      console.error("❌ Telegram debit neutral alert error:", body);
    }
  } catch (err) {
    console.error("❌ Telegram debit neutral alert exception:", err.message);
  }
};