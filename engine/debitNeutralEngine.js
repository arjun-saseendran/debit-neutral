/**
 * ╔══════════════════════════════════════════════════════════════════════════╗
 * ║              DEBIT NEUTRAL STRATEGY — ENGINE (SENSEX Weekly)            ║
 * ╠══════════════════════════════════════════════════════════════════════════╣
 * ║                                                                          ║
 * ║  STRATEGY LOGIC SUMMARY                                                  ║
 * ║  ─────────────────────────────────────────────────────────────────────  ║
 * ║                                                                          ║
 * ║  WHAT IS DEBIT NEUTRAL?                                                  ║
 * ║  A 4-leg options strategy combining a Bull Call Spread + Bear Put        ║
 * ║  Spread on SENSEX. Net debit is paid at entry (cost = net premium).     ║
 * ║  Profit potential: both spreads gain value when market moves strongly   ║
 * ║  in either direction. Loss is limited to the net debit paid.            ║
 * ║                                                                          ║
 * ║  LEGS (4 orders placed at entry):                                        ║
 * ║   1. CALL BUY  — ATM ~Δ0.50 call  (long, gains on upside move)         ║
 * ║   2. CALL SELL — OTM ~Δ0.40 call  (short, caps upside cost)            ║
 * ║   3. PUT BUY   — ATM ~Δ0.50 put   (long, gains on downside move)       ║
 * ║   4. PUT SELL  — OTM ~Δ0.40 put   (short, caps downside cost)          ║
 * ║                                                                          ║
 * ║  ENTRY TRIGGER:                                                          ║
 * ║   • Auto: Friday 3:20–3:25 PM IST                                       ║
 * ║   • Manual: POST /api/trade/enter                                       ║
 * ║   • Expiry: next Friday (BSE weekly SENSEX options)                     ║
 * ║   • Strikes by delta (buy ~Δ0.50, sell ~Δ0.40 OTM of buy)              ║
 * ║   • Net delta target ≈ 0; warns if |netDelta| > 0.10                    ║
 * ║                                                                          ║
 * ║  EXIT CONDITIONS (checked on every live price tick & periodic scan):    ║
 * ║   1. OVERALL SL    — Total loss ≥ 60% of net debit → exit all 4 legs   ║
 * ║   2. CALL BUY SL   — CallBuy LTP drops 60% from entry → exit call legs ║
 * ║   3. PUT BUY SL    — PutBuy LTP drops 60% from entry  → exit put legs  ║
 * ║   4. CALL SELL SL  — CallSell LTP rises 60% above entry → exit callSell║
 * ║   5. PUT SELL SL   — PutSell LTP rises 60% above entry  → exit putSell ║
 * ║   6. PROFIT TRAIL  — P&L ≥ ₹1000 → lock ₹250                           ║
 * ║                       P&L ≥ ₹2000 → lock ₹1000                          ║
 * ║                       P&L ≥ ₹3000 → lock ₹1750                          ║
 * ║                       Exit all when P&L falls to/below locked floor     ║
 * ║   7. EOD EXIT      — Monday 3:20 PM IST → unconditional full exit       ║
 * ║                                                                          ║
 * ║  ORDER PLACEMENT & CONFIRMATION (placeAndConfirmUpstox):                ║
 * ║   • MARKET order placed via Upstox OrderApi                             ║
 * ║   • PHASE 1 — fast window: polls getOrderDetails every 500 ms × 10     ║
 * ║               Returns on "complete". Throws on "rejected".              ║
 * ║   • PHASE 2 — if still pending after 5 s: infinite retry every 2 s.    ║
 * ║               Bot execution is BLOCKED until Upstox confirms or rejects.║
 * ║               Telegram alert sent when Phase 2 begins.                  ║
 * ║               Handles "order not yet in book" (null result) gracefully  ║
 * ║               by continuing to retry — never gives up on delay.         ║
 * ║   • Paper mode (LIVE_TRADING != "true"): simulates order IDs only.     ║
 * ║                                                                          ║
 * ║  LIVE DATA:                                                              ║
 * ║   • Prices via Upstox WebSocket v3 protobuf feed                       ║
 * ║   • Feed staleness (>30 s no tick) pauses SL/trail checks              ║
 * ║   • Telegram alert on stale; recovery auto-logged                       ║
 * ║   • Periodic sync also refreshes Upstox live position P&L              ║
 * ║                                                                          ║
 * ║  LOT SIZE : env DEBIT_NEUTRAL_LOT_SIZE (default 20)                     ║
 * ║  SL %     : BUY_LEG=60%  SELL_LEG=60%  OVERALL=60%                     ║
 * ╚══════════════════════════════════════════════════════════════════════════╝
 */

import "dotenv/config";
import { getIO }                    from "../config/socket.js";
import { sendDebitNeutralAlert }    from "../services/telegramService.js";
import {
  getLTP,
  getPCOptionChain,
  getOptionGreeks,
  placeOrder,
  getPositions,
} from "../config/upstoxConfig.js";
import {
  subscribeDebitNeutralSymbol,
  onPriceUpdate,
  isFeedStale,
  getLastTickAge,
  waitForOrderConfirmation,
} from "../services/liveDataService.js";
import getTradeModel               from "../models/tradeModel.js";
import { getTradePerformanceModel } from "../models/tradePerformanceModel.js";

// ─── Live price store ─────────────────────────────────────────────────────────
export const debitNeutralPrices = {};

// ─── Reentrancy guard ─────────────────────────────────────────────────────────
let _actionInProgress = false;

// ─── Stale feed alert deduplication ──────────────────────────────────────────
let _staleAlertSent = false;

// ─── Config ───────────────────────────────────────────────────────────────────
const SENSEX_INDEX_KEY = "BSE_INDEX|SENSEX";
const LOT_SIZE         = parseInt(process.env.DEBIT_NEUTRAL_LOT_SIZE || "20");
const LIVE             = () => process.env.LIVE_TRADING === "true";

const BUY_LEG_SL_PCT  = 0.60;
const SELL_LEG_SL_PCT = 0.60;
const OVERALL_SL_PCT  = 0.60;

// Order confirmation polling
const PHASE1_POLL_MS      = 500;
const PHASE1_MAX_ATTEMPTS = 10;    // 10 × 500 ms = 5 s fast window
const PHASE2_POLL_MS      = 2000;  // infinite retry every 2 s

// ─── Socket log helper ────────────────────────────────────────────────────────
const debitNeutralLog = (msg, level = "info") => {
  console.log(`[DEBIT_NEUTRAL] ${msg}`);
  try {
    const io = getIO();
    if (io) io.emit("trade_log", {
      strategy: "DEBIT_NEUTRAL",
      time: new Date().toLocaleTimeString("en-IN", { timeZone: "Asia/Kolkata" }),
      level,
      msg,
    });
  } catch (_) {}

  if (level === "info") return;
  const prefix = level === "success" ? "✅" : level === "warn" ? "⚠️" : "🚨";
  sendDebitNeutralAlert(`${prefix} <b>[Debit Neutral]</b>\n${msg}`).catch(() => {});
};

// ─── IST helpers ──────────────────────────────────────────────────────────────
const getIST = () => {
  const now = new Date();
  const ist = new Date(now.getTime() + (5.5 * 60 * 60 * 1000));
  return {
    day:     ist.getUTCDay(),
    hours:   ist.getUTCHours(),
    minutes: ist.getUTCMinutes(),
    date:    ist.toISOString().split("T")[0],
  };
};

const isAt320PM     = (h, m) => h > 15 || (h === 15 && m >= 20);
const isBefore325PM = (h, m) => h < 15 || (h === 15 && m < 25);

// ─── Spot price ───────────────────────────────────────────────────────────────
const getSensexSpot = async () => {
  const data = await getLTP([SENSEX_INDEX_KEY]);
  const ltp  = data?.[SENSEX_INDEX_KEY.replace("|", ":")]?.last_price
            || data?.[SENSEX_INDEX_KEY]?.last_price;
  if (ltp) return ltp;
  throw new Error("Cannot fetch SENSEX spot price from Upstox");
};

// ─── Next Friday expiry ───────────────────────────────────────────────────────
export const getNextFridayExpiry = () => {
  const now = new Date();
  const ist = new Date(now.getTime() + (5.5 * 60 * 60 * 1000));
  for (let d = 0; d <= 7; d++) {
    const dt = new Date(ist);
    dt.setUTCDate(ist.getUTCDate() + d);
    if (dt.getUTCDay() === 5) return dt.toISOString().split("T")[0];
  }
  throw new Error("Cannot find next Friday expiry");
};

// ─── Delta-based strike selection ────────────────────────────────────────────
export const selectDeltaStrikes = async (expiry) => {
  const chain = await getPCOptionChain(SENSEX_INDEX_KEY, expiry);
  if (!chain || !Array.isArray(chain)) throw new Error("Empty SENSEX option chain from Upstox");

  const allKeys = [];
  for (const row of chain) {
    if (row.call_options?.instrument_key) allKeys.push(row.call_options.instrument_key);
    if (row.put_options?.instrument_key)  allKeys.push(row.put_options.instrument_key);
  }

  let greeks = {};
  if (allKeys.length > 0) {
    try { greeks = await getOptionGreeks(allKeys) || {}; }
    catch (e) { debitNeutralLog(`⚠️ Greeks fetch failed: ${e.message} — using chain delta`, "warn"); }
  }

  const rows = chain.map(row => {
    const callKey   = row.call_options?.instrument_key;
    const putKey    = row.put_options?.instrument_key;
    const callGreek = greeks[callKey] || greeks[callKey?.replace(":", "|")];
    const putGreek  = greeks[putKey]  || greeks[putKey?.replace(":", "|")];
    return {
      strike: row.strike_price,
      call: {
        instrumentKey: callKey,
        tradingsymbol: row.call_options?.tradingsymbol,
        ltp:           row.call_options?.market_data?.ltp || 0,
        delta: callGreek?.delta ?? row.call_options?.option_greeks?.delta ?? 0,
      },
      put: {
        instrumentKey: putKey,
        tradingsymbol: row.put_options?.tradingsymbol,
        ltp:           row.put_options?.market_data?.ltp || 0,
        delta: Math.abs(putGreek?.delta ?? row.put_options?.option_greeks?.delta ?? 0),
      },
    };
  }).filter(r => r.call.instrumentKey && r.put.instrumentKey);

  if (rows.length === 0) throw new Error("No valid option rows found in SENSEX chain");

  const callBuyRow = rows.reduce((b, r) => r.call.delta > 0 && Math.abs(r.call.delta - 0.50) < Math.abs(b.call.delta - 0.50) ? r : b);
  const putBuyRow  = rows.reduce((b, r) => r.put.delta  > 0 && Math.abs(r.put.delta  - 0.50) < Math.abs(b.put.delta  - 0.50) ? r : b);

  const callSellCandidates = rows.filter(r => r.strike > callBuyRow.strike && r.call.delta > 0);
  const putSellCandidates  = rows.filter(r => r.strike < putBuyRow.strike  && r.put.delta  > 0);

  if (!callSellCandidates.length) throw new Error("No call sell candidates OTM of call buy");
  if (!putSellCandidates.length)  throw new Error("No put sell candidates OTM of put buy");

  const callSellRow = callSellCandidates.reduce((b, r) => Math.abs(r.call.delta - 0.40) < Math.abs(b.call.delta - 0.40) ? r : b);
  const putSellRow  = putSellCandidates.reduce( (b, r) => Math.abs(r.put.delta  - 0.40) < Math.abs(b.put.delta  - 0.40) ? r : b);

  const netDelta = (callBuyRow.call.delta - callSellRow.call.delta)
                 - (putBuyRow.put.delta   - putSellRow.put.delta);
  if (Math.abs(netDelta) > 0.10)
    debitNeutralLog(`⚠️ Net delta = ${netDelta.toFixed(3)} (target ≈ 0) — position may not be perfectly neutral`, "warn");

  return {
    callBuy:  { strike: callBuyRow.strike,  instrumentKey: callBuyRow.call.instrumentKey,  tradingsymbol: callBuyRow.call.tradingsymbol,  ltp: callBuyRow.call.ltp,  delta: callBuyRow.call.delta  },
    callSell: { strike: callSellRow.strike, instrumentKey: callSellRow.call.instrumentKey, tradingsymbol: callSellRow.call.tradingsymbol, ltp: callSellRow.call.ltp, delta: callSellRow.call.delta },
    putBuy:   { strike: putBuyRow.strike,   instrumentKey: putBuyRow.put.instrumentKey,    tradingsymbol: putBuyRow.put.tradingsymbol,    ltp: putBuyRow.put.ltp,    delta: putBuyRow.put.delta    },
    putSell:  { strike: putSellRow.strike,  instrumentKey: putSellRow.put.instrumentKey,   tradingsymbol: putSellRow.put.tradingsymbol,   ltp: putSellRow.put.ltp,   delta: putSellRow.put.delta   },
  };
};

// ─── placeAndConfirmUpstox ────────────────────────────────────────────────────
/**
 * Places a MARKET order on Upstox and BLOCKS until Upstox confirms it.
 *
 * Phase 1 — Fast window (5 s):
 *   Polls getOrderDetails every 500 ms × 10 attempts.
 *   Returns on "complete". Throws on "rejected".
 *
 * Phase 2 — Infinite retry (if delayed beyond 5 s):
 *   Retries every 2 s with NO time limit.
 *   Handles "order not in book yet" (null response) — keeps retrying.
 *   Handles transient API/network errors — keeps retrying.
 *   Sends Telegram alert so operator knows bot is blocked.
 *   Returns on "complete". Throws on "rejected".
 */
const placeAndConfirmUpstox = async (instrumentKey, side, qty, tag = "") => {
  if (!LIVE()) {
    const id = `PAPER-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;
    console.log(`📝 [PAPER] ${side} ${qty}×${instrumentKey} tag=${tag}`);
    return id;
  }

  // 1. Place order
  const res = await placeOrder({
    instrumentToken: instrumentKey,
    side,
    qty,
    orderType:  "MARKET",
    product:    "I",
    validity:   "DAY",
    price:      0,
    tag:        tag || "DEBIT_NEUTRAL",
  });

  if (res?.status !== "success")
    throw new Error(`ORDER PLACEMENT FAILED: ${instrumentKey} — ${JSON.stringify(res)}`);

  const orderId = res.data?.order_id;
  if (!orderId) throw new Error(`No order_id returned for ${instrumentKey}`);

  debitNeutralLog(`📤 Order placed | ${side} ${qty}×${instrumentKey} | orderId=${orderId}`, "info");

  // ── PRIMARY: PortfolioDataStreamer push ────────────────────────────────────
  // Upstox pushes order status as JSON on the portfolio WebSocket.
  // Resolves instantly on complete, rejects on rejected/cancelled.
  // 30s hard timeout → falls through to REST fallback.
  try {
    await waitForOrderConfirmation(orderId, 30000);
    debitNeutralLog(`✅ Confirmed via socket | ${side} ${instrumentKey} | orderId=${orderId}`, "success");
    return orderId;

  } catch (socketErr) {
    if (socketErr.message.startsWith("REJECTED:")) throw socketErr;

    // Socket timed out — fall back to REST polling
    debitNeutralLog(
      `⚠️ Socket timeout for ${orderId} — falling back to REST poll: ${socketErr.message}`,
      "warn"
    );
    await sendDebitNeutralAlert(
      `⚠️ <b>Order data delayed</b>\n` +
      `Leg: <b>${tag}</b> | Side: ${side}\n` +
      `Symbol: ${instrumentKey} | OrderId: ${orderId}\n` +
      `Upstox socket gave no push in 30s — checking via REST every 2s\n` +
      `⏳ Bot waiting for confirmation, position being managed`
    );
  }

  // ── FALLBACK: REST poll — 20 attempts × 2s = 40s ─────────────────────────
  const { getUpstoxOrderApi } = await import("../config/upstoxConfig.js");
  const api = getUpstoxOrderApi();

  for (let attempt = 1; attempt <= 20; attempt++) {
    await new Promise(r => setTimeout(r, 2000));
    try {
      const r     = await api.getOrderDetails(process.env.UPSTOX_API_VERSION || "2.0", orderId);
      const order = r?.data ?? null;

      if (!order) {
        console.warn(`⚠️ [REST fallback] attempt ${attempt}/20: ${orderId} not in order book yet`);
        continue;
      }
      if (order.status === "complete") {
        debitNeutralLog(`✅ Confirmed via REST | ${side} ${instrumentKey} | orderId=${orderId} (attempt ${attempt})`, "success");
        await sendDebitNeutralAlert(
          `✅ <b>Order confirmed via REST</b>\n` +
          `Leg: <b>${tag}</b> | Side: ${side}\n` +
          `Symbol: ${instrumentKey} | OrderId: ${orderId}\n` +
          `(Socket timeout fallback — attempt ${attempt}/20)`
        );
        return orderId;
      }
      if (order.status === "rejected")
        throw new Error(`REJECTED: ${instrumentKey} — ${order.status_message || "broker rejected"} | orderId=${orderId}`);

      console.warn(`⚠️ [REST fallback] attempt ${attempt}/20: ${orderId} status="${order.status}"`);
    } catch (err) {
      if (err.message.startsWith("REJECTED:")) throw err;
      console.warn(`⚠️ [REST fallback] attempt ${attempt}/20 error: ${err.message}`);
    }
  }

  // Both socket and REST exhausted — hard stop, manual intervention required
  await sendDebitNeutralAlert(
    `🚨 <b>Order UNCONFIRMED — Manual Action Required</b>\n` +
    `Leg: <b>${tag}</b> | Side: ${side}\n` +
    `Symbol: ${instrumentKey} | OrderId: ${orderId}\n` +
    `Socket + REST both gave no answer after ~70s\n` +
    `⚠️ Check Upstox manually — do NOT restart bot until resolved`
  );
  throw new Error(`Order ${orderId} (${instrumentKey}): Unconfirmed after socket timeout + 20 REST attempts. Manual check required.`);
};

// ─── Subscribe WebSocket for all legs ────────────────────────────────────────
const subscribeLegs = (sel) => {
  [sel.callBuy, sel.callSell, sel.putBuy, sel.putSell].forEach(leg => {
    if (leg?.instrumentKey) subscribeDebitNeutralSymbol(leg.instrumentKey);
  });
};

// ─── LTP from price store ─────────────────────────────────────────────────────
const getLtp = (key) => {
  if (!key) return 0;
  return debitNeutralPrices[key] || debitNeutralPrices[key?.replace("|", ":")] || 0;
};

// ─── Live positions cache ─────────────────────────────────────────────────────
let _upstoxPositions = [];
export const updateDebitNeutralPositions = (positions) => { _upstoxPositions = positions || []; };

// ─── Live P&L ─────────────────────────────────────────────────────────────────
const getLivePnL = (trade) => {
  if (_upstoxPositions.length > 0) {
    const keys = [
      trade.instrumentKeys.callBuy, trade.instrumentKeys.callSell,
      trade.instrumentKeys.putBuy,  trade.instrumentKeys.putSell,
    ].filter(Boolean);
    let pnl = 0, found = false;
    for (const key of keys) {
      const pos = _upstoxPositions.find(p => p.instrument_token === key || p.tradingsymbol === key);
      if (pos) { pnl += pos.pnl || pos.unrealised_profit || 0; found = true; }
    }
    if (found) return pnl;
  }
  const { entryPremiums, instrumentKeys, legsAlive, quantity } = trade;
  let pnl = 0;
  if (legsAlive.callBuy)  pnl += (getLtp(instrumentKeys.callBuy)  - entryPremiums.callBuy);
  if (legsAlive.callSell) pnl += (entryPremiums.callSell - getLtp(instrumentKeys.callSell));
  if (legsAlive.putBuy)   pnl += (getLtp(instrumentKeys.putBuy)   - entryPremiums.putBuy);
  if (legsAlive.putSell)  pnl += (entryPremiums.putSell  - getLtp(instrumentKeys.putSell));
  return pnl * quantity;
};

// ─── ENTRY ────────────────────────────────────────────────────────────────────
export const enterDebitNeutral = async () => {
  const Trade = getTradeModel();
  const existing = await Trade.findOne({ status: { $in: ["ACTIVE", "EXITING"] } });
  if (existing) throw new Error("Active debit neutral already exists — exit first");

  const expiry = getNextFridayExpiry();
  const sel    = await selectDeltaStrikes(expiry);
  const qty    = LOT_SIZE;

  debitNeutralLog(
    `📐 Strikes | CallBuy:${sel.callBuy.strike} Δ${sel.callBuy.delta.toFixed(2)} ₹${sel.callBuy.ltp} | ` +
    `CallSell:${sel.callSell.strike} Δ${sel.callSell.delta.toFixed(2)} ₹${sel.callSell.ltp} | ` +
    `PutBuy:${sel.putBuy.strike} Δ${sel.putBuy.delta.toFixed(2)} ₹${sel.putBuy.ltp} | ` +
    `PutSell:${sel.putSell.strike} Δ${sel.putSell.delta.toFixed(2)} ₹${sel.putSell.ltp}`, "info"
  );

  subscribeLegs(sel);  // subscribe before orders so LTPs start flowing

  debitNeutralLog("📤 Placing 4 legs — awaiting Upstox confirmation for each…", "info");

  await placeAndConfirmUpstox(sel.callBuy.instrumentKey,  "BUY",  qty, "CALL_BUY");
  debitNeutralLog(`✅ Leg 1/4 confirmed: CALL BUY  ${sel.callBuy.strike}CE`, "info");

  await placeAndConfirmUpstox(sel.putBuy.instrumentKey,   "BUY",  qty, "PUT_BUY");
  debitNeutralLog(`✅ Leg 2/4 confirmed: PUT BUY   ${sel.putBuy.strike}PE`, "info");

  await placeAndConfirmUpstox(sel.callSell.instrumentKey, "SELL", qty, "CALL_SELL");
  debitNeutralLog(`✅ Leg 3/4 confirmed: CALL SELL ${sel.callSell.strike}CE`, "info");

  await placeAndConfirmUpstox(sel.putSell.instrumentKey,  "SELL", qty, "PUT_SELL");
  debitNeutralLog(`✅ Leg 4/4 confirmed: PUT SELL  ${sel.putSell.strike}PE`, "info");

  const totalPremiumPaid =
    (sel.callBuy.ltp + sel.putBuy.ltp - sel.callSell.ltp - sel.putSell.ltp) * qty;

  const trade = await Trade.create({
    index: "SENSEX", status: "ACTIVE", quantity: qty, expiry,
    instrumentKeys: { callBuy: sel.callBuy.instrumentKey, callSell: sel.callSell.instrumentKey, putBuy: sel.putBuy.instrumentKey, putSell: sel.putSell.instrumentKey },
    symbols:        { callBuy: sel.callBuy.tradingsymbol,  callSell: sel.callSell.tradingsymbol,  putBuy: sel.putBuy.tradingsymbol,  putSell: sel.putSell.tradingsymbol  },
    strikes:        { callBuy: sel.callBuy.strike,         callSell: sel.callSell.strike,         putBuy: sel.putBuy.strike,         putSell: sel.putSell.strike         },
    entryPremiums:  { callBuy: sel.callBuy.ltp,            callSell: sel.callSell.ltp,            putBuy: sel.putBuy.ltp,            putSell: sel.putSell.ltp            },
    totalPremiumPaid,
    legsAlive: { callBuy: true, callSell: true, putBuy: true, putSell: true },
    lockedProfit: 0, peakProfit: 0, trailActive: false,
  });

  await sendDebitNeutralAlert(
    `🟢 <b>Debit Neutral ENTERED — All 4 legs confirmed by Upstox</b>\n` +
    `Expiry: ${expiry}\n` +
    `CallBuy:  ${sel.callBuy.strike}CE  Δ${sel.callBuy.delta.toFixed(2)}  ₹${sel.callBuy.ltp}\n` +
    `CallSell: ${sel.callSell.strike}CE  Δ${sel.callSell.delta.toFixed(2)}  ₹${sel.callSell.ltp}\n` +
    `PutBuy:   ${sel.putBuy.strike}PE  Δ${sel.putBuy.delta.toFixed(2)}  ₹${sel.putBuy.ltp}\n` +
    `PutSell:  ${sel.putSell.strike}PE  Δ${sel.putSell.delta.toFixed(2)}  ₹${sel.putSell.ltp}\n` +
    `Net debit: ₹${totalPremiumPaid.toFixed(2)} | Qty: ${qty} | Exit: Monday 3:20 PM`
  );
  debitNeutralLog(`🟢 ENTERED SENSEX | expiry=${expiry} | qty=${qty} | netDebit=₹${totalPremiumPaid.toFixed(2)}`, "success");
  return trade;
};

// ─── Single leg exit ──────────────────────────────────────────────────────────
const exitLeg = async (trade, legName) => {
  if (!trade.legsAlive[legName]) return;
  const instrumentKey = trade.instrumentKeys[legName];
  if (!instrumentKey) return;
  const isBuyLeg = legName === "callBuy" || legName === "putBuy";
  const side     = isBuyLeg ? "SELL" : "BUY";
  await placeAndConfirmUpstox(instrumentKey, side, trade.quantity, legName.toUpperCase() + "_EXIT");
  await getTradeModel().updateOne({ _id: trade._id }, { $set: { [`legsAlive.${legName}`]: false } });
  debitNeutralLog(`🔴 Leg exited & confirmed: ${legName} (${trade.symbols[legName]}) side=${side}`, "info");
};

// ─── Full exit ────────────────────────────────────────────────────────────────
export const exitAllDebitNeutralLegs = async (trade, reason) => {
  const Trade = getTradeModel();
  const Perf  = getTradePerformanceModel();

  const current = await Trade.findById(trade._id);
  if (!current || current.status !== "ACTIVE") {
    console.warn(`⚠️ exitAllDebitNeutralLegs skipped — status is ${current?.status}`); return;
  }
  await Trade.updateOne({ _id: trade._id }, { status: "EXITING" });
  debitNeutralLog(`🔴 Exiting all legs | reason=${reason}`, "warn");

  for (const leg of ["callSell", "putSell", "callBuy", "putBuy"]) {
    if (current.legsAlive[leg]) {
      try { await exitLeg(current, leg); }
      catch (e) {
        debitNeutralLog(`🚨 Exit leg ${leg} failed (attempt 1): ${e.message}`, "error");
        await new Promise(r => setTimeout(r, 2000));
        try {
          await exitLeg(current, leg);
          debitNeutralLog(`✅ Exit leg ${leg} retry succeeded`, "info");
        } catch (retryErr) {
          debitNeutralLog(`🚨 Exit leg ${leg} retry also failed: ${retryErr.message} — MANUAL INTERVENTION NEEDED`, "error");
          await sendDebitNeutralAlert(
            `🚨 <b>EXIT LEG FAILED — Manual Action Required</b>\n` +
            `Leg: ${leg} (${current.symbols[leg]})\nError: ${retryErr.message}\n` +
            `⚠️ Please close this leg manually in Upstox immediately.`
          );
        }
      }
    }
  }

  const pnl = getLivePnL(current);
  await Trade.updateOne({ _id: trade._id }, { status: "COMPLETED" });
  await Perf.create({ activeTradeId: trade._id, index: "SENSEX", realizedPnL: pnl, exitReason: reason });

  const io = getIO();
  if (io) io.emit("debitNeutral:exited", { reason, pnl: pnl.toFixed(2) });

  await sendDebitNeutralAlert(
    `🔴 <b>Debit Neutral EXITED — All legs confirmed by Upstox</b>\n` +
    `Reason: <b>${reason}</b> · PnL: <b>₹${pnl.toFixed(2)}</b>`
  );
  debitNeutralLog(`🔴 EXITED | reason=${reason} | PnL=₹${pnl.toFixed(2)}`, pnl >= 0 ? "success" : "error");
};

// ─── Profit trail ─────────────────────────────────────────────────────────────
// Profit trail locking levels:
//   P&L crosses ₹1000 → lock ₹250
//   P&L crosses ₹2000 → lock ₹1000
//   P&L crosses ₹3000 → lock ₹1750
// Each tier is independent — highest applicable lock always wins via Math.max.
// Exit triggered when trailActive and currentPnL falls to/below lockedProfit.
const TRAIL_LEVELS = [
  { trigger: 3000, lock: 1750 },
  { trigger: 2000, lock: 1000 },
  { trigger: 1000, lock:  250 },
];

const updateTrail = (currentPnL, trade) => {
  let { lockedProfit: newLocked, peakProfit, trailActive: newActive } = trade;
  let newPeak = Math.max(peakProfit, currentPnL);

  for (const { trigger, lock } of TRAIL_LEVELS) {
    if (currentPnL >= trigger) {
      newLocked = Math.max(newLocked, lock);
      newActive = true;
      break; // highest applicable tier found — lower tiers won't increase the lock
    }
  }

  return { shouldExit: newActive && currentPnL <= newLocked, newLocked, newPeak, newActive };
};

// ─── Condition check ──────────────────────────────────────────────────────────
const _checkConditions = async (trade) => {
  const { entryPremiums, instrumentKeys, legsAlive, totalPremiumPaid } = trade;

  const callBuyLtp  = getLtp(instrumentKeys.callBuy);
  const callSellLtp = getLtp(instrumentKeys.callSell);
  const putBuyLtp   = getLtp(instrumentKeys.putBuy);
  const putSellLtp  = getLtp(instrumentKeys.putSell);
  const livePnL     = getLivePnL(trade);

  // 1. Overall SL
  const overallLoss = -livePnL;
  if (overallLoss >= totalPremiumPaid * OVERALL_SL_PCT) {
    debitNeutralLog(`🛑 OVERALL SL | loss=₹${overallLoss.toFixed(0)} ≥ SL=₹${(totalPremiumPaid*OVERALL_SL_PCT).toFixed(0)} | exiting all`, "error");
    await exitAllDebitNeutralLegs(trade, "OVERALL_SL"); return;
  }

  // 2. Call buy SL → exit both call legs
  if (legsAlive.callBuy && callBuyLtp > 0) {
    const loss = entryPremiums.callBuy - callBuyLtp;
    if (loss >= entryPremiums.callBuy * BUY_LEG_SL_PCT) {
      debitNeutralLog(`🔴 CALL BUY SL | ltp=₹${callBuyLtp.toFixed(2)} loss=${((loss/entryPremiums.callBuy)*100).toFixed(0)}%`, "error");
      try { await exitLeg(trade, "callSell"); } catch (e) { debitNeutralLog(`🚨 callSell exit: ${e.message}`, "error"); }
      try { await exitLeg(trade, "callBuy");  } catch (e) { debitNeutralLog(`🚨 callBuy exit: ${e.message}`, "error"); }
      await sendDebitNeutralAlert(`🔴 <b>Call Spread Exited</b> · SL hit\nCallBuy ltp ₹${callBuyLtp.toFixed(2)} (entry ₹${entryPremiums.callBuy.toFixed(2)})\nRemaining: PutBuy + PutSell`);
      trade = await getTradeModel().findById(trade._id).lean(); return;
    }
  }

  // 3. Put buy SL → exit both put legs
  if (legsAlive.putBuy && putBuyLtp > 0) {
    const loss = entryPremiums.putBuy - putBuyLtp;
    if (loss >= entryPremiums.putBuy * BUY_LEG_SL_PCT) {
      debitNeutralLog(`🔴 PUT BUY SL | ltp=₹${putBuyLtp.toFixed(2)} loss=${((loss/entryPremiums.putBuy)*100).toFixed(0)}%`, "error");
      try { await exitLeg(trade, "putSell"); } catch (e) { debitNeutralLog(`🚨 putSell exit: ${e.message}`, "error"); }
      try { await exitLeg(trade, "putBuy");  } catch (e) { debitNeutralLog(`🚨 putBuy exit: ${e.message}`, "error"); }
      await sendDebitNeutralAlert(`🔴 <b>Put Spread Exited</b> · SL hit\nPutBuy ltp ₹${putBuyLtp.toFixed(2)} (entry ₹${entryPremiums.putBuy.toFixed(2)})\nRemaining: CallBuy + CallSell`);
      trade = await getTradeModel().findById(trade._id).lean(); return;
    }
  }

  // 4. Call sell SL → exit callSell only
  if (legsAlive.callSell && callSellLtp > 0) {
    const slLevel = entryPremiums.callSell * (1 + SELL_LEG_SL_PCT);
    if (callSellLtp >= slLevel) {
      debitNeutralLog(`🔴 CALL SELL SL | ltp=₹${callSellLtp.toFixed(2)} SL=₹${slLevel.toFixed(2)}`, "error");
      try { await exitLeg(trade, "callSell"); } catch (e) { debitNeutralLog(`🚨 callSell exit: ${e.message}`, "error"); }
      await sendDebitNeutralAlert(`🔴 <b>Call Sell Exited</b> · SL hit\nltp ₹${callSellLtp.toFixed(2)} (entry ₹${entryPremiums.callSell.toFixed(2)} → SL ₹${slLevel.toFixed(2)})\nRemaining: CallBuy + PutBuy + PutSell`);
    }
  }

  // 5. Put sell SL → exit putSell only
  if (legsAlive.putSell && putSellLtp > 0) {
    const slLevel = entryPremiums.putSell * (1 + SELL_LEG_SL_PCT);
    if (putSellLtp >= slLevel) {
      debitNeutralLog(`🔴 PUT SELL SL | ltp=₹${putSellLtp.toFixed(2)} SL=₹${slLevel.toFixed(2)}`, "error");
      try { await exitLeg(trade, "putSell"); } catch (e) { debitNeutralLog(`🚨 putSell exit: ${e.message}`, "error"); }
      await sendDebitNeutralAlert(`🔴 <b>Put Sell Exited</b> · SL hit\nltp ₹${putSellLtp.toFixed(2)} (entry ₹${entryPremiums.putSell.toFixed(2)} → SL ₹${slLevel.toFixed(2)})\nRemaining: CallBuy + CallSell + PutBuy`);
    }
  }

  // 6. Profit trail
  if (livePnL > 0) {
    const { shouldExit, newLocked, newPeak, newActive } = updateTrail(livePnL, trade);
    if (newLocked !== trade.lockedProfit || newPeak !== trade.peakProfit || newActive !== trade.trailActive) {
      await getTradeModel().updateOne({ _id: trade._id }, { $set: { lockedProfit: newLocked, peakProfit: newPeak, trailActive: newActive } });
      if (newLocked > trade.lockedProfit) {
        debitNeutralLog(`📈 TRAIL LOCK | pnl=₹${livePnL.toFixed(0)} → locked=₹${newLocked}`, "success");
        await sendDebitNeutralAlert(`📈 <b>Profit Trail Locked</b>\nP&L: ₹${livePnL.toFixed(0)} | Floor: ₹${newLocked}`);
      }
    }
    if (shouldExit) {
      debitNeutralLog(`🎯 TRAIL HIT | pnl=₹${livePnL.toFixed(0)} ≤ locked=₹${newLocked}`, "success");
      await exitAllDebitNeutralLegs(trade, "TRAIL_HIT"); return;
    }
  }

  // Emit live monitor snapshot to frontend
  const io = getIO();
  if (io) io.emit("debitNeutral:monitor", {
    index: "SENSEX", expiry: trade.expiry,
    pnl: livePnL.toFixed(2), lockedProfit: trade.lockedProfit, trailActive: trade.trailActive, legsAlive: trade.legsAlive,
    legs: {
      callBuy:  { symbol: trade.symbols.callBuy,  entry: entryPremiums.callBuy,  ltp: callBuyLtp  },
      callSell: { symbol: trade.symbols.callSell, entry: entryPremiums.callSell, ltp: callSellLtp },
      putBuy:   { symbol: trade.symbols.putBuy,   entry: entryPremiums.putBuy,   ltp: putBuyLtp   },
      putSell:  { symbol: trade.symbols.putSell,  entry: entryPremiums.putSell,  ltp: putSellLtp  },
    },
  });
};

// ─── Price tick handler ───────────────────────────────────────────────────────
onPriceUpdate(async (instrumentKey, ltp) => {
  debitNeutralPrices[instrumentKey] = ltp;

  if (_staleAlertSent) {
    _staleAlertSent = false;
    debitNeutralLog("✅ Upstox feed RECOVERED — SL/trail checks resuming", "success");
  }

  const io = getIO();
  if (io) io.emit("debitNeutral:price", { key: instrumentKey, ltp });

  if (_actionInProgress) return;

  const Trade = getTradeModel();
  const trade = await Trade.findOne({ status: "ACTIVE" }).lean().catch(() => null);
  if (!trade) return;

  _actionInProgress = true;
  try   { await _checkConditions(trade); }
  catch (err) { console.error("❌ Debit Neutral tick check error:", err.message); }
  finally { _actionInProgress = false; }
});

// ─── Periodic scan & sync ─────────────────────────────────────────────────────
export const debitNeutralScanAndSync = async () => {
  const Trade = getTradeModel();
  const trade = await Trade.findOne({ status: "ACTIVE" }).lean();
  if (!trade) return;

  try {
    const positions = await getPositions();
    updateDebitNeutralPositions(Array.isArray(positions) ? positions : []);
  } catch (e) { console.warn("⚠️ Upstox positions fetch error:", e.message); }

  const { day, hours, minutes } = getIST();

  // EOD exit on Monday 3:20 PM
  if (day === 1 && isAt320PM(hours, minutes)) {
    debitNeutralLog("⏰ Monday 3:20 PM — EOD exit triggered", "warn");
    await exitAllDebitNeutralLegs(trade, "EOD_EXIT"); return;
  }

  // Stale feed check
  if (isFeedStale()) {
    if (!_staleAlertSent) {
      _staleAlertSent = true;
      const age = getLastTickAge();
      debitNeutralLog(
        age ? `🚨 Upstox feed STALE (${age}s) — SL/trail checks PAUSED. Reconnecting…`
            : `🚨 Upstox feed DARK — no ticks. SL/trail checks PAUSED.`,
        "error"
      );
    }
    return;
  }

  if (!_actionInProgress) {
    _actionInProgress = true;
    try   { await _checkConditions(trade); }
    catch (e) { console.error("❌ debitNeutralScanAndSync check:", e.message); }
    finally { _actionInProgress = false; }
  }
};

// ─── Auto-entry on Friday 3:20–3:25 PM IST ───────────────────────────────────
let _entryAttempted = false;

export const debitNeutralAutoEnter = async () => {
  if (_entryAttempted) return;
  const { day, hours, minutes } = getIST();
  if (day !== 5) return;
  if (!isAt320PM(hours, minutes) || !isBefore325PM(hours, minutes)) return;
  _entryAttempted = true;

  const Trade = getTradeModel();
  const existing = await Trade.findOne({ status: { $in: ["ACTIVE", "EXITING"] } });
  if (existing) return;

  debitNeutralLog("🚀 Auto entry: Friday 3:20 PM — entering debit neutral", "info");
  try {
    await enterDebitNeutral();
  } catch (err) {
    _entryAttempted = false;
    debitNeutralLog(`❌ Auto entry failed: ${err.message}`, "error");
    await sendDebitNeutralAlert(`❌ <b>Debit Neutral auto-entry failed</b>\n${err.message}`);
  }
};

// ─── Daily reset ──────────────────────────────────────────────────────────────
export const resetDebitNeutralDay = () => {
  _entryAttempted = false;
  debitNeutralLog("🔄 Debit Neutral day reset", "info");
};