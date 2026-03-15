import "dotenv/config";
import { getIO }          from "../config/socket.js";
import { sendDebitNeutralAlert } from "../services/telegramService.js";
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
} from "../services/liveDataService.js";
import getTradeModel from "../models/tradeModel.js";
import { getTradePerformanceModel } from "../models/tradePerformanceModel.js";

// ─── Live price store (upstox instrument key → ltp) ──────────────────────────
export const debitNeutralPrices = {};

// ─── Reentrancy guard ─────────────────────────────────────────────────────────
let _actionInProgress = false;

// ─── Stale feed alert deduplication ─────────────────────────────────────────
let _staleAlertSent = false;

// ─── Config ───────────────────────────────────────────────────────────────────
const SENSEX_INDEX_KEY = "BSE_INDEX|SENSEX";
const LOT_SIZE         = parseInt(process.env.DEBIT_NEUTRAL_LOT_SIZE  || "20");
const LIVE             = () => process.env.LIVE_TRADING === "true";

// SL thresholds
const BUY_LEG_SL_PCT  = 0.60;  // exit when buy leg loses 60% of entry premium
const SELL_LEG_SL_PCT = 0.60;  // exit sell leg when it loses 60% (ltp = entry × 1.60)
const OVERALL_SL_PCT  = 0.60;  // exit all when total position loses 60%

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
    day:     ist.getUTCDay(),     // 0=Sun,1=Mon,...,5=Fri,6=Sat
    hours:   ist.getUTCHours(),
    minutes: ist.getUTCMinutes(),
    date:    ist.toISOString().split("T")[0],
  };
};

const isAt320PM  = (h, m) => h > 15 || (h === 15 && m >= 20);
const isBefore325PM = (h, m) => h < 15 || (h === 15 && m < 25);

// ─── Spot price ───────────────────────────────────────────────────────────────
const getSensexSpot = async () => {
  const data = await getLTP([SENSEX_INDEX_KEY]);
  const ltp  = data?.[SENSEX_INDEX_KEY.replace("|", ":")]?.last_price
            || data?.[SENSEX_INDEX_KEY]?.last_price;
  if (ltp) return ltp;
  throw new Error("Cannot fetch SENSEX spot price from Upstox");
};

// ─── Next Friday expiry (BSE weekly expiry = Friday) ─────────────────────────
export const getNextFridayExpiry = () => {
  const now = new Date();
  const ist = new Date(now.getTime() + (5.5 * 60 * 60 * 1000));
  for (let d = 0; d <= 7; d++) {
    const dt = new Date(ist);
    dt.setUTCDate(ist.getUTCDate() + d);
    if (dt.getUTCDay() === 5) {
      return dt.toISOString().split("T")[0];
    }
  }
  throw new Error("Cannot find next Friday expiry");
};

export const selectDeltaStrikes = async (expiry) => {
  const chain = await getPCOptionChain(SENSEX_INDEX_KEY, expiry);
  if (!chain || !Array.isArray(chain)) {
    throw new Error("Empty SENSEX option chain from Upstox");
  }

  const allKeys = [];
  for (const row of chain) {
    if (row.call_options?.instrument_key) allKeys.push(row.call_options.instrument_key);
    if (row.put_options?.instrument_key)  allKeys.push(row.put_options.instrument_key);
  }

  let greeks = {};
  if (allKeys.length > 0) {
    try {
      greeks = await getOptionGreeks(allKeys) || {};
    } catch (e) {
      debitNeutralLog(`⚠️ Greeks fetch failed: ${e.message} — falling back to chain delta`, "warn");
    }
  }

  const rows = chain.map(row => {
    const callKey  = row.call_options?.instrument_key;
    const putKey   = row.put_options?.instrument_key;
    const callGreek = greeks[callKey] || greeks[callKey?.replace(":", "|")];
    const putGreek  = greeks[putKey]  || greeks[putKey?.replace(":", "|")];

    return {
      strike: row.strike_price,
      call: {
        instrumentKey:    callKey,
        tradingsymbol:    row.call_options?.tradingsymbol,
        ltp:              row.call_options?.market_data?.ltp || 0,
        delta: callGreek?.delta ?? row.call_options?.option_greeks?.delta ?? 0,
      },
      put: {
        instrumentKey:    putKey,
        tradingsymbol:    row.put_options?.tradingsymbol,
        ltp:              row.put_options?.market_data?.ltp || 0,
        delta: Math.abs(putGreek?.delta ?? row.put_options?.option_greeks?.delta ?? 0),
      },
    };
  }).filter(r => r.call.instrumentKey && r.put.instrumentKey);

  if (rows.length === 0) throw new Error("No valid option rows found in SENSEX chain");

  const callBuyRow = rows.reduce((best, r) =>
    r.call.delta > 0 && Math.abs(r.call.delta - 0.50) < Math.abs(best.call.delta - 0.50) ? r : best
  );
  const putBuyRow = rows.reduce((best, r) =>
    r.put.delta > 0 && Math.abs(r.put.delta - 0.50) < Math.abs(best.put.delta - 0.50) ? r : best
  );

  const callSellCandidates = rows.filter(r => r.strike > callBuyRow.strike && r.call.delta > 0);
  const putSellCandidates = rows.filter(r => r.strike < putBuyRow.strike && r.put.delta > 0);

  if (callSellCandidates.length === 0) throw new Error("No call sell candidates found OTM of call buy");
  if (putSellCandidates.length === 0)  throw new Error("No put sell candidates found OTM of put buy");

  const callSellRow = callSellCandidates.reduce((best, r) =>
    Math.abs(r.call.delta - 0.40) < Math.abs(best.call.delta - 0.40) ? r : best
  );
  const putSellRow = putSellCandidates.reduce((best, r) =>
    Math.abs(r.put.delta - 0.40) < Math.abs(best.put.delta - 0.40) ? r : best
  );

  const netDelta = (callBuyRow.call.delta - callSellRow.call.delta)
                 - (putBuyRow.put.delta   - putSellRow.put.delta);
  if (Math.abs(netDelta) > 0.10) {
    debitNeutralLog(
      `⚠️ Net delta = ${netDelta.toFixed(3)} (target ≈ 0) — best available strikes selected, ` +
      `position may not be perfectly delta-neutral`, "warn"
    );
  }

  return {
    callBuy: {
      strike:        callBuyRow.strike,
      instrumentKey: callBuyRow.call.instrumentKey,
      tradingsymbol: callBuyRow.call.tradingsymbol,
      ltp:           callBuyRow.call.ltp,
      delta:         callBuyRow.call.delta,
    },
    callSell: {
      strike:        callSellRow.strike,
      instrumentKey: callSellRow.call.instrumentKey,
      tradingsymbol: callSellRow.call.tradingsymbol,
      ltp:           callSellRow.call.ltp,
      delta:         callSellRow.call.delta,
    },
    putBuy: {
      strike:        putBuyRow.strike,
      instrumentKey: putBuyRow.put.instrumentKey,
      tradingsymbol: putBuyRow.put.tradingsymbol,
      ltp:           putBuyRow.put.ltp,
      delta:         putBuyRow.put.delta,
    },
    putSell: {
      strike:        putSellRow.strike,
      instrumentKey: putSellRow.put.instrumentKey,
      tradingsymbol: putSellRow.put.tradingsymbol,
      ltp:           putSellRow.put.ltp,
      delta:         putSellRow.put.delta,
    },
  };
};

const placeAndConfirmUpstox = async (instrumentKey, side, qty, tag = "") => {
  if (!LIVE()) {
    const id = `PAPER-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;
    console.log(`📝 [PAPER] ${side} ${qty} × ${instrumentKey} ${tag}`);
    return id;
  }

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

  if (res?.status !== "success") {
    throw new Error(`REJECTED: ${instrumentKey} — ${JSON.stringify(res)}`);
  }

  const orderId = res.data?.order_id;
  if (!orderId) throw new Error(`No order_id returned for ${instrumentKey}`);

  const { getUpstoxOrderApi } = await import("../config/upstoxConfig.js");
  const api = getUpstoxOrderApi();

  for (let i = 0; i < 10; i++) {
    await new Promise(r => setTimeout(r, 500));
    try {
      const orderRes = await api.getOrderDetails(process.env.UPSTOX_API_VERSION || "2.0", orderId);
      const order = orderRes?.data;
      if (!order) continue;
      if (order.status === "complete") {
        debitNeutralLog(`✅ ${side} ${instrumentKey} confirmed | orderId=${orderId}`, "info");
        return orderId;
      }
      if (order.status === "rejected") {
        throw new Error(`REJECTED: ${instrumentKey} — ${order.status_message || "broker rejected"}`);
      }
    } catch (err) {
      if (err.message.startsWith("REJECTED:")) throw err;
      console.warn(`⚠️ Phase1 poll ${i + 1}/10 ${instrumentKey}: ${err.message}`);
    }
  }

  debitNeutralLog(`⚠️ ${instrumentKey} not confirmed in 5s — entering background retry`, "warn");
  await sendDebitNeutralAlert(
    `⚠️ <b>Order slow to confirm</b>\n${side} ${instrumentKey}\nPolling every 2s — bot blocked until confirmed`
  );

  let attempt = 0;
  while (true) {
    attempt++;
    await new Promise(r => setTimeout(r, 2000));
    try {
      const orderRes = await api.getOrderDetails(process.env.UPSTOX_API_VERSION || "2.0", orderId);
      const order = orderRes?.data;
      if (!order) {
        console.warn(`⚠️ BG retry ${attempt}: ${orderId} not in order book yet`);
        continue;
      }
      if (order.status === "complete") {
        debitNeutralLog(`✅ BG retry ${attempt}: ${instrumentKey} COMPLETE`, "success");
        await sendDebitNeutralAlert(`✅ <b>Order confirmed</b> (retry ${attempt})\n${side} ${instrumentKey}`);
        return orderId;
      }
      if (order.status === "rejected") {
        throw new Error(`REJECTED: ${instrumentKey} — ${order.status_message || "broker rejected"}`);
      }
      console.warn(`⚠️ BG retry ${attempt}: ${orderId} status=${order.status}`);
    } catch (err) {
      if (err.message.startsWith("REJECTED:")) throw err;
      console.warn(`⚠️ BG retry ${attempt}: getOrderDetails error — ${err.message}`);
    }
  }
};

const subscribeLegs = (sel) => {
  [sel.callBuy, sel.callSell, sel.putBuy, sel.putSell].forEach(leg => {
    if (leg?.instrumentKey) subscribeDebitNeutralSymbol(leg.instrumentKey);
  });
};

const getLtp = (instrumentKey) => {
  if (!instrumentKey) return 0;
  return debitNeutralPrices[instrumentKey] || debitNeutralPrices[instrumentKey?.replace("|", ":")] || 0;
};

let _upstoxPositions = [];
export const updateDebitNeutralPositions = (positions) => { _upstoxPositions = positions || []; };

const getLivePnL = (trade) => {
  if (_upstoxPositions.length > 0) {
    const keys = [
      trade.instrumentKeys.callBuy,
      trade.instrumentKeys.callSell,
      trade.instrumentKeys.putBuy,
      trade.instrumentKeys.putSell,
    ].filter(Boolean);
    let pnl = 0, found = false;
    for (const key of keys) {
      const pos = _upstoxPositions.find(
        p => p.instrument_token === key || p.tradingsymbol === key
      );
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

export const enterDebitNeutral = async () => {
  const Trade = getTradeModel();

  const existing = await Trade.findOne({ status: { $in: ["ACTIVE", "EXITING"] } });
  if (existing) throw new Error("Active debit neutral already exists — exit first");

  const expiry = getNextFridayExpiry();
  const sel    = await selectDeltaStrikes(expiry);
  const qty    = LOT_SIZE;

  debitNeutralLog(
    `📐 Strikes selected | ` +
    `CallBuy: ${sel.callBuy.strike} Δ${sel.callBuy.delta.toFixed(2)} ₹${sel.callBuy.ltp} | ` +
    `CallSell: ${sel.callSell.strike} Δ${sel.callSell.delta.toFixed(2)} ₹${sel.callSell.ltp} | ` +
    `PutBuy: ${sel.putBuy.strike} Δ${sel.putBuy.delta.toFixed(2)} ₹${sel.putBuy.ltp} | ` +
    `PutSell: ${sel.putSell.strike} Δ${sel.putSell.delta.toFixed(2)} ₹${sel.putSell.ltp}`,
    "info"
  );

  subscribeLegs(sel);

  await placeAndConfirmUpstox(sel.callBuy.instrumentKey,  "BUY",  qty, "CB");
  await placeAndConfirmUpstox(sel.putBuy.instrumentKey,   "BUY",  qty, "PB");
  await placeAndConfirmUpstox(sel.callSell.instrumentKey, "SELL", qty, "CS");
  await placeAndConfirmUpstox(sel.putSell.instrumentKey,  "SELL", qty, "PS");

  const totalPremiumPaid =
    (sel.callBuy.ltp + sel.putBuy.ltp - sel.callSell.ltp - sel.putSell.ltp) * qty;

  const trade = await Trade.create({
    index:    "SENSEX",
    status:   "ACTIVE",
    quantity: qty,
    expiry,
    instrumentKeys: {
      callBuy:  sel.callBuy.instrumentKey,
      callSell: sel.callSell.instrumentKey,
      putBuy:   sel.putBuy.instrumentKey,
      putSell:  sel.putSell.instrumentKey,
    },
    symbols: {
      callBuy:  sel.callBuy.tradingsymbol,
      callSell: sel.callSell.tradingsymbol,
      putBuy:   sel.putBuy.tradingsymbol,
      putSell:  sel.putSell.tradingsymbol,
    },
    strikes: {
      callBuy:  sel.callBuy.strike,
      callSell: sel.callSell.strike,
      putBuy:   sel.putBuy.strike,
      putSell:  sel.putSell.strike,
    },
    entryPremiums: {
      callBuy:  sel.callBuy.ltp,
      callSell: sel.callSell.ltp,
      putBuy:   sel.putBuy.ltp,
      putSell:  sel.putSell.ltp,
    },
    totalPremiumPaid,
    legsAlive:    { callBuy: true, callSell: true, putBuy: true, putSell: true },
    lockedProfit: 0,
    peakProfit:   0,
    trailActive:  false,
  });

  await sendDebitNeutralAlert(
    `🟢 <b>Debit Neutral ENTERED</b> · SENSEX\n` +
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

const exitLeg = async (trade, legName) => {
  if (!trade.legsAlive[legName]) return;
  const instrumentKey = trade.instrumentKeys[legName];
  if (!instrumentKey) return;

  const isBuyLeg = legName === "callBuy" || legName === "putBuy";
  const side     = isBuyLeg ? "SELL" : "BUY";

  await placeAndConfirmUpstox(instrumentKey, side, trade.quantity, legName.toUpperCase());

  await getTradeModel().updateOne(
    { _id: trade._id },
    { $set: { [`legsAlive.${legName}`]: false } }
  );
  debitNeutralLog(`🔴 Leg exited: ${legName} (${trade.symbols[legName]}) | ${side}`, "info");
};

export const exitAllDebitNeutralLegs = async (trade, reason) => {
  const Trade = getTradeModel();
  const Perf  = getTradePerformanceModel();

  const current = await Trade.findById(trade._id);
  if (!current || current.status !== "ACTIVE") {
    console.warn(`⚠️ exitAllDebitNeutralLegs skipped — status is ${current?.status}`);
    return;
  }
  await Trade.updateOne({ _id: trade._id }, { status: "EXITING" });

  for (const leg of ["callSell", "putSell", "callBuy", "putBuy"]) {
    if (current.legsAlive[leg]) {
      try {
        await exitLeg(current, leg);
      } catch (e) {
        debitNeutralLog(`🚨 Exit leg ${leg} failed: ${e.message}`, "error");
        await new Promise(r => setTimeout(r, 2000));
        try {
          await exitLeg(current, leg);
          debitNeutralLog(`✅ Exit leg ${leg} retry succeeded`, "info");
        } catch (retryErr) {
          debitNeutralLog(`🚨 Exit leg ${leg} retry also failed: ${retryErr.message} — manual intervention needed`, "error");
          await sendDebitNeutralAlert(
            `🚨 <b>EXIT LEG FAILED</b>\nLeg: ${leg} (${current.symbols[leg]})\n` +
            `${retryErr.message}\n⚠️ Manual intervention required in Upstox`
          );
        }
      }
    }
  }

  const pnl = getLivePnL(current);
  await Trade.updateOne({ _id: trade._id }, { status: "COMPLETED" });
  await Perf.create({
    activeTradeId: trade._id,
    index: "SENSEX",
    realizedPnL: pnl,
    exitReason: reason,
  });

  const io = getIO();
  if (io) io.emit("debitNeutral:exited", { reason, pnl: pnl.toFixed(2) });

  await sendDebitNeutralAlert(
    `🔴 <b>Debit Neutral EXITED</b>\nReason: <b>${reason}</b> · PnL: <b>₹${pnl.toFixed(2)}</b>`
  );
  debitNeutralLog(`🔴 EXITED | reason=${reason} | PnL=₹${pnl.toFixed(2)}`, pnl >= 0 ? "success" : "error");
};

const updateTrail = (currentPnL, trade) => {
  const { lockedProfit, peakProfit, trailActive } = trade;

  let newLocked = lockedProfit;
  let newPeak   = Math.max(peakProfit, currentPnL);
  let newActive = trailActive;

  if (currentPnL >= 2000) {
    newLocked = 750;
    newActive = true;
  } else if (currentPnL >= 1000) {
    newLocked = Math.max(newLocked, 250);
    newActive = true;
  }

  const shouldExit = newActive && currentPnL <= newLocked;
  return { shouldExit, newLocked, newPeak, newActive };
};

const _checkConditions = async (trade) => {
  const { entryPremiums, instrumentKeys, legsAlive, quantity, totalPremiumPaid } = trade;

  const callBuyLtp  = getLtp(instrumentKeys.callBuy);
  const callSellLtp = getLtp(instrumentKeys.callSell);
  const putBuyLtp   = getLtp(instrumentKeys.putBuy);
  const putSellLtp  = getLtp(instrumentKeys.putSell);

  const livePnL = getLivePnL(trade);
  const overallLoss = -livePnL;
  const overallSLLevel = totalPremiumPaid * OVERALL_SL_PCT;

  if (overallLoss >= overallSLLevel) {
    debitNeutralLog(
      `🛑 OVERALL SL HIT | loss=₹${overallLoss.toFixed(0)} ≥ SL=₹${overallSLLevel.toFixed(0)} | exiting all`,
      "error"
    );
    await exitAllDebitNeutralLegs(trade, "OVERALL_SL");
    return;
  }

  if (legsAlive.callBuy && callBuyLtp > 0) {
    const callBuyLoss = entryPremiums.callBuy - callBuyLtp;
    const callBuySLLevel = entryPremiums.callBuy * BUY_LEG_SL_PCT;
    if (callBuyLoss >= callBuySLLevel) {
      debitNeutralLog(
        `🔴 CALL BUY SL | ltp=₹${callBuyLtp.toFixed(2)} entry=₹${entryPremiums.callBuy.toFixed(2)} ` +
        `loss=${((callBuyLoss / entryPremiums.callBuy) * 100).toFixed(0)}% | exiting call spread`,
        "error"
      );
      try { await exitLeg(trade, "callSell"); } catch (e) { debitNeutralLog(`🚨 callSell exit: ${e.message}`, "error"); }
      try { await exitLeg(trade, "callBuy");  } catch (e) { debitNeutralLog(`🚨 callBuy exit: ${e.message}`, "error"); }

      const updated = await getTradeModel().findById(trade._id).lean();
      await sendDebitNeutralAlert(
        `🔴 <b>Call Spread Exited</b> · SL hit\n` +
        `CallBuy ltp: ₹${callBuyLtp.toFixed(2)} (entry ₹${entryPremiums.callBuy.toFixed(2)})\n` +
        `Remaining: PutBuy + PutSell`
      );
      trade = updated;
      return;
    }
  }

  if (legsAlive.putBuy && putBuyLtp > 0) {
    const putBuyLoss = entryPremiums.putBuy - putBuyLtp;
    const putBuySLLevel = entryPremiums.putBuy * BUY_LEG_SL_PCT;
    if (putBuyLoss >= putBuySLLevel) {
      debitNeutralLog(
        `🔴 PUT BUY SL | ltp=₹${putBuyLtp.toFixed(2)} entry=₹${entryPremiums.putBuy.toFixed(2)} ` +
        `loss=${((putBuyLoss / entryPremiums.putBuy) * 100).toFixed(0)}% | exiting put spread`,
        "error"
      );
      try { await exitLeg(trade, "putSell"); } catch (e) { debitNeutralLog(`🚨 putSell exit: ${e.message}`, "error"); }
      try { await exitLeg(trade, "putBuy");  } catch (e) { debitNeutralLog(`🚨 putBuy exit: ${e.message}`, "error"); }

      const updated = await getTradeModel().findById(trade._id).lean();
      await sendDebitNeutralAlert(
        `🔴 <b>Put Spread Exited</b> · SL hit\n` +
        `PutBuy ltp: ₹${putBuyLtp.toFixed(2)} (entry ₹${entryPremiums.putBuy.toFixed(2)})\n` +
        `Remaining: CallBuy + CallSell`
      );
      trade = updated;
      return;
    }
  }

  if (legsAlive.callSell && callSellLtp > 0) {
    const callSellSLLevel = entryPremiums.callSell * (1 + SELL_LEG_SL_PCT);
    if (callSellLtp >= callSellSLLevel) {
      debitNeutralLog(
        `🔴 CALL SELL SL | ltp=₹${callSellLtp.toFixed(2)} SL=₹${callSellSLLevel.toFixed(2)} | exiting callSell only`,
        "error"
      );
      try { await exitLeg(trade, "callSell"); } catch (e) { debitNeutralLog(`🚨 callSell exit: ${e.message}`, "error"); }
      await sendDebitNeutralAlert(
        `🔴 <b>Call Sell Exited</b> · SL hit\n` +
        `ltp: ₹${callSellLtp.toFixed(2)} (entry ₹${entryPremiums.callSell.toFixed(2)} → SL ₹${callSellSLLevel.toFixed(2)})\n` +
        `Remaining: CallBuy + PutBuy + PutSell`
      );
    }
  }

  if (legsAlive.putSell && putSellLtp > 0) {
    const putSellSLLevel = entryPremiums.putSell * (1 + SELL_LEG_SL_PCT);
    if (putSellLtp >= putSellSLLevel) {
      debitNeutralLog(
        `🔴 PUT SELL SL | ltp=₹${putSellLtp.toFixed(2)} SL=₹${putSellSLLevel.toFixed(2)} | exiting putSell only`,
        "error"
      );
      try { await exitLeg(trade, "putSell"); } catch (e) { debitNeutralLog(`🚨 putSell exit: ${e.message}`, "error"); }
      await sendDebitNeutralAlert(
        `🔴 <b>Put Sell Exited</b> · SL hit\n` +
        `ltp: ₹${putSellLtp.toFixed(2)} (entry ₹${entryPremiums.putSell.toFixed(2)} → SL ₹${putSellSLLevel.toFixed(2)})\n` +
        `Remaining: CallBuy + CallSell + PutBuy`
      );
    }
  }

  if (livePnL > 0) {
    const { shouldExit, newLocked, newPeak, newActive } = updateTrail(livePnL, trade);

    if (newLocked !== trade.lockedProfit || newPeak !== trade.peakProfit || newActive !== trade.trailActive) {
      await getTradeModel().updateOne(
        { _id: trade._id },
        { $set: { lockedProfit: newLocked, peakProfit: newPeak, trailActive: newActive } }
      );
      if (newLocked > trade.lockedProfit) {
        debitNeutralLog(
          `📈 TRAIL LOCK UPDATED | pnl=₹${livePnL.toFixed(0)} | locked=₹${newLocked}`,
          "success"
        );
        await sendDebitNeutralAlert(
          `📈 <b>Profit Trail Locked</b>\nCurrent P&L: ₹${livePnL.toFixed(0)}\nFloor locked at: ₹${newLocked}`
        );
      }
    }

    if (shouldExit) {
      debitNeutralLog(
        `🎯 TRAIL HIT | pnl=₹${livePnL.toFixed(0)} ≤ locked=₹${newLocked} | exiting all`,
        "success"
      );
      await exitAllDebitNeutralLegs(trade, "TRAIL_HIT");
      return;
    }
  }

  const io = getIO();
  if (io) {
    io.emit("debitNeutral:monitor", {
      index:       "SENSEX",
      expiry:      trade.expiry,
      pnl:         livePnL.toFixed(2),
      lockedProfit: trade.lockedProfit,
      trailActive:  trade.trailActive,
      legsAlive:   trade.legsAlive,
      legs: {
        callBuy:  { symbol: trade.symbols.callBuy,  entry: entryPremiums.callBuy,  ltp: callBuyLtp  },
        callSell: { symbol: trade.symbols.callSell, entry: entryPremiums.callSell, ltp: callSellLtp },
        putBuy:   { symbol: trade.symbols.putBuy,   entry: entryPremiums.putBuy,   ltp: putBuyLtp   },
        putSell:  { symbol: trade.symbols.putSell,  entry: entryPremiums.putSell,  ltp: putSellLtp  },
      },
    });
  }
};

onPriceUpdate(async (instrumentKey, ltp) => {
  debitNeutralPrices[instrumentKey] = ltp;

  if (_staleAlertSent) {
    _staleAlertSent = false;
    debitNeutralLog("✅ Upstox feed RECOVERED — checks resuming", "success");
  }

  const io = getIO();
  if (io) io.emit("debitNeutral:price", { key: instrumentKey, ltp });

  if (_actionInProgress) return;

  const Trade = getTradeModel();
  const trade = await Trade.findOne({ status: "ACTIVE" }).lean().catch(() => null);
  if (!trade) return;

  _actionInProgress = true;
  try {
    await _checkConditions(trade);
  } catch (err) {
    console.error("❌ Debit Neutral tick check error:", err.message);
  } finally {
    _actionInProgress = false;
  }
});

export const debitNeutralScanAndSync = async () => {
  const Trade = getTradeModel();
  const trade = await Trade.findOne({ status: "ACTIVE" }).lean();
  if (!trade) return;

  try {
    const positions = await getPositions();
    updateDebitNeutralPositions(Array.isArray(positions) ? positions : []);
  } catch (e) {
    console.warn("⚠️ Upstox positions fetch error:", e.message);
  }

  const { day, hours, minutes } = getIST();
  if (day === 1 && isAt320PM(hours, minutes)) {
    debitNeutralLog("⏰ Monday 3:20 PM — EOD exit triggered", "warn");
    await exitAllDebitNeutralLegs(trade, "EOD_EXIT");
    return;
  }

  if (isFeedStale()) {
    if (!_staleAlertSent) {
      _staleAlertSent = true;
      const age = getLastTickAge();
      const msg = age
        ? `🚨 Upstox feed STALE (${age}s) — SL/trail checks PAUSED. Reconnecting…`
        : `🚨 Upstox feed DARK — no ticks. SL/trail checks PAUSED.`;
      debitNeutralLog(msg, "error");
    }
    return;
  }

  if (!_actionInProgress) {
    _actionInProgress = true;
    try { await _checkConditions(trade); }
    catch (e) { console.error("❌ debitNeutralScanAndSync check:", e.message); }
    finally { _actionInProgress = false; }
  }
};

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

export const resetDebitNeutralDay = () => {
  _entryAttempted = false;
  debitNeutralLog("🔄 Debit Neutral day reset", "info");
};