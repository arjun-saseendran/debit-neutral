import express from "express";
import getTradeModel from "../models/tradeModel.js";
import { getTradePerformanceModel } from "../models/tradePerformanceModel.js";
import {
  enterDebitNeutral,
  exitAllDebitNeutralLegs,
  getNextFridayExpiry,
  selectDeltaStrikes,
} from "../engine/debitNeutralEngine.js";

const router = express.Router();

// ── POST /api/debit-neutral/enter — manual entry ──────────────────────────────────
router.post("/enter", async (req, res) => {
  try {
    const trade = await enterDebitNeutral();
    res.json({ success: true, tradeId: trade._id });
  } catch (err) {
    console.error("❌ /api/debit-neutral/enter:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── POST /api/debit-neutral/exit — manual full exit ───────────────────────────────
router.post("/exit", async (req, res) => {
  try {
    const { reason = "MANUAL_EXIT" } = req.body;
    const Trade = getTradeModel();
    const trade = await Trade.findOne({ status: "ACTIVE" });
    if (!trade) return res.status(404).json({ error: "No active debit neutral position" });
    await exitAllDebitNeutralLegs(trade, reason);
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── GET /api/debit-neutral/active — current active trade ─────────────────────────
router.get("/active", async (req, res) => {
  try {
    const Trade = getTradeModel();
    res.json(await Trade.findOne({ status: "ACTIVE" }) || null);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── GET /api/debit-neutral/preview — preview strikes without placing orders ────────
router.get("/preview", async (req, res) => {
  try {
    const expiry = getNextFridayExpiry();
    const sel    = await selectDeltaStrikes(expiry);
    res.json({
      index:  "SENSEX",
      expiry,
      callBuy:  { strike: sel.callBuy.strike,  delta: sel.callBuy.delta,  ltp: sel.callBuy.ltp  },
      callSell: { strike: sel.callSell.strike, delta: sel.callSell.delta, ltp: sel.callSell.ltp },
      putBuy:   { strike: sel.putBuy.strike,   delta: sel.putBuy.delta,   ltp: sel.putBuy.ltp   },
      putSell:  { strike: sel.putSell.strike,  delta: sel.putSell.delta,  ltp: sel.putSell.ltp  },
      netDebit: +(sel.callBuy.ltp + sel.putBuy.ltp - sel.callSell.ltp - sel.putSell.ltp).toFixed(2),
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── GET /api/debit-neutral/history — last 50 completed trades ────────────────────
router.get("/history", async (req, res) => {
  try {
    const Perf = getTradePerformanceModel();
    const history = await Perf.find({ strategy: "DEBIT_NEUTRAL" })
      .sort({ createdAt: -1 })
      .limit(50);
    res.json(history.map(h => ({
      exitReason: h.exitReason,
      pnl:        h.realizedPnL,
      createdAt:  h.createdAt,
    })));
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

export default router;