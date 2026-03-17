// ─── Upstox Live Data ─────────────────────────────────────────────────────────
// Two Upstox WebSocket connections initialized together:
//
//   1. Market Data Feed v3 (protobuf binary) → price ticks → debitNeutralPrices
//   2. PortfolioDataStreamer (JSON text)      → order updates → waitForOrderConfirmation()
//
// WHY TWO:
//   Upstox market data and order updates are separate WebSocket endpoints.
//   Both are initialized here so the full Upstox connection lifecycle
//   lives in one place. debitNeutralEngine never needs to know about sockets.
//
// ✅ FIX (2025): Switched from v2 → v3 WebSocket endpoint.
//         Upstox permanently retired v2 (returns HTTP 410 Gone).
//         v3 sends protobuf binary frames — decoded inline using
//         manual field parsing (no external .proto file needed).
//         URL: wss://api.upstox.com/v3/feed/market-data-feed
// ─────────────────────────────────────────────────────────────────────────────

import "dotenv/config";
import { WebSocket } from "ws";
import https from "https";
import pkg from "upstox-js-sdk";
const { PortfolioDataStreamer } = pkg;

let ws             = null;
let reconnectTimer = null;
let _is410Error    = false;   // ✅ prevent infinite reconnect on 410
const subscribedKeys = new Set();

// ─── Staleness tracking ───────────────────────────────────────────────────────
let _lastTickTime  = null;
let _feedConnected = false;

const STALE_THRESHOLD_MS = 30_000;

export const isFeedStale = () => {
  if (!_feedConnected) return true;
  if (!_lastTickTime)  return true;
  return (Date.now() - _lastTickTime) > STALE_THRESHOLD_MS;
};

export const getLastTickAge = () =>
  _lastTickTime ? Math.floor((Date.now() - _lastTickTime) / 1000) : null;

// Callback registered by debitNeutralEngine to receive price updates
let _priceCallback = null;
export const onPriceUpdate = (fn) => { _priceCallback = fn; };

// ─── Order confirmation ───────────────────────────────────────────────────────
// TWO sources can resolve a pending order — whichever arrives first wins:
//   1. Upstox Postback (HTTP POST to /api/orders/postback-debit) — most reliable
//   2. Upstox PortfolioDataStreamer (WebSocket push) — backup
//
// Both call _resolveOrder() internally.
// Hard timeout 60s → falls back to REST poll in debitNeutralEngine.
// ─────────────────────────────────────────────────────────────────────────────

// Pending order promises: Map of orderId → { resolve, reject, timer }
const _pendingOrders = new Map();

// ─── Internal: resolve or reject a pending order ─────────────────────────────
function _resolveOrder(order, source) {
  try {
    const data   = typeof order === "string" ? JSON.parse(order) : order;
    const o      = data?.data ?? data;
    const id     = String(o?.order_id ?? "");
    const status = (o?.status ?? "").toLowerCase();

    if (!id || !_pendingOrders.has(id)) return;

    if (status === "complete") {
      const { resolve, timer } = _pendingOrders.get(id);
      clearTimeout(timer);
      _pendingOrders.delete(id);
      console.log(`✅ [${source}] Order ${id} complete`);
      resolve({ orderId: id });

    } else if (status === "rejected" || status === "cancelled") {
      const { reject, timer } = _pendingOrders.get(id);
      clearTimeout(timer);
      _pendingOrders.delete(id);
      const reason = o?.status_message || status;
      console.error(`❌ [${source}] Order ${id} ${status}: ${reason}`);
      reject(new Error(`REJECTED: ${o?.tradingsymbol ?? id} — ${reason}`));
    }
    // OPEN / UPDATE / pending → Upstox will push again when final
  } catch (e) {
    console.warn(`⚠️ [${source}] Failed to parse order update:`, e.message);
  }
}

// ─── PUBLIC: called by debit-neutral server.js postback route ────────────────
// Upstox POSTs order updates to /api/orders/postback-debit when orders fill.
// This is the PRIMARY confirmation method — independent of WebSocket.
export const resolveOrderFromPostback = (order) => {
  const o  = order?.data ?? order;
  const id = String(o?.order_id ?? "");
  console.log(`📬 [Postback] order_id=${id} status=${o?.status}`);
  _resolveOrder(order, "Postback");
};

// ─── PUBLIC: called by debitNeutralEngine's placeAndConfirmUpstox() ──────────
// Register BEFORE placing the order so postback/socket cannot be missed.
// Hard timeout 60s → falls back to REST poll in debitNeutralEngine.
export const waitForOrderConfirmation = (orderId, timeoutMs = 60000) => {
  return new Promise((resolve, reject) => {
    const id = String(orderId);

    const timer = setTimeout(() => {
      _pendingOrders.delete(id);
      reject(new Error(`Order ${id}: No confirmation from Upstox in ${timeoutMs / 1000}s (postback + socket both silent)`));
    }, timeoutMs);

    _pendingOrders.set(id, { resolve, reject, timer });
  });
};

// ─── Internal: handle order update pushed by PortfolioDataStreamer ────────────
// BACKUP — fires if postback didn't arrive first.
// Both can fire safely — _resolveOrder() ignores already-resolved orders.
function _handleOrderUpdate(msg) {
  _resolveOrder(msg, "PortfolioStream");
}

// ─── Fetch authorised WebSocket URL from Upstox ──────────────────────────────
const _getStreamerUrl = (token) =>
  new Promise((resolve, reject) => {
    const options = {
      hostname: "api.upstox.com",
      path:     "/v3/feed/market-data-feed/authorize",
      method:   "GET",
      headers:  {
        Authorization: `Bearer ${token}`,
        Accept:        "application/json",
        "Api-Version": "2.0",
      },
    };

    const req = https.request(options, (res) => {
      let body = "";
      res.on("data", (chunk) => (body += chunk));
      res.on("end", () => {
        try {
          const json = JSON.parse(body);
          const url  = json?.data?.authorizedRedirectUri;
          if (url) resolve(url);
          else reject(new Error(`No authorizedRedirectUri in response: ${body}`));
        } catch (e) {
          reject(e);
        }
      });
    });

    req.on("error", reject);
    req.end();
  });

// ─── Protobuf binary decoder ──────────────────────────────────────────────────
const _readVarint = (buf, pos) => {
  let result = 0n, shift = 0n;
  while (pos < buf.length) {
    const byte = BigInt(buf[pos++]);
    result |= (byte & 0x7fn) << shift;
    shift += 7n;
    if (!(byte & 0x80n)) break;
  }
  return { value: result, pos };
};

const _decodeProtobuf = (buf) => {
  const prices = {};
  try {
    let pos = 0;
    while (pos < buf.length) {
      const tagResult  = _readVarint(buf, pos);
      pos              = tagResult.pos;
      const tag        = tagResult.value;
      const fieldNum   = tag >> 3n;
      const wireType   = Number(tag & 0x7n);

      if (wireType === 2) {
        const lenResult = _readVarint(buf, pos);
        pos             = lenResult.pos;
        const len       = Number(lenResult.value);
        const slice     = buf.slice(pos, pos + len);
        pos            += len;

        if (fieldNum === 1n) {
          _parseMapEntry(slice, prices);
        }
      } else if (wireType === 0) {
        const v = _readVarint(buf, pos); pos = v.pos;
      } else if (wireType === 1) {
        pos += 8;
      } else if (wireType === 5) {
        pos += 4;
      } else {
        break;
      }
    }
  } catch (_) {}
  return prices;
};

const _parseMapEntry = (buf, prices) => {
  let key = null, ltp = null;
  let pos = 0;
  while (pos < buf.length) {
    const tagR   = _readVarint(buf, pos); pos = tagR.pos;
    const tag    = tagR.value;
    const field  = tag >> 3n;
    const wire   = Number(tag & 0x7n);

    if (wire === 2) {
      const lenR  = _readVarint(buf, pos); pos = lenR.pos;
      const len   = Number(lenR.value);
      const slice = buf.slice(pos, pos + len);
      pos        += len;

      if (field === 1n) {
        key = slice.toString("utf8");
      } else if (field === 2n) {
        ltp = _extractLtp(slice);
      }
    } else if (wire === 0) {
      const v = _readVarint(buf, pos); pos = v.pos;
    } else if (wire === 1) { pos += 8; }
      else if (wire === 5) { pos += 4; }
      else break;
  }
  if (key && ltp !== null) prices[key] = ltp;
};

const _extractLtp = (buf) => {
  try {
    const ff       = _getField(buf, 1n, 2);
    if (!ff) return null;
    const marketFF = _getField(ff,  1n, 2);
    if (!marketFF) return null;
    const ltpc     = _getField(marketFF, 1n, 2);
    if (!ltpc) return null;
    return _getDouble(ltpc, 1n);
  } catch (_) { return null; }
};

const _getField = (buf, fieldNum, wireType) => {
  let pos = 0;
  while (pos < buf.length) {
    const tagR  = _readVarint(buf, pos); pos = tagR.pos;
    const tag   = tagR.value;
    const fNum  = tag >> 3n;
    const wType = Number(tag & 0x7n);

    if (wType === 2) {
      const lenR  = _readVarint(buf, pos); pos = lenR.pos;
      const len   = Number(lenR.value);
      const slice = buf.slice(pos, pos + len);
      pos        += len;
      if (fNum === fieldNum && wireType === 2) return slice;
    } else if (wType === 0) {
      const v = _readVarint(buf, pos); pos = v.pos;
      if (fNum === fieldNum && wireType === 0) return v.value;
    } else if (wType === 1) {
      const slice = buf.slice(pos, pos + 8); pos += 8;
      if (fNum === fieldNum && wireType === 1) return slice;
    } else if (wType === 5) { pos += 4; }
      else break;
  }
  return null;
};

const _getDouble = (buf, fieldNum) => {
  const slice = _getField(buf, fieldNum, 1);
  if (!slice || slice.length < 8) return null;
  return slice.readDoubleBE(0) || Buffer.from(slice).readDoubleLE(0);
};

// ─── Connect ──────────────────────────────────────────────────────────────────
export const initUpstoxLiveData = async () => {
  const token = process.env.UPSTOX_ACCESS_TOKEN;
  if (!token) {
    console.warn("⚠️ UPSTOX_ACCESS_TOKEN missing — live data disabled");
    return;
  }

  // ── 1. Market data feed (price ticks — protobuf binary) ───────────────────
  await _connect(token);

  // ── 2. Portfolio order stream (order updates — JSON text) ─────────────────
  // PortfolioDataStreamer from upstox-js-sdk uses the token already set on
  // ApiClient.instance — no extra auth needed here.
  console.log("🔌 Connecting Upstox PortfolioDataStreamer (order updates)...");
  const portfolio = new PortfolioDataStreamer(true, false, false, false);

  portfolio.on("open", () => {
    console.log("✅ Upstox PortfolioDataStreamer connected — order updates live");
  });

  portfolio.on("message", (data) => {
    const msg = data.toString("utf-8");
    _handleOrderUpdate(msg);
  });

  portfolio.on("close", () => {
    console.warn("⚠️ Upstox PortfolioDataStreamer closed — auto-reconnecting...");
  });

  portfolio.on("error", (err) => {
    console.error("❌ Upstox PortfolioDataStreamer error:", err?.message ?? err);
  });

  portfolio.autoReconnect(true, 5, 20);
  portfolio.connect();
};

const _connect = async (token) => {
  if (ws) { try { ws.terminate(); } catch (_) {} }

  let wsUrl;
  try {
    wsUrl = await _getStreamerUrl(token);
    console.log("✅ Upstox streamer URL obtained");
  } catch (err) {
    console.error("❌ Failed to get streamer URL:", err.message);
    wsUrl = "wss://api.upstox.com/v3/feed/market-data-feed";
  }

  ws = new WebSocket(wsUrl, {
    headers: {
      Authorization: `Bearer ${token}`,
      "Api-Version": "2.0",
    },
  });

  ws.binaryType = "nodebuffer";

  ws.on("open", () => {
    console.log("✅ Upstox WebSocket connected (v3)");
    _feedConnected = true;
    _is410Error    = false;
    if (subscribedKeys.size > 0) _sendSubscribe([...subscribedKeys]);
  });

  ws.on("message", (data) => {
    try {
      const buf    = Buffer.isBuffer(data) ? data : Buffer.from(data);
      const prices = _decodeProtobuf(buf);

      for (const [key, ltp] of Object.entries(prices)) {
        if (ltp && _priceCallback) {
          _lastTickTime = Date.now();
          _priceCallback(key, ltp);
        }
      }
    } catch (err) {
      console.warn("⚠️ Upstox WS decode error:", err.message);
    }
  });

  ws.on("close", (code) => {
    _feedConnected = false;
    if (_is410Error) {
      console.error("❌ Upstox WS: endpoint gone (410). Check API access / token. Not reconnecting.");
      return;
    }
    console.warn(`⚠️ Upstox WS closed (${code}) — reconnecting in 5s`);
    clearTimeout(reconnectTimer);
    reconnectTimer = setTimeout(() => _connect(token), 5000);
  });

  ws.on("error", (err) => {
    _feedConnected = false;
    if (err.message?.includes("410")) {
      _is410Error = true;
      console.error("❌ Upstox WS 410 Gone — v3 endpoint unreachable. Check token/subscription.");
    } else {
      console.error("❌ Upstox WS error:", err.message);
    }
  });
};

// ─── Subscribe ────────────────────────────────────────────────────────────────
const _sendSubscribe = (keys) => {
  if (!ws || ws.readyState !== WebSocket.OPEN) return;
  ws.send(JSON.stringify({
    guid:            `sub-${Date.now()}`,
    method:          "sub",
    data_type:       "full",
    instrument_keys: keys,
  }));
};

export const subscribeDebitNeutralSymbol = (upstoxKey) => {
  if (!upstoxKey) return;
  if (!subscribedKeys.has(upstoxKey)) {
    subscribedKeys.add(upstoxKey);
    _sendSubscribe([upstoxKey]);
  }
};

export const subscribeMany = (upstoxKeys) => {
  const newKeys = upstoxKeys.filter(k => k && !subscribedKeys.has(k));
  if (newKeys.length === 0) return;
  newKeys.forEach(k => subscribedKeys.add(k));
  _sendSubscribe(newKeys);
};