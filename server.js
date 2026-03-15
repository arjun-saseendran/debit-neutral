import "dotenv/config";
import express from "express";
import http from "http";
import { Server } from "socket.io";
import cors from "cors";
import cron from "node-cron";

// ─── Config & DB ──────────────────────────────────────────────────────────────
import { connectDatabases } from "./config/db.js";
import {
  setUpstoxAccessToken,
  loadTokenFromDB,
} from "./config/upstoxConfig.js";
import { setIO as setSocketIO } from "./config/socket.js";

// ─── Routes ───────────────────────────────────────────────────────────────────
import debitNeutralRoutes from "./routes/tradeRoutes.js";
import authRoutes from './routes/authRoutes.js'

// ─── Services ─────────────────────────────────────────────────────────────────
import { sendDebitNeutralAlert } from "./services/telegramService.js";
import { initUpstoxLiveData } from "./services/liveDataService.js";

// ─── Engine ───────────────────────────────────────────────────────────────────
import {
  debitNeutralScanAndSync,
  debitNeutralAutoEnter,
  resetDebitNeutralDay,
} from "./engine/debitNeutralEngine.js";

// ─────────────────────────────────────────────────────────────────────────────
const app = express();
const server = http.createServer(app);

// ─── CORS ─────────────────────────────────────────────────────────────────────
const ORIGINS = [
  "https://mariaalgo.online",
  "https://www.mariaalgo.online",
  "https://api.mariaalgo.online",
  process.env.CLIENT_ORIGIN || "http://localhost:5173",
  "http://localhost:3003",
];

app.use(
  cors({
    origin: ORIGINS,
    credentials: true,
    methods: ["GET", "POST", "OPTIONS"],
    allowedHeaders: ["Content-Type", "Authorization"],
  }),
);
app.use(express.json());

// ─── Socket.IO ────────────────────────────────────────────────────────────────
const io = new Server(server, {
  cors: { origin: ORIGINS, methods: ["GET", "POST"], credentials: true },
});
setSocketIO(io);
io.on("connection", (socket) => {
  console.log(`🔌 Dashboard connected: ${socket.id}`);
  socket.on("disconnect", () =>
    console.log(`🔌 Dashboard disconnected: ${socket.id}`),
  );
});

// ─── Routes ───────────────────────────────────────────────────────────────────
app.use("/api/debit-neutral", debitNeutralRoutes);
app.use("/api/auth", authRoutes);

// ─── Health ───────────────────────────────────────────────────────────────────
app.get("/status", (_req, res) =>
  res.json({
    status: "Online",
    strategy: "Debit Neutral",
    timestamp: new Date(),
  }),
);

// ─── Manual Upstox token refresh ─────────────────────────────────────────────
// POST /api/token  { "token": "<new_access_token>" }
app.post("/api/token", async (req, res) => {
  const { token } = req.body;
  if (!token) return res.status(400).json({ error: "token required" });
  await setUpstoxAccessToken(token);
  res.json({ success: true });
});

// ─── Global error handlers ────────────────────────────────────────────────────
process.on("uncaughtException", async (err) => {
  console.error("💥 Uncaught:", err.message);
  try {
    await sendDebitNeutralAlert(
      `💥 <b>Debit Neutral Server Crash</b>\n<code>${err.message}</code>`,
    );
  } catch (_) {}
  process.exit(1);
});

process.on("unhandledRejection", async (reason) => {
  const msg = reason instanceof Error ? reason.message : String(reason);
  console.error("💥 Unhandled:", msg);
  try {
    await sendDebitNeutralAlert(
      `⚠️ <b>Unhandled Rejection</b>\n<code>${msg}</code>`,
    );
  } catch (_) {}
});

// ─── Startup ──────────────────────────────────────────────────────────────────
const start = async () => {
  // 1. Connect to MongoDB first
  await connectDatabases();

  // 2. Load token from DB into process memory
  const currentToken = await loadTokenFromDB();

  const PORT = process.env.PORT || 3004;
  server.listen(PORT, async () => {
    console.log(`🚀 Debit Neutral Server · port ${PORT}`);
    await sendDebitNeutralAlert("🟢 <b>Debit Neutral Server Online ✅</b>");

    // 3. Initialize live data if token was successfully loaded
    if (currentToken || process.env.UPSTOX_ACCESS_TOKEN) {
      await initUpstoxLiveData();
      console.log("✅ Upstox WebSocket started");
    } else {
      console.warn(
        "⚠️ UPSTOX_ACCESS_TOKEN missing — live data disabled. Run: npm run login:upstox",
      );
    }

    // ── Main loop every 5 seconds ──────────────────────────────────────────
    setInterval(async () => {
      try {
        await debitNeutralScanAndSync();
        await debitNeutralAutoEnter();
      } catch (err) {
        console.error("❌ Main loop error:", err.message);
      }
    }, 5000);
  });
};

// ─── Cron jobs ────────────────────────────────────────────────────────────────
cron.schedule(
  "0 0 * * *",
  () => {
    resetDebitNeutralDay();
    console.log("🔄 Debit Neutral day reset (midnight)");
  },
  { timezone: "Asia/Kolkata" },
);

start();
