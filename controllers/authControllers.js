import { defaultClient } from "../config/upstoxConfig.js";
import pkg from "upstox-js-sdk";
const { UserApi, MarketQuoteApi } = pkg;
import axios from "axios";
import { Token } from "../models/tokenModel.js"; 

export const login = (req, res) => {
  try {
    const loginUrl =
      `https://api.upstox.com/v2/login/authorization/dialog` +
      `?client_id=${process.env.UPSTOX_API_KEY}` +
      `&redirect_uri=${encodeURIComponent(process.env.UPSTOX_REDIRECT_URI)}` +
      `&response_type=code` +
      `&scope=orders`;

    console.log("🔗 Redirecting to Upstox login...");
    res.redirect(loginUrl);
  } catch (error) {
    console.error("❌ Upstox Login URL error:", error.message);
    res.status(500).json({ error: "Could not generate Upstox login URL" });
  }
};

export const callback = async (req, res) => {
  const authCode = req.query.code;

  if (!authCode) {
    return res.status(400).json({ error: "No auth code received from Upstox" });
  }

  try {
    const tokenRes = await axios.post(
      "https://api.upstox.com/v2/login/authorization/token",
      new URLSearchParams({
        code:          authCode,
        client_id:     process.env.UPSTOX_API_KEY,
        client_secret: process.env.UPSTOX_API_SECRET,
        redirect_uri:  process.env.UPSTOX_REDIRECT_URI,
        grant_type:    "authorization_code",
      }),
      { headers: { "Content-Type": "application/x-www-form-urlencoded" } }
    );

    if (!tokenRes.data?.access_token) {
      console.error("❌ Upstox token exchange failed:", tokenRes.data);
      return res.status(400).send("Upstox token generation failed");
    }

    const accessToken = tokenRes.data.access_token;

    // 1. Save or update token in MongoDB
    let tokenDoc = await Token.findOne();
    if (tokenDoc) {
      tokenDoc.accessToken = accessToken;
      await tokenDoc.save();
    } else {
      await Token.create({ accessToken: accessToken });
    }

    // 2. Set token in SDK for immediate use
    const oauth2 = defaultClient.authentications['OAUTH2'];
    oauth2.accessToken = accessToken;

    // 3. Set in process memory for other services
    process.env.UPSTOX_ACCESS_TOKEN = accessToken;

    console.log("✅ Upstox session created & token saved to MongoDB.");
    res.status(200).send(
      "<h1>✅ Upstox Connected!</h1><p>Access token saved. You can close this tab.</p>"
    );
  } catch (error) {
    console.error("❌ Upstox Auth Error:", error.message);
    res.status(500).json({ error: "Upstox authentication failed", details: error.message });
  }
};