import { Router } from "express";

import { login, callback } from "../controllers/authControllers.js";

const router = Router();

router.get("/upstox/login", login);
router.get("/upstox/callback", callback);

export default router;
