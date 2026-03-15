weekend-strangle/
├── server.js                          ← main entry point, port 3003
├── package.json
├── upstoxAutoLogin.js                 ← copied from existing project (identical)
├── Engines/
│   └── weekendStrangleEngine.js       ← all strategy logic
├── models/
│   ├── weekendStrangleModel.js        ← active trade schema
│   └── weekendStranglePerformanceModel.js
├── routes/
│   └── weekendStrangleRoutes.js       ← REST API
├── services/
│   ├── telegramService.js
│   └── upstoxLiveData.js              ← copied from existing project (identical)
└── config/
    ├── db.js                          ← copied (same MongoDB)
    ├── socket.js                      ← copied (same Socket.IO pattern)
    └── upstoxConfig.js                ← copied (same Upstox SDK)