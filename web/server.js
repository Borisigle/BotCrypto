const express = require("express");

const app = express();
const PORT = process.env.PORT || 3000;
const API_BASE_URL = process.env.API_BASE_URL || "http://localhost:8080";

app.get("/healthz", (_req, res) => {
  res.json({ status: "ok" });
});

app.get("/", (_req, res) => {
  res.send(`<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <title>Ingestion Monitor Dashboard</title>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
      body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; margin: 3rem; background: #0f172a; color: #f8fafc; }
      h1 { font-size: 2.5rem; margin-bottom: 1rem; }
      a { color: #38bdf8; }
      .card { background: rgba(15, 23, 42, 0.85); border: 1px solid rgba(148, 163, 184, 0.2); border-radius: 0.75rem; padding: 2rem; max-width: 640px; box-shadow: 0 20px 45px rgba(15, 23, 42, 0.45); }
      code { background: rgba(148, 163, 184, 0.2); padding: 0.25rem 0.5rem; border-radius: 0.35rem; }
      ul { line-height: 1.75; }
    </style>
  </head>
  <body>
    <div class="card">
      <h1>Ingestion Monitor Web</h1>
      <p>The API service is available at <code>${API_BASE_URL}</code>.</p>
      <ul>
        <li><a href="${API_BASE_URL}/api/v1/health" target="_blank" rel="noreferrer">API health endpoint</a></li>
        <li><a href="${API_BASE_URL}/dashboard" target="_blank" rel="noreferrer">Monitoring dashboard</a></li>
      </ul>
      <p>Use <code>./scripts/compose_up.sh</code> to start the Docker Compose stack.</p>
    </div>
  </body>
</html>`);
});

app.listen(PORT, () => {
  console.log(`Web placeholder listening on port ${PORT}`);
  console.log(`Proxying API calls to ${API_BASE_URL}`);
});
