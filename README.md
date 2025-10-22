# Ingestion Monitoring Service

This repository provides a lightweight FastAPI service exposing monitoring metrics for ingestion pipelines,
strategy indicators, and signal health. It delivers a JSON API, a browser dashboard, Prometheus/Grafana-ready metrics,
and alerting hooks for stalled data feeds or cadence deviations.

## Features

- **Aggregated metrics API** covering ingestion latency, signal counts, and win/loss performance statistics.
- **Dashboard UI** with status panels summarising ingestion freshness, signal health, and performance KPIs.
- **Prometheus endpoint** (`/metrics/prometheus`) ready for Grafana dashboards.
- **Signal engine setups** classifying squeeze/reversal and absorption opportunities with confidence tagging and refined volume profiles.
- **Signal scoring worker** deriving 0–7 composite scores with trend, flow, and session weightings while persisting audit metadata.
- **Alerting harness** that can push webhooks when latency or cadence thresholds are violated.
- **Backtesting engine** producing hit-rate, expectancy, and drawdown metrics across 30–90 day windows with JSON/CSV exports.
- **File-based snapshot store** for rapid prototyping that can be swapped with a persistent datastore later.

## Quick start

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
uvicorn app.main:app --reload
```

Visit [http://localhost:8080/dashboard](http://localhost:8080/dashboard) to view the monitoring dashboard.

## Docker Compose environment

A multi-service Docker Compose stack is provided to run the API, worker, web frontend, TimescaleDB, and Redis locally.

### Prerequisites

- Docker Engine 24+
- Docker Compose v2 (available via the `docker compose` CLI)

### Starting the stack

```bash
./scripts/compose_up.sh
```

The helper script forwards any extra flags to `docker compose`. For example, append `--build` to force an image rebuild.

Once the stack is running:

- **API** (FastAPI) is available at [http://localhost:8080](http://localhost:8080) with its health check at `/api/v1/health` and the dashboard at `/dashboard`.
- **Web placeholder** (Node/Express) serves a hand-off page at [http://localhost:3000](http://localhost:3000).
- **TimescaleDB** listens on `postgresql://monitor:monitor@localhost:5432/monitoring` with the Timescale extension pre-enabled.
- **Redis** is exposed on [redis://localhost:6379](redis://localhost:6379).
- **Worker** logs can be tailed with `docker compose logs -f worker` and periodically process the bundled market snapshot.

Both the API and worker containers wait for the database to accept connections and run a placeholder Alembic migration hook before starting.

### Stopping the stack

```bash
./scripts/compose_down.sh
```

Pass `-v` to remove the TimescaleDB volume when you want a fresh database (`./scripts/compose_down.sh -v`).

## Project layout

```
app/
  ├── main.py             # FastAPI application bootstrap
  ├── config.py           # Runtime configuration and thresholds
  ├── backtest.py         # Backtesting engine and reporting helpers
  ├── data_source.py      # File-backed repository used by the metrics service
  ├── metrics_service.py  # Aggregation logic for latency, signal, and performance stats
  ├── alerting.py         # Alert evaluation and webhook dispatch
  ├── templates/          # Jinja templates for the dashboard view
  └── static/             # Static assets (CSS)
app/binance_backfill.py        # Helpers and client for Binance historical ingestion
app/data/sample_metrics.json   # Example snapshot powering the metrics
scripts/
  ├── binance_backfill.py      # CLI entry point for Binance historical ingestion
  └── run_backtest.py          # Command-line helper for analysts
```

Further implementation and monitoring guidance is provided in [`docs/monitoring.md`](docs/monitoring.md).
Additional details on the backtesting subsystem can be found in [`docs/backtesting.md`](docs/backtesting.md).

## Binance data backfill runbook

The repository ships with a dedicated utility for backfilling Binance USDT-margined futures data covering 1m candles, aggregated trades, open interest, and funding rates. This allows the monitoring service to ingest 30–90 day windows (or longer) while keeping the local dataset idempotent.

### Running the backfill

```bash
python scripts/binance_backfill.py --symbol BTCUSDT --window-days 60
```

Key flags:

- `--resume` (default) restarts from the most recent stored timestamp to support scheduled jobs. Use `--full-refresh` when you need to rebuild the entire window.
- `--skip-trades`, `--skip-open-interest`, `--skip-funding`, and `--skip-candles` let you tailor the workload when trades are not required or when running lightweight checks.
- `--interval` and `--oi-period` control candle granularity and open-interest aggregation.
- `--output` points the script to an alternate storage directory (defaults to `app/data/binance`).

The script logs progress for every batch (records fetched, inserted, and the next cursor) and emits a JSON summary with per-stage pacing metrics suitable for schedulers to parse.

### Scheduling & monitoring

For cron or Airflow usage, keep the default `--resume` flag so repeated runs only fetch new slices. The JSON summary written to stdout exposes start/end timestamps, totals per data type, and `records_per_second` throughput for quick health checks. The logs include ISO-8601 cursors that can be forwarded to Grafana/Loki for trend analysis.

### Data integrity checks

After a run, validate data completeness with the following steps:

1. Confirm record counts per dataset:
   ```bash
   wc -l app/data/binance/btcusdt_1m_candles.jsonl
   wc -l app/data/binance/btcusdt_open_interest_5m.jsonl
   ```
2. Inspect the emitted JSON summary to ensure the reported `earliest` and `latest` timestamps match your requested window.
3. Spot-check for gaps by parsing the JSONL files:
   ```python
   python - <<'PY'
   import json
   from pathlib import Path

   path = Path("app/data/binance/btcusdt_1m_candles.jsonl")
   rows = [json.loads(line) for line in path.open()]
   rows.sort(key=lambda r: r["open_time"])
   gaps = [
       (rows[i]["open_time"], rows[i + 1]["open_time"])
       for i in range(len(rows) - 1)
       if rows[i + 1]["open_time"] - rows[i]["open_time"] > 60_000
   ]
   print(f"Detected {len(gaps)} candle gaps")
   PY
   ```

Document any gaps and rerun the backfill with `--full-refresh` if needed. The idempotent writer rewrites the JSONL snapshots atomically, preserving data integrity for downstream ingestion.
