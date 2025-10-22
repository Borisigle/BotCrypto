# Ingestion Monitoring Service

This repository provides a lightweight FastAPI service exposing monitoring metrics for ingestion pipelines,
strategy indicators, and signal health. It delivers a JSON API, a browser dashboard, Prometheus/Grafana-ready metrics,
and alerting hooks for stalled data feeds or cadence deviations.

## Features

- **Aggregated metrics API** covering ingestion latency, signal counts, and win/loss performance statistics.
- **Dashboard UI** with status panels summarising ingestion freshness, signal health, and performance KPIs.
- **Prometheus endpoint** (`/metrics/prometheus`) ready for Grafana dashboards.
- **Signal engine setups** classifying squeeze/reversal and absorption opportunities with confidence tagging and refined volume profiles.
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
app/data/sample_metrics.json  # Example snapshot powering the metrics
scripts/run_backtest.py       # Command-line helper for analysts
```

Further implementation and monitoring guidance is provided in [`docs/monitoring.md`](docs/monitoring.md).
Additional details on the backtesting subsystem can be found in [`docs/backtesting.md`](docs/backtesting.md).
