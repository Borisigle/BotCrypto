# Monitoring guide

This document describes how to operate and extend the ingestion monitoring service.

## Architecture overview

The service exposes a FastAPI application with three integration surfaces:

1. **JSON API** providing aggregated metrics at `GET /api/v1/metrics`.
2. **Browser dashboard** served at `GET /dashboard` for human-friendly status checks.
3. **Prometheus-compatible feed** at `GET /metrics/prometheus` that can be scraped by Grafana/Prometheus.

Source data is currently backed by a JSON snapshot on disk (`app/data/sample_metrics.json`). The repository abstraction in
`app/data_source.py` can be replaced with a database- or message-queue-backed implementation without changing the outer
API.

## Metrics exposed

### Ingestion

- `ingestion.latest_source` – most recent provider delivering data.
- `ingestion.current_latency_seconds` – latency of the latest event.
- `ingestion.average_latency_seconds` / `max_latency_seconds` – rolling statistics over the snapshot window.
- `ingestion.time_since_last_event_seconds` – freshness indicator used for alerting.
- `ingestion.sources` – per-exchange/latest arrival information.

### Signals

- Total tracked signals and their status breakdown.
- Counts of signals generated within the last hour and day.
- Derived cadence (average seconds between signals).
- Setup categories, average setup score, and confidence breakdown derived from the signal engine.

### Performance

- Win / loss counts from recent executed trades.
- Win rate and average realised return (fractional, e.g. `0.012 == 1.2%`).

## Indicator overlays API

The UI overlays and the signal engine now consume indicator slices through dedicated endpoints powered by
Timescale snapshots. Each endpoint accepts the `symbol`, `timeframe`, and optional `session` query parameters
so consumers can request the exact slice they need.

- `GET /api/v1/indicators/cvd` returns cumulative volume delta curves.
- `GET /api/v1/indicators/delta-oi` returns delta open interest percentage traces.
- `GET /api/v1/indicators/volume-profile` returns value area, VWAP, and histogram statistics.

Example request:

```bash
curl "http://localhost:8080/api/v1/indicators/volume-profile?symbol=BTCUSDT&timeframe=5m&session=new_york"
```

### Caching and configuration

Indicator responses are cached via Redis when `REDIS_URL` is configured. If Redis is unavailable the service
falls back to an in-memory TTL cache. The following knobs control the integration:

| Variable | Default | Description |
| --- | --- | --- |
| `INDICATOR_SNAPSHOT_PATH` | `app/data/sample_indicator_data.json` | Path to the seeded Timescale export used for local development. |
| `REDIS_URL` | _unset_ | Connection string for the Redis indicator cache. When omitted the in-memory cache is used. |
| `INDICATOR_CACHE_TTL_SECONDS` | `30` | Cache duration applied to indicator query results. |

## Alerting hooks

`POST /api/v1/alerts/evaluate` evaluates the latest metrics and optionally pushes a webhook when:

- The most recent ingestion latency or gap exceeds configured thresholds.
- No signals have been generated in the last hour or the cadence slips below expectations.
- Win rate falls below the configured `MIN_WIN_RATE` once a meaningful sample is available.

Set `ALERT_WEBHOOK_URL` to enable outbound webhooks. Failed deliveries are logged and surfaced in the JSON response.

### Threshold configuration

The following environment variables adjust alert sensitivity:

| Variable | Default | Description |
| --- | --- | --- |
| `MAX_INGESTION_LATENCY_SECONDS` | `120` | Warning threshold for single-event latency. |
| `MAX_INGESTION_GAP_SECONDS` | `600` | Warning threshold for time since last ingestion. |
| `MIN_SIGNALS_PER_HOUR` | `6` | Expected minimum signals in the last hour. |
| `MIN_WIN_RATE` | `0.35` | Acceptable lower bound on win rate when enough data is present. |

## Grafana/Prometheus integration

The `/metrics/prometheus` endpoint emits gauges compatible with Prometheus scraping. Example scrape configuration:

```yaml
scrape_configs:
  - job_name: ingestion-monitor
    static_configs:
      - targets: ['localhost:8080']
```

The following metrics are exported:

- `ingestion_latency_seconds`
- `ingestion_gap_seconds`
- `ingestion_latency_seconds_max`
- `ingestion_source_latency_seconds{source="..."}`
- `signals_generated_total`
- `signals_last_hour`
- `signals_by_status_total{status="..."}`
- `strategy_win_rate`
- `strategy_average_return`

Grafana panels can use these gauges directly for alert thresholds or visual charts.

## Replacing the snapshot repository

To connect the service to a live datastore:

1. Implement a repository exposing a `.fetch_snapshot() -> MetricsSnapshot` method.
2. Wire it into `app/main.py` (replace `FileMetricsRepository` in the FastAPI dependency graph).
3. Ensure datetime fields are timezone-aware (`UTC`).

The rest of the service remains untouched thanks to the `MetricsSnapshot` schema defined in `app/models.py`.

## Local development tips

- Use `uvicorn app.main:app --reload` for local iteration.
- Update `app/data/sample_metrics.json` with recent snapshots to test edge cases (stalled feeds, losses, etc.).
- Trigger the alert path with `curl -X POST http://localhost:8080/api/v1/alerts/evaluate`.
- Scrape metrics via `curl http://localhost:8080/metrics/prometheus` to validate Prometheus output.
- Review [`docs/backtesting.md`](backtesting.md) for guidance on running the signal performance backtests and exporting JSON/CSV summaries.
