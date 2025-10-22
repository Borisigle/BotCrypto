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

### Performance

- Win / loss counts from recent executed trades.
- Win rate and average realised return (fractional, e.g. `0.012 == 1.2%`).

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
