# Database schema and TimescaleDB migrations

This project now ships with a PostgreSQL/TimescaleDB schema that captures real-time
market data and makes it available to both the API and the background workers. The
schema lives in the `db` package alongside SQLAlchemy models, Alembic migrations, and
thin repository helpers to encourage reuse across services.

## Getting started

1. Ensure a PostgreSQL instance with the TimescaleDB extension is available. The
   default development connection string points to
   `postgresql+psycopg://postgres:postgres@localhost:5432/ingestion` and can be
   overridden with the `DATABASE_URL` environment variable.
2. Install the new dependencies and editable package:

   ```bash
   python -m venv .venv
   source .venv/bin/activate
   pip install -e .
   ```

3. Run the initial migration to create the hypertables and retention policies:

   ```bash
   alembic -c db/alembic.ini upgrade head
   ```

   The Alembic environment automatically enables the TimescaleDB extension on the
   target database before applying migrations. You can supply a custom URL at runtime
   with `alembic -x db_url=postgresql+psycopg://user:pass@host:5432/db upgrade head`.

4. Consume the ORM models or repository helpers from `db.repository` in application
   code:

   ```python
   from datetime import datetime, timedelta
   from decimal import Decimal

   from db import session_scope
   from db.repository import CandlePayload, fetch_candles, upsert_candles

   horizon = datetime.utcnow()

   with session_scope() as session:
       upsert_candles(
           session,
           [
               CandlePayload(
                   symbol="BTCUSDT",
                   bucket_start=horizon.replace(second=0, microsecond=0),
                   open=Decimal("64000.12"),
                   high=Decimal("64120.55"),
                   low=Decimal("63950.02"),
                   close=Decimal("64080.10"),
                   volume=Decimal("152.4"),
               )
           ],
       )
       latest = fetch_candles(
           session,
           symbol="BTCUSDT",
           start=horizon - timedelta(hours=1),
           end=horizon,
       )
   ```

## Table reference

All tables share a composite primary key of `(symbol, timestamp)` and are promoted to
TimescaleDB hypertables. The Alembic migration explicitly configures chunk intervals,
partitioning, and retention for each dataset.

| Table | Purpose | Key columns | Additional columns | Chunk interval | Retention |
|-------|---------|-------------|--------------------|----------------|-----------|
| `candles_1m` | 1-minute OHLCV candles | `symbol`, `bucket_start` | `open`, `high`, `low`, `close`, `volume`, optional quote/taker stats | 7 days | 180 days |
| `trades` | Individual aggregated trades | `symbol`, `trade_ts`, `trade_id` | `price`, `quantity`, `is_buyer_maker`, optional `side` | 3 days | 30 days |
| `oi_snapshots` | Futures open interest snapshots | `symbol`, `snapshot_ts` | `open_interest`, optional USD/basis points | 7 days | 120 days |
| `funding` | Perpetual swap funding history | `symbol`, `funding_ts` | `funding_rate`, optional annualised rate and payment | 30 days | 365 days |

Additional B-tree indexes exist on the raw timestamp columns to support cross-symbol
queries (e.g. retrieving all funding rates across instruments during a time window).

The `created_at` timestamp on each table is populated by PostgreSQL and records when
rows were ingested.

## Retention and partition strategy

The retention policies have been selected to balance storage requirements with common
analysis windows:

- **Candles (180 days):** Supports six months of 1-minute bars for medium-term
  backtesting while keeping hypertable chunks around one week in size.
- **Trades (30 days):** Large trade volumes benefit from aggressive roll-off. A
  three-day chunk interval keeps insert latency low while still allowing a month of
  lookback for reconciliation logic.
- **Open interest (120 days):** Four months of data is sufficient for most trend
  analyses, and the weekly chunks align with the candle hypertable.
- **Funding (365 days):** Funding seasonality analyses often require a one-year view;
  monthly chunks minimise background job overhead while retaining fidelity.

These policies can be tuned via `ALTER POLICY` or by editing the migration constants if
requirements change. Dropping a policy can be achieved with
`SELECT remove_retention_policy('<table>');`.

## Using the repository layer

The `db.repository` module provides idempotent upsert helpers using PostgreSQL's
`ON CONFLICT` support:

- `upsert_candles`, `upsert_trades`, `upsert_open_interest`, `upsert_funding` accept
  sequences of payload dataclasses for batch ingestion.
- `fetch_candles`, `recent_trades`, `latest_open_interest`, and `latest_funding` wrap
  common read patterns for the API layer.

All helpers operate on a plain SQLAlchemy `Session` instance; they do not commit on
behalf of the caller so they can participate in broader transactional units.

## Running further migrations

To create new migrations, point Alembic at the project configuration and use the
standard revision workflow:

```bash
alembic -c db/alembic.ini revision -m "add liquidations table"
alembic -c db/alembic.ini upgrade head
```

The environment automatically loads `db.models.Base.metadata`, so model changes in
`db/models.py` will be reflected during autogeneration. Remember to design new tables
with composite keys that align to TimescaleDB hypertables and to document any new
retention policies in this file.
