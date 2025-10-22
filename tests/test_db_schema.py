from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import MagicMock

from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import Session

from db.models import Candle1m, FundingRate, OpenInterestSnapshot, Trade
from db.repository import CandlePayload, recent_trades, upsert_candles


def _pk_columns(model) -> tuple[str, ...]:
    return tuple(model.__table__.primary_key.columns.keys())


def test_timescale_models_primary_keys() -> None:
    assert _pk_columns(Candle1m) == ("symbol", "bucket_start")
    assert _pk_columns(Trade) == ("symbol", "trade_ts", "trade_id")
    assert _pk_columns(OpenInterestSnapshot) == ("symbol", "snapshot_ts")
    assert _pk_columns(FundingRate) == ("symbol", "funding_ts")


def test_timescale_models_indexes() -> None:
    candle_indexes = {index.name for index in Candle1m.__table__.indexes}
    trade_indexes = {index.name for index in Trade.__table__.indexes}
    oi_indexes = {index.name for index in OpenInterestSnapshot.__table__.indexes}
    funding_indexes = {index.name for index in FundingRate.__table__.indexes}

    assert candle_indexes == {"ix_candles_1m_bucket_start"}
    assert trade_indexes == {"ix_trades_trade_ts"}
    assert oi_indexes == {"ix_oi_snapshots_snapshot_ts"}
    assert funding_indexes == {"ix_funding_funding_ts"}


def test_upsert_candles_uses_on_conflict() -> None:
    session = MagicMock(spec=Session)
    payload = CandlePayload(
        symbol="BTCUSDT",
        bucket_start=datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc),
        open=Decimal("64000.12"),
        high=Decimal("64050.34"),
        low=Decimal("63990.11"),
        close=Decimal("64010.45"),
        volume=Decimal("125.6"),
    )

    upsert_candles(session, [payload])

    session.execute.assert_called_once()
    statement = session.execute.call_args[0][0]
    compiled = statement.compile(dialect=postgresql.dialect())
    sql = str(compiled)

    assert "ON CONFLICT" in sql
    assert "candles_1m" in sql
    for column in ("open", "high", "low", "close", "volume"):
        assert f"excluded.{column}" in sql


def test_recent_trades_returns_chronological_order() -> None:
    session = MagicMock(spec=Session)
    session.scalars.return_value.all.return_value = ["t3", "t2", "t1"]

    result = recent_trades(session, "BTCUSDT", limit=3)

    assert result == ["t1", "t2", "t3"]
    session.scalars.assert_called()
