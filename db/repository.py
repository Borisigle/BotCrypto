from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from decimal import Decimal
from typing import Optional, Sequence

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from .models import Candle1m, FundingRate, OpenInterestSnapshot, Trade


@dataclass(frozen=True)
class CandlePayload:
    symbol: str
    bucket_start: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    quote_volume: Optional[Decimal] = None
    trade_count: Optional[int] = None
    taker_buy_volume: Optional[Decimal] = None
    taker_buy_quote_volume: Optional[Decimal] = None


@dataclass(frozen=True)
class TradePayload:
    symbol: str
    trade_ts: datetime
    trade_id: int
    price: Decimal
    quantity: Decimal
    is_buyer_maker: bool
    side: Optional[str] = None


@dataclass(frozen=True)
class OpenInterestPayload:
    symbol: str
    snapshot_ts: datetime
    open_interest: Decimal
    open_interest_usd: Optional[Decimal] = None
    basis_points: Optional[Decimal] = None


@dataclass(frozen=True)
class FundingPayload:
    symbol: str
    funding_ts: datetime
    funding_rate: Decimal
    funding_rate_annualized: Optional[Decimal] = None
    funding_payment: Optional[Decimal] = None


def upsert_candles(session: Session, rows: Sequence[CandlePayload]) -> None:
    if not rows:
        return

    stmt = insert(Candle1m).values([asdict(row) for row in rows])
    update_columns = {
        "open": stmt.excluded.open,
        "high": stmt.excluded.high,
        "low": stmt.excluded.low,
        "close": stmt.excluded.close,
        "volume": stmt.excluded.volume,
        "quote_volume": stmt.excluded.quote_volume,
        "trade_count": stmt.excluded.trade_count,
        "taker_buy_volume": stmt.excluded.taker_buy_volume,
        "taker_buy_quote_volume": stmt.excluded.taker_buy_quote_volume,
    }
    session.execute(
        stmt.on_conflict_do_update(
            index_elements=[Candle1m.symbol, Candle1m.bucket_start],
            set_=update_columns,
        )
    )


def fetch_candles(
    session: Session,
    symbol: str,
    start: datetime,
    end: datetime,
) -> list[Candle1m]:
    query = (
        select(Candle1m)
        .where(
            Candle1m.symbol == symbol,
            Candle1m.bucket_start >= start,
            Candle1m.bucket_start < end,
        )
        .order_by(Candle1m.bucket_start)
    )
    return session.scalars(query).all()


def upsert_trades(session: Session, rows: Sequence[TradePayload]) -> None:
    if not rows:
        return

    stmt = insert(Trade).values([asdict(row) for row in rows])
    update_columns = {
        "price": stmt.excluded.price,
        "quantity": stmt.excluded.quantity,
        "is_buyer_maker": stmt.excluded.is_buyer_maker,
        "side": stmt.excluded.side,
    }
    session.execute(
        stmt.on_conflict_do_update(
            index_elements=[Trade.symbol, Trade.trade_ts, Trade.trade_id],
            set_=update_columns,
        )
    )


def recent_trades(session: Session, symbol: str, limit: int = 200) -> list[Trade]:
    query = (
        select(Trade)
        .where(Trade.symbol == symbol)
        .order_by(Trade.trade_ts.desc())
        .limit(limit)
    )
    result = session.scalars(query).all()
    result.reverse()
    return result


def upsert_open_interest(session: Session, rows: Sequence[OpenInterestPayload]) -> None:
    if not rows:
        return

    stmt = insert(OpenInterestSnapshot).values([asdict(row) for row in rows])
    update_columns = {
        "open_interest": stmt.excluded.open_interest,
        "open_interest_usd": stmt.excluded.open_interest_usd,
        "basis_points": stmt.excluded.basis_points,
    }
    session.execute(
        stmt.on_conflict_do_update(
            index_elements=[OpenInterestSnapshot.symbol, OpenInterestSnapshot.snapshot_ts],
            set_=update_columns,
        )
    )


def latest_open_interest(session: Session, symbol: str) -> Optional[OpenInterestSnapshot]:
    query: Select[OpenInterestSnapshot] = (
        select(OpenInterestSnapshot)
        .where(OpenInterestSnapshot.symbol == symbol)
        .order_by(OpenInterestSnapshot.snapshot_ts.desc())
        .limit(1)
    )
    return session.scalars(query).first()


def upsert_funding(session: Session, rows: Sequence[FundingPayload]) -> None:
    if not rows:
        return

    stmt = insert(FundingRate).values([asdict(row) for row in rows])
    update_columns = {
        "funding_rate": stmt.excluded.funding_rate,
        "funding_rate_annualized": stmt.excluded.funding_rate_annualized,
        "funding_payment": stmt.excluded.funding_payment,
    }
    session.execute(
        stmt.on_conflict_do_update(
            index_elements=[FundingRate.symbol, FundingRate.funding_ts],
            set_=update_columns,
        )
    )


def latest_funding(session: Session, symbol: str) -> Optional[FundingRate]:
    query: Select[FundingRate] = (
        select(FundingRate)
        .where(FundingRate.symbol == symbol)
        .order_by(FundingRate.funding_ts.desc())
        .limit(1)
    )
    return session.scalars(query).first()


__all__ = [
    "CandlePayload",
    "TradePayload",
    "OpenInterestPayload",
    "FundingPayload",
    "fetch_candles",
    "latest_funding",
    "latest_open_interest",
    "recent_trades",
    "upsert_candles",
    "upsert_funding",
    "upsert_open_interest",
    "upsert_trades",
]
