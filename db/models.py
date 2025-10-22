from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Optional

from sqlalchemy import (
    BigInteger,
    Boolean,
    Index,
    Integer,
    MetaData,
    Numeric,
    String,
    TIMESTAMP,
    text,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

_naming_convention = {
    "ix": "ix_%(table_name)s_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}


class Base(DeclarativeBase):
    metadata = MetaData(naming_convention=_naming_convention)


class Candle1m(Base):
    __tablename__ = "candles_1m"

    symbol: Mapped[str] = mapped_column(String(32), primary_key=True)
    bucket_start: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), primary_key=True)
    open: Mapped[Decimal] = mapped_column(Numeric(18, 8), nullable=False)
    high: Mapped[Decimal] = mapped_column(Numeric(18, 8), nullable=False)
    low: Mapped[Decimal] = mapped_column(Numeric(18, 8), nullable=False)
    close: Mapped[Decimal] = mapped_column(Numeric(18, 8), nullable=False)
    volume: Mapped[Decimal] = mapped_column(Numeric(24, 8), nullable=False)
    quote_volume: Mapped[Optional[Decimal]] = mapped_column(Numeric(24, 8), nullable=True)
    trade_count: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    taker_buy_volume: Mapped[Optional[Decimal]] = mapped_column(Numeric(24, 8), nullable=True)
    taker_buy_quote_volume: Mapped[Optional[Decimal]] = mapped_column(Numeric(24, 8), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=text("now()"), nullable=False
    )


class Trade(Base):
    __tablename__ = "trades"

    symbol: Mapped[str] = mapped_column(String(32), primary_key=True)
    trade_ts: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), primary_key=True)
    trade_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    price: Mapped[Decimal] = mapped_column(Numeric(18, 8), nullable=False)
    quantity: Mapped[Decimal] = mapped_column(Numeric(18, 8), nullable=False)
    is_buyer_maker: Mapped[bool] = mapped_column(Boolean, nullable=False)
    side: Mapped[Optional[str]] = mapped_column(String(4), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=text("now()"), nullable=False
    )


class OpenInterestSnapshot(Base):
    __tablename__ = "oi_snapshots"

    symbol: Mapped[str] = mapped_column(String(32), primary_key=True)
    snapshot_ts: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), primary_key=True)
    open_interest: Mapped[Decimal] = mapped_column(Numeric(28, 8), nullable=False)
    open_interest_usd: Mapped[Optional[Decimal]] = mapped_column(Numeric(28, 8), nullable=True)
    basis_points: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 6), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=text("now()"), nullable=False
    )


class FundingRate(Base):
    __tablename__ = "funding"

    symbol: Mapped[str] = mapped_column(String(32), primary_key=True)
    funding_ts: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), primary_key=True)
    funding_rate: Mapped[Decimal] = mapped_column(Numeric(12, 10), nullable=False)
    funding_rate_annualized: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 10), nullable=True)
    funding_payment: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 8), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=text("now()"), nullable=False
    )


Index("ix_candles_1m_bucket_start", Candle1m.bucket_start)
Index("ix_trades_trade_ts", Trade.trade_ts)
Index("ix_oi_snapshots_snapshot_ts", OpenInterestSnapshot.snapshot_ts)
Index("ix_funding_funding_ts", FundingRate.funding_ts)

__all__ = [
    "Base",
    "Candle1m",
    "FundingRate",
    "OpenInterestSnapshot",
    "Trade",
]
