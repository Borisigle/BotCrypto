from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Mapping, Sequence


@dataclass(frozen=True)
class Candle:
    """Normalized kline record for Binance Futures data."""

    symbol: str
    open_time: int
    close_time: int
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    volume: float
    quote_volume: float
    number_of_trades: int
    taker_buy_volume: float
    taker_buy_quote_volume: float

    @classmethod
    def from_rest(cls, symbol: str, payload: Sequence[object]) -> "Candle":
        if len(payload) < 11:
            raise ValueError("Incomplete kline payload received from Binance.")
        return cls(
            symbol=symbol.upper(),
            open_time=int(payload[0]),
            open_price=float(payload[1]),
            high_price=float(payload[2]),
            low_price=float(payload[3]),
            close_price=float(payload[4]),
            volume=float(payload[5]),
            close_time=int(payload[6]),
            quote_volume=float(payload[7]),
            number_of_trades=int(payload[8]),
            taker_buy_volume=float(payload[9]),
            taker_buy_quote_volume=float(payload[10]),
        )

    def as_dict(self) -> Dict[str, object]:
        return {
            "symbol": self.symbol,
            "open_time": self.open_time,
            "close_time": self.close_time,
            "open_price": self.open_price,
            "high_price": self.high_price,
            "low_price": self.low_price,
            "close_price": self.close_price,
            "volume": self.volume,
            "quote_volume": self.quote_volume,
            "number_of_trades": self.number_of_trades,
            "taker_buy_volume": self.taker_buy_volume,
            "taker_buy_quote_volume": self.taker_buy_quote_volume,
        }


@dataclass(frozen=True)
class AggTrade:
    """Aggregated trade item used for CVD calculations."""

    symbol: str
    agg_trade_id: int
    price: float
    quantity: float
    first_trade_id: int
    last_trade_id: int
    timestamp: int
    is_buyer_maker: bool

    @classmethod
    def from_rest(cls, symbol: str, payload: Mapping[str, object]) -> "AggTrade":
        return cls(
            symbol=symbol.upper(),
            agg_trade_id=int(payload["a"]),
            price=float(payload["p"]),
            quantity=float(payload["q"]),
            first_trade_id=int(payload["f"]),
            last_trade_id=int(payload["l"]),
            timestamp=int(payload["T"]),
            is_buyer_maker=bool(payload["m"]),
        )

    def as_dict(self) -> Dict[str, object]:
        return {
            "symbol": self.symbol,
            "agg_trade_id": self.agg_trade_id,
            "price": self.price,
            "quantity": self.quantity,
            "first_trade_id": self.first_trade_id,
            "last_trade_id": self.last_trade_id,
            "timestamp": self.timestamp,
            "is_buyer_maker": self.is_buyer_maker,
        }


@dataclass(frozen=True)
class OpenInterestStat:
    """Open interest statistics sample from Binance."""

    symbol: str
    timestamp: int
    sum_open_interest: float
    sum_open_interest_value: float

    @classmethod
    def from_rest(cls, symbol: str, payload: Mapping[str, object]) -> "OpenInterestStat":
        return cls(
            symbol=symbol.upper(),
            timestamp=int(payload["timestamp"]),
            sum_open_interest=float(payload["sumOpenInterest"]),
            sum_open_interest_value=float(payload["sumOpenInterestValue"]),
        )

    def as_dict(self) -> Dict[str, object]:
        return {
            "symbol": self.symbol,
            "timestamp": self.timestamp,
            "sum_open_interest": self.sum_open_interest,
            "sum_open_interest_value": self.sum_open_interest_value,
        }


@dataclass(frozen=True)
class FundingRate:
    """Funding rate observation from Binance Futures."""

    symbol: str
    funding_time: int
    funding_rate: float
    mark_price: float
    index_price: float

    @classmethod
    def from_rest(cls, symbol: str, payload: Mapping[str, object]) -> "FundingRate":
        return cls(
            symbol=symbol.upper(),
            funding_time=int(payload["fundingTime"]),
            funding_rate=float(payload["fundingRate"]),
            mark_price=float(payload["markPrice"]),
            index_price=float(payload["indexPrice"]),
        )

    def as_dict(self) -> Dict[str, object]:
        return {
            "symbol": self.symbol,
            "funding_time": self.funding_time,
            "funding_rate": self.funding_rate,
            "mark_price": self.mark_price,
            "index_price": self.index_price,
        }
