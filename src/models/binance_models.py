from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import re
from typing import Any, Dict, List, Sequence

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


# --- Historical collector model (Option B: store timestamps as unix ms ints) ---

SUPPORTED_INTERVALS = {"5m", "15m"}
SYMBOL_PATTERN = re.compile(r"^[A-Z0-9]{5,30}$")


def to_decimal(value: Any) -> Decimal:
    """Convert Binance numeric strings into Decimal to avoid precision loss."""
    try:
        return value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as e:
        raise ValueError(f"Invalid decimal: {value!r}") from e


class HistoricalKline(BaseModel):
    """Validated Binance spot kline, ready for storage and files.

    Binance /api/v3/klines item order:
    [
      0 open_time_ms,
      1 open,
      2 high,
      3 low,
      4 close,
      5 volume,
      6 close_time_ms,
      7 quote_volume,
      8 trade_count,
      9 taker_buy_base_volume,
      10 taker_buy_quote_volume,
      11 ignore
    ]
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    symbol: str
    interval: str

    open_time_ms: int
    close_time_ms: int

    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal

    volume: Decimal
    quote_volume: Decimal
    trade_count: int

    taker_buy_base_volume: Decimal
    taker_buy_quote_volume: Decimal

    @field_validator("symbol", mode="before")
    @classmethod
    def normalize_symbol(cls, v: Any) -> str:
        s = str(v).strip().upper()
        if not SYMBOL_PATTERN.match(s):
            raise ValueError(f"Invalid symbol: {s!r}")
        return s

    @field_validator("interval")
    @classmethod
    def validate_interval(cls, v: str) -> str:
        if v not in SUPPORTED_INTERVALS:
            raise ValueError(f"Unsupported interval: {v!r}. Allowed: {sorted(SUPPORTED_INTERVALS)}")
        return v

    @field_validator(
        "open",
        "high",
        "low",
        "close",
        "volume",
        "quote_volume",
        "taker_buy_base_volume",
        "taker_buy_quote_volume",
        mode="before",
    )
    @classmethod
    def parse_decimal(cls, v: Any) -> Decimal:
        return to_decimal(v)

    @field_validator("trade_count")
    @classmethod
    def validate_trade_count(cls, v: int) -> int:
        if v < 0:
            raise ValueError("trade_count must be non-negative")
        return v

    @model_validator(mode="after")
    def validate_values(self) -> "HistoricalKline":
        # timestamps
        if self.open_time_ms <= 0 or self.close_time_ms <= 0:
            raise ValueError("timestamps must be positive")
        if self.open_time_ms >= self.close_time_ms:
            raise ValueError("open_time_ms must be < close_time_ms")

        # OHLC sanity
        if self.high < self.low:
            raise ValueError("high must be >= low")
        if not (self.low <= self.open <= self.high):
            raise ValueError("open must be within [low, high]")
        if not (self.low <= self.close <= self.high):
            raise ValueError("close must be within [low, high]")

        # volumes
        if self.volume < 0 or self.quote_volume < 0:
            raise ValueError("volume fields must be non-negative")
        if self.taker_buy_base_volume < 0 or self.taker_buy_quote_volume < 0:
            raise ValueError("taker buy volume fields must be non-negative")

        return self

    @classmethod
    def from_binance(cls, *, symbol: str, interval: str, raw: Sequence[Any]) -> "HistoricalKline":
        """Convert one raw Binance kline array into a validated model."""
        if len(raw) < 11:
            raise ValueError(f"Expected kline array length >= 11, got {len(raw)}")

        return cls(
            symbol=symbol,
            interval=interval,
            open_time_ms=int(raw[0]),
            open=raw[1],
            high=raw[2],
            low=raw[3],
            close=raw[4],
            volume=raw[5],
            close_time_ms=int(raw[6]),
            quote_volume=raw[7],
            trade_count=int(raw[8]),
            taker_buy_base_volume=raw[9],
            taker_buy_quote_volume=raw[10],
        )

    def key(self) -> Dict[str, Any]:
        """Mongo idempotency key."""
        return {"symbol": self.symbol, "interval": self.interval, "open_time_ms": self.open_time_ms}

    def to_mongo_doc(self) -> Dict[str, Any]:
        """Mongo-friendly document.

        We use Decimal128 if bson is present (it is with pymongo/motor).
        """
        try:
            from bson.decimal128 import Decimal128  # type: ignore

            def dec(v: Decimal) -> Any:
                return Decimal128(v)

        except Exception:
            # Fallback: store decimals as strings to preserve precision.
            def dec(v: Decimal) -> Any:
                return str(v)

        return {
            "symbol": self.symbol,
            "interval": self.interval,
            "open_time_ms": self.open_time_ms,
            "close_time_ms": self.close_time_ms,
            "open": dec(self.open),
            "high": dec(self.high),
            "low": dec(self.low),
            "close": dec(self.close),
            "volume": dec(self.volume),
            "quote_volume": dec(self.quote_volume),
            "trade_count": self.trade_count,
            "taker_buy_base_volume": dec(self.taker_buy_base_volume),
            "taker_buy_quote_volume": dec(self.taker_buy_quote_volume),
        }

    def to_processed_row(self) -> Dict[str, Any]:
        """File-friendly row (Decimals as strings).

        This fixes your error:
          AttributeError: 'HistoricalKline' object has no attribute 'to_processed_row'
        """
        return {
            "symbol": self.symbol,
            "interval": self.interval,
            "open_time_ms": self.open_time_ms,
            "close_time_ms": self.close_time_ms,
            "open": str(self.open),
            "high": str(self.high),
            "low": str(self.low),
            "close": str(self.close),
            "volume": str(self.volume),
            "quote_volume": str(self.quote_volume),
            "trade_count": self.trade_count,
            "taker_buy_base_volume": str(self.taker_buy_base_volume),
            "taker_buy_quote_volume": str(self.taker_buy_quote_volume),
        }


# --- Legacy models (kept for streaming compatibility) ---
class BinanceKline(BaseModel):
    """Pydantic model for raw Binance kline/candlestick data (legacy)."""

    open_time: int = Field(..., description="Kline open time in milliseconds")
    open: float = Field(..., description="Open price")
    high: float = Field(..., description="High price")
    low: float = Field(..., description="Low price")
    close: float = Field(..., description="Close price")
    volume: float = Field(..., description="Base asset volume")
    close_time: int = Field(..., description="Kline close time in milliseconds")
    quote_volume: float = Field(..., description="Quote asset volume")
    trade_count: int = Field(..., description="Number of trades")
    taker_buy_base_volume: float = Field(..., description="Taker buy base asset volume")
    taker_buy_quote_volume: float = Field(..., description="Taker buy quote asset volume")

    @field_validator(
        "open",
        "high",
        "low",
        "close",
        "volume",
        "quote_volume",
        "taker_buy_base_volume",
        "taker_buy_quote_volume",
        mode="before",
    )
    @classmethod
    def convert_to_float(cls, v: Any) -> float:
        return float(v)

    @classmethod
    def from_binance_array(cls, data: List[Any]) -> "BinanceKline":
        return cls(
            open_time=int(data[0]),
            open=data[1],
            high=data[2],
            low=data[3],
            close=data[4],
            volume=data[5],
            close_time=int(data[6]),
            quote_volume=data[7],
            trade_count=int(data[8]),
            taker_buy_base_volume=data[9],
            taker_buy_quote_volume=data[10],
        )

    def to_timestamp(self) -> datetime:
        return datetime.fromtimestamp(self.open_time / 1000, tz=timezone.utc)


class KlineData(BaseModel):
    """Enriched kline data with symbol and interval (legacy)."""

    symbol: str
    interval: str
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    quote_volume: float
    trade_count: int

    @classmethod
    def from_kline(cls, kline: BinanceKline, symbol: str, interval: str) -> "KlineData":
        return cls(
            symbol=symbol,
            interval=interval,
            timestamp=kline.to_timestamp(),
            open=kline.open,
            high=kline.high,
            low=kline.low,
            close=kline.close,
            volume=kline.volume,
            quote_volume=kline.quote_volume,
            trade_count=kline.trade_count,
        )


class KlineMessage(BaseModel):
    """Kafka message value model for the Binance WebSocket stream (legacy)."""

    model_config = ConfigDict(str_strip_whitespace=True)

    event_time: int = Field(..., description="Event time in milliseconds - unique per message")
    symbol: str = Field(..., description="Trading pair symbol (e.g., BTCUSDT)")
    interval: str = Field(..., description="Kline interval (e.g., 1m, 5m, 1h, 1d)")
    timestamp: str = Field(..., description="Kline timestamp in ISO format")
    open: float = Field(..., description="Open price")
    high: float = Field(..., description="High price")
    low: float = Field(..., description="Low price")
    close: float = Field(..., description="Close price")
    volume: float = Field(..., description="Base asset volume")
    quote_volume: float = Field(..., description="Quote asset volume")
    trade_count: int = Field(..., description="Number of trades")
    is_closed: bool = Field(..., description="Whether the kline/candle is closed")

    def to_timestamp(self) -> datetime:
        return datetime.fromisoformat(self.timestamp)

    def to_event_timestamp(self) -> datetime:
        return datetime.fromtimestamp(self.event_time / 1000, tz=timezone.utc)

    def to_mongo_doc(self) -> dict:
        return {
            "event_time": self.event_time,
            "event_timestamp": self.to_event_timestamp(),
            "symbol": self.symbol,
            "interval": self.interval,
            "timestamp": self.to_timestamp(),
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
            "quote_volume": self.quote_volume,
            "trade_count": self.trade_count,
            "is_closed": self.is_closed,
        }
