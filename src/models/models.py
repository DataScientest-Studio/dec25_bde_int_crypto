from datetime import datetime, timezone
from typing import List
from pydantic import BaseModel, Field, field_validator, ConfigDict


class BinanceKline(BaseModel):
    """
    Pydantic model for Binance kline/candlestick data.

    Binance kline array format:
    [
      1499040000000,      // 0: Kline open time
      "0.01634000",       // 1: Open price
      "0.80000000",       // 2: High price
      "0.01575800",       // 3: Low price
      "0.01577100",       // 4: Close price
      "148976.11427815",  // 5: Volume
      1499644799999,      // 6: Kline close time
      "2434.19055334",    // 7: Quote asset volume
      308,                // 8: Number of trades
      "1756.87402397",    // 9: Taker buy base asset volume
      "28.46694368",      // 10: Taker buy quote asset volume
      "0"                 // 11: Unused field, ignore
    ]
    """
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

    @field_validator('open', 'high', 'low', 'close', 'volume', 'quote_volume', 'taker_buy_base_volume', 'taker_buy_quote_volume', mode='before')
    @classmethod
    def convert_to_float(cls, v):
        """Convert string numbers to float"""
        return float(v)

    @classmethod
    def from_binance_array(cls, data: List) -> 'BinanceKline':
        """Parse Binance kline array into BinanceKline model"""
        return cls(
            open_time=data[0],
            open=data[1],
            high=data[2],
            low=data[3],
            close=data[4],
            volume=data[5],
            close_time=data[6],
            quote_volume=data[7],
            trade_count=data[8],
            taker_buy_base_volume=data[9],
            taker_buy_quote_volume=data[10]
        )

    def to_timestamp(self) -> datetime:
        """Convert open_time to datetime"""
        return datetime.fromtimestamp(self.open_time / 1000, tz=timezone.utc)


class KlineData(BaseModel):
    """Enriched kline data with symbol and interval"""
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
    def from_kline(cls, kline: BinanceKline, symbol: str, interval: str) -> 'KlineData':
        """Create KlineData from BinanceKline"""
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
            trade_count=kline.trade_count
        )


class KlineMessage(BaseModel):
    """
    Pydantic model for Kafka message value from Binance WebSocket stream.

    This model represents the kline data structure as published to Redpanda/Kafka
    by the WebSocket producer (stream_producer.py lines 242-255).
    """
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
        """Convert ISO format timestamp string to datetime object."""
        return datetime.fromisoformat(self.timestamp)

    def to_event_timestamp(self) -> datetime:
        """Convert event_time milliseconds to datetime object."""
        return datetime.fromtimestamp(self.event_time / 1000, tz=timezone.utc)

    def to_mongo_doc(self) -> dict:
        """
        Convert to MongoDB document format.

        Returns a dict suitable for insertion into MongoDB with datetime objects
        instead of strings for better querying and indexing.
        """
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
            "is_closed": self.is_closed
        }
