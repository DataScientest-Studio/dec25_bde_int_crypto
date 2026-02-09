"""
Mapper for converting Binance WebSocket messages to KlineMessage objects.

This module provides clean separation between raw message parsing
and business logic in the stream producer.
"""

from typing import Optional
from src.models.models import BinanceKline, KlineData, KlineMessage


class KlineMapper:
    """Maps Binance WebSocket kline messages to structured models."""

    @staticmethod
    def parse_websocket_message(message: dict) -> Optional[tuple[KlineData, bool]]:
        """
        Parse WebSocket kline message into KlineData model.

        Binance kline WebSocket message format:
        {
          "e": "kline",     // Event type
          "E": 123456789,   // Event time
          "s": "BTCUSDT",   // Symbol
          "k": {
            "t": 123400000, // Kline start time
            "T": 123460000, // Kline close time
            "s": "BTCUSDT", // Symbol
            "i": "1m",      // Interval
            "f": 100,       // First trade ID
            "L": 200,       // Last trade ID
            "o": "0.0010",  // Open price
            "c": "0.0020",  // Close price
            "h": "0.0025",  // High price
            "l": "0.0015",  // Low price
            "v": "1000",    // Base asset volume
            "n": 100,       // Number of trades
            "x": false,     // Is this kline closed?
            "q": "1.0000",  // Quote asset volume
            "V": "500",     // Taker buy base asset volume
            "Q": "0.500",   // Taker buy quote asset volume
            "B": "123456"   // Ignore
          }
        }

        Args:
            message: Raw WebSocket message dict

        Returns:
            Tuple of (KlineData, is_closed) or None if parsing fails
        """
        if message.get('e') != 'kline':
            return None

        kline_data = message.get('k', {})

        # Create BinanceKline from WebSocket data
        binance_kline = BinanceKline(
            open_time=kline_data['t'],
            open=float(kline_data['o']),
            high=float(kline_data['h']),
            low=float(kline_data['l']),
            close=float(kline_data['c']),
            volume=float(kline_data['v']),
            close_time=kline_data['T'],
            quote_volume=float(kline_data['q']),
            trade_count=kline_data['n'],
            taker_buy_base_volume=float(kline_data['V']),
            taker_buy_quote_volume=float(kline_data['Q'])
        )

        # Convert to enriched KlineData
        kline = KlineData.from_kline(
            kline=binance_kline,
            symbol=kline_data['s'],
            interval=kline_data['i']
        )

        # Extract is_closed flag
        is_closed = kline_data['x']

        return kline, is_closed

    @staticmethod
    def to_kafka_message(kline: KlineData, event_time: int, is_closed: bool) -> KlineMessage:
        """
        Convert KlineData to KlineMessage for Kafka publishing.

        Args:
            kline: Parsed kline data
            event_time: Event time from WebSocket message (milliseconds)
            is_closed: Whether the kline/candle is closed

        Returns:
            KlineMessage ready for Kafka serialization
        """
        return KlineMessage(
            event_time=event_time,
            symbol=kline.symbol,
            interval=kline.interval,
            timestamp=kline.timestamp.isoformat(),
            open=kline.open,
            high=kline.high,
            low=kline.low,
            close=kline.close,
            volume=kline.volume,
            quote_volume=kline.quote_volume,
            trade_count=kline.trade_count,
            is_closed=is_closed
        )

    @classmethod
    def websocket_to_kafka_message(cls, message: dict) -> Optional[tuple[KlineMessage, KlineData, bool]]:
        """
        One-step conversion from raw WebSocket message to KlineMessage.

        This is a convenience method that combines parse_websocket_message
        and to_kafka_message for cleaner usage in the stream producer.

        Args:
            message: Raw WebSocket message dict

        Returns:
            Tuple of (KlineMessage, KlineData, is_closed) or None if parsing fails
        """
        result = cls.parse_websocket_message(message)
        if result is None:
            return None

        kline, is_closed = result
        event_time = message.get('E')
        kafka_message = cls.to_kafka_message(kline, event_time, is_closed)

        return kafka_message, kline, is_closed
