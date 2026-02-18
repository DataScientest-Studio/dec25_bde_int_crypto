"""
Unit tests for KlineMapper.

Tests the mapping logic between Binance WebSocket messages and domain models.
"""

import pytest
from datetime import datetime, timezone
from src.mappers.kline_mapper import KlineMapper
from src.models.models import KlineData, KlineMessage

# Import fixtures from separate file
pytest_plugins = ['tests.mappers.fixtures']


class TestKlineMapperParseWebSocketMessage:
    """Tests for parse_websocket_message method."""

    def test_parse_valid_open_candle(self, valid_websocket_message):
        """Test parsing a valid open (not closed) kline message."""
        result = KlineMapper.parse_websocket_message(valid_websocket_message)

        assert result is not None
        kline, is_closed = result

        # Check KlineData fields
        assert isinstance(kline, KlineData)
        assert kline.symbol == "BTCUSDT"
        assert kline.interval == "1m"
        assert kline.open == 50000.00
        assert kline.high == 50200.00
        assert kline.low == 49900.00
        assert kline.close == 50100.00
        assert kline.volume == 10.5
        assert kline.quote_volume == 525000.00
        assert kline.trade_count == 150

        # Check is_closed flag
        assert is_closed is False

    def test_parse_valid_closed_candle(self, closed_websocket_message):
        """Test parsing a valid closed kline message."""
        result = KlineMapper.parse_websocket_message(closed_websocket_message)

        assert result is not None
        kline, is_closed = result

        # Check KlineData fields
        assert isinstance(kline, KlineData)
        assert kline.symbol == "ETHUSDT"
        assert kline.interval == "5m"
        assert kline.open == 3000.00
        assert kline.high == 3100.00
        assert kline.low == 2950.00
        assert kline.close == 3050.00
        assert kline.volume == 100.0
        assert kline.quote_volume == 305000.00
        assert kline.trade_count == 250

        # Check is_closed flag
        assert is_closed is True

    def test_parse_invalid_event_type(self, invalid_event_type_message):
        """Test parsing returns None for non-kline events."""
        result = KlineMapper.parse_websocket_message(invalid_event_type_message)
        assert result is None

    def test_parse_malformed_message_raises_error(self, malformed_message):
        """Test parsing malformed message raises KeyError."""
        with pytest.raises(KeyError):
            KlineMapper.parse_websocket_message(malformed_message)

    def test_parse_empty_message(self):
        """Test parsing empty message returns None."""
        result = KlineMapper.parse_websocket_message({})
        assert result is None

    def test_parse_missing_kline_data(self):
        """Test parsing message without 'k' field raises error."""
        message = {"e": "kline", "E": 1234567890000, "s": "BTCUSDT"}
        with pytest.raises(KeyError):
            KlineMapper.parse_websocket_message(message)


class TestKlineMapperToKafkaMessage:
    """Tests for to_kafka_message method."""

    def test_to_kafka_message_with_open_candle(self, valid_websocket_message):
        """Test converting KlineData to KlineMessage for open candle."""
        # First parse the message
        kline, is_closed = KlineMapper.parse_websocket_message(valid_websocket_message)
        event_time = valid_websocket_message['E']

        # Convert to Kafka message
        kafka_msg = KlineMapper.to_kafka_message(kline, event_time, is_closed)

        assert isinstance(kafka_msg, KlineMessage)
        assert kafka_msg.event_time == 1234567890000
        assert kafka_msg.symbol == "BTCUSDT"
        assert kafka_msg.interval == "1m"
        assert kafka_msg.open == 50000.00
        assert kafka_msg.high == 50200.00
        assert kafka_msg.low == 49900.00
        assert kafka_msg.close == 50100.00
        assert kafka_msg.volume == 10.5
        assert kafka_msg.quote_volume == 525000.00
        assert kafka_msg.trade_count == 150
        assert kafka_msg.is_closed is False
        # Check timestamp is ISO format string
        assert isinstance(kafka_msg.timestamp, str)
        assert "T" in kafka_msg.timestamp

    def test_to_kafka_message_with_closed_candle(self, closed_websocket_message):
        """Test converting KlineData to KlineMessage for closed candle."""
        # First parse the message
        kline, is_closed = KlineMapper.parse_websocket_message(closed_websocket_message)
        event_time = closed_websocket_message['E']

        # Convert to Kafka message
        kafka_msg = KlineMapper.to_kafka_message(kline, event_time, is_closed)

        assert isinstance(kafka_msg, KlineMessage)
        assert kafka_msg.event_time == 1234567890000
        assert kafka_msg.symbol == "ETHUSDT"
        assert kafka_msg.interval == "5m"
        assert kafka_msg.is_closed is True


class TestKlineMapperWebSocketToKafkaMessage:
    """Tests for websocket_to_kafka_message convenience method."""

    def test_websocket_to_kafka_full_conversion(self, valid_websocket_message):
        """Test full conversion from WebSocket message to Kafka message."""
        result = KlineMapper.websocket_to_kafka_message(valid_websocket_message)

        assert result is not None
        kafka_msg, kline, is_closed = result

        # Check all three return values
        assert isinstance(kafka_msg, KlineMessage)
        assert isinstance(kline, KlineData)
        assert isinstance(is_closed, bool)

        # Check consistency between kafka_msg and kline
        assert kafka_msg.symbol == kline.symbol
        assert kafka_msg.interval == kline.interval
        assert kafka_msg.open == kline.open
        assert kafka_msg.high == kline.high
        assert kafka_msg.low == kline.low
        assert kafka_msg.close == kline.close
        assert kafka_msg.volume == kline.volume
        assert kafka_msg.is_closed == is_closed

    def test_websocket_to_kafka_with_closed_candle(self, closed_websocket_message):
        """Test full conversion with closed candle."""
        result = KlineMapper.websocket_to_kafka_message(closed_websocket_message)

        assert result is not None
        kafka_msg, kline, is_closed = result

        assert is_closed is True
        assert kafka_msg.is_closed is True

    def test_websocket_to_kafka_invalid_event(self, invalid_event_type_message):
        """Test full conversion returns None for invalid event type."""
        result = KlineMapper.websocket_to_kafka_message(invalid_event_type_message)
        assert result is None

    def test_websocket_to_kafka_malformed_message(self, malformed_message):
        """Test full conversion raises error for malformed message."""
        with pytest.raises(KeyError):
            KlineMapper.websocket_to_kafka_message(malformed_message)


class TestKlineMapperEdgeCases:
    """Tests for edge cases and boundary conditions."""

    def test_very_small_prices(self):
        """Test parsing with very small price values."""
        message = {
            "e": "kline",
            "E": 1234567890000,
            "s": "DOGEUSDT",
            "k": {
                "t": 1234567800000,
                "T": 1234567859999,
                "s": "DOGEUSDT",
                "i": "1m",
                "f": 100,
                "L": 200,
                "o": "0.00001234",
                "c": "0.00001235",
                "h": "0.00001240",
                "l": "0.00001230",
                "v": "1000000.0",
                "n": 500,
                "x": False,
                "q": "12.35",
                "V": "500000.0",
                "Q": "6.175",
                "B": "123456"
            }
        }

        result = KlineMapper.parse_websocket_message(message)
        assert result is not None
        kline, is_closed = result

        assert kline.open == pytest.approx(0.00001234, rel=1e-8)
        assert kline.close == pytest.approx(0.00001235, rel=1e-8)

    def test_very_large_volumes(self):
        """Test parsing with very large volume values."""
        message = {
            "e": "kline",
            "E": 1234567890000,
            "s": "BTCUSDT",
            "k": {
                "t": 1234567800000,
                "T": 1234567859999,
                "s": "BTCUSDT",
                "i": "1d",
                "f": 100,
                "L": 20000,
                "o": "50000.00",
                "c": "50100.00",
                "h": "51000.00",
                "l": "49000.00",
                "v": "999999999.99",
                "n": 1000000,
                "x": True,
                "q": "50000000000.00",
                "V": "500000000.00",
                "Q": "25000000000.00",
                "B": "123456"
            }
        }

        result = KlineMapper.parse_websocket_message(message)
        assert result is not None
        kline, is_closed = result

        assert kline.volume == pytest.approx(999999999.99, rel=1e-6)
        assert kline.quote_volume == pytest.approx(50000000000.00, rel=1e-6)

    def test_zero_volume(self):
        """Test parsing with zero volume."""
        message = {
            "e": "kline",
            "E": 1234567890000,
            "s": "BTCUSDT",
            "k": {
                "t": 1234567800000,
                "T": 1234567859999,
                "s": "BTCUSDT",
                "i": "1m",
                "f": 100,
                "L": 100,
                "o": "50000.00",
                "c": "50000.00",
                "h": "50000.00",
                "l": "50000.00",
                "v": "0.0",
                "n": 0,
                "x": False,
                "q": "0.0",
                "V": "0.0",
                "Q": "0.0",
                "B": "123456"
            }
        }

        result = KlineMapper.parse_websocket_message(message)
        assert result is not None
        kline, is_closed = result

        assert kline.volume == 0.0
        assert kline.trade_count == 0
