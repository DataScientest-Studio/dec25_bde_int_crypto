"""
Test fixtures for message_processor tests.

Provides reusable test data fixtures for testing message processing logic.
"""

import pytest
from datetime import datetime, timezone
from src.models.models import KlineData, KlineMessage, BinanceKline


@pytest.fixture
def raw_json_valid_message():
    """Raw JSON string of a valid WebSocket message."""
    return '''{
        "e": "kline",
        "E": 1234567890000,
        "s": "BTCUSDT",
        "k": {
            "t": 1234567800000,
            "T": 1234567859999,
            "s": "BTCUSDT",
            "i": "1m",
            "f": 100,
            "L": 200,
            "o": "50000.00",
            "c": "50100.00",
            "h": "50200.00",
            "l": "49900.00",
            "v": "10.5",
            "n": 150,
            "x": false,
            "q": "525000.00",
            "V": "5.25",
            "Q": "262500.00",
            "B": "123456"
        }
    }'''


@pytest.fixture
def raw_json_closed_message():
    """Raw JSON string of a closed candle WebSocket message."""
    return '''{
        "e": "kline",
        "E": 1234567890000,
        "s": "ETHUSDT",
        "k": {
            "t": 1234567500000,
            "T": 1234567799999,
            "s": "ETHUSDT",
            "i": "5m",
            "f": 1000,
            "L": 1200,
            "o": "3000.00",
            "c": "3050.00",
            "h": "3100.00",
            "l": "2950.00",
            "v": "100.0",
            "n": 250,
            "x": true,
            "q": "305000.00",
            "V": "50.0",
            "Q": "152500.00",
            "B": "789012"
        }
    }'''


@pytest.fixture
def raw_json_invalid():
    """Invalid JSON string."""
    return '{"invalid": json, missing quotes}'


@pytest.fixture
def parsed_valid_message():
    """Parsed valid WebSocket message dict."""
    return {
        "e": "kline",
        "E": 1234567890000,
        "s": "BTCUSDT",
        "k": {
            "t": 1234567800000,
            "T": 1234567859999,
            "s": "BTCUSDT",
            "i": "1m",
            "f": 100,
            "L": 200,
            "o": "50000.00",
            "c": "50100.00",
            "h": "50200.00",
            "l": "49900.00",
            "v": "10.5",
            "n": 150,
            "x": False,
            "q": "525000.00",
            "V": "5.25",
            "Q": "262500.00",
            "B": "123456"
        }
    }


@pytest.fixture
def parsed_closed_message():
    """Parsed closed candle WebSocket message dict."""
    return {
        "e": "kline",
        "E": 1234567890000,
        "s": "ETHUSDT",
        "k": {
            "t": 1234567500000,
            "T": 1234567799999,
            "s": "ETHUSDT",
            "i": "5m",
            "f": 1000,
            "L": 1200,
            "o": "3000.00",
            "c": "3050.00",
            "h": "3100.00",
            "l": "2950.00",
            "v": "100.0",
            "n": 250,
            "x": True,
            "q": "305000.00",
            "V": "50.0",
            "Q": "152500.00",
            "B": "789012"
        }
    }


@pytest.fixture
def parsed_invalid_event_type():
    """Message with invalid event type."""
    return {
        "e": "trade",
        "E": 1234567890000,
        "s": "BTCUSDT"
    }


@pytest.fixture
def parsed_missing_fields():
    """Message missing required fields."""
    return {
        "e": "kline",
        "E": 1234567890000,
        # Missing 's' and 'k' fields
    }


@pytest.fixture
def parsed_invalid_kline_structure():
    """Message with invalid kline structure."""
    return {
        "e": "kline",
        "E": 1234567890000,
        "s": "BTCUSDT",
        "k": "not_a_dict"  # Should be a dict
    }


@pytest.fixture
def parsed_missing_kline_fields():
    """Message with missing kline fields."""
    return {
        "e": "kline",
        "E": 1234567890000,
        "s": "BTCUSDT",
        "k": {
            "t": 1234567800000,
            # Missing required fields like 'o', 'h', 'l', 'c', 'v', 'x'
        }
    }


@pytest.fixture
def sample_kline_data():
    """Sample KlineData instance for testing."""
    binance_kline = BinanceKline(
        open_time=1234567800000,
        open=50000.00,
        high=50200.00,
        low=49900.00,
        close=50100.00,
        volume=10.5,
        close_time=1234567859999,
        quote_volume=525000.00,
        trade_count=150,
        taker_buy_base_volume=5.25,
        taker_buy_quote_volume=262500.00
    )

    return KlineData.from_kline(
        kline=binance_kline,
        symbol="BTCUSDT",
        interval="1m"
    )


@pytest.fixture
def sample_kline_message():
    """Sample KlineMessage instance for testing."""
    return KlineMessage(
        event_time=1234567890000,
        symbol="BTCUSDT",
        interval="1m",
        timestamp="2009-02-13T23:31:30+00:00",
        open=50000.00,
        high=50200.00,
        low=49900.00,
        close=50100.00,
        volume=10.5,
        quote_volume=525000.00,
        trade_count=150,
        is_closed=False
    )
