"""
Test fixtures for mapper tests.

This module provides reusable test data fixtures for testing mappers.
"""

import pytest


@pytest.fixture
def valid_websocket_message():
    """Fixture providing a valid Binance WebSocket kline message."""
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
def closed_websocket_message():
    """Fixture providing a closed kline WebSocket message."""
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
            "x": True,  # Closed candle
            "q": "305000.00",
            "V": "50.0",
            "Q": "152500.00",
            "B": "789012"
        }
    }


@pytest.fixture
def invalid_event_type_message():
    """Fixture providing a message with invalid event type."""
    return {
        "e": "trade",  # Not a kline event
        "E": 1234567890000,
        "s": "BTCUSDT"
    }


@pytest.fixture
def malformed_message():
    """Fixture providing a malformed message missing required fields."""
    return {
        "e": "kline",
        "E": 1234567890000,
        "s": "BTCUSDT",
        "k": {
            "t": 1234567800000,
            # Missing required fields
        }
    }
