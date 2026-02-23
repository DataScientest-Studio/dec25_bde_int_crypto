"""Constants for Binance API configuration (env-overridable)."""

from __future__ import annotations

import os

# Binance REST klines endpoint
BASE_URL: str = os.getenv("BINANCE_KLINES_URL", "https://api.binance.com/api/v3/klines")
MAX_LIMIT: int = int(os.getenv("BINANCE_MAX_LIMIT", "1000"))
PAGE_SLEEP_S: float = float(os.getenv("BINANCE_PAGE_SLEEP", "0.25"))

# Defaults for the collector
SYMBOL: str = os.getenv("BINANCE_SYMBOL", "BTCUSDT")
INTERVAL: str = os.getenv("BINANCE_INTERVAL", "5m")  # allowed: 5m, 15m
START_DATE: str = os.getenv("BINANCE_START_DATE", "2025-01-01")
END_DATE: str | None = os.getenv("BINANCE_END_DATE") or None
