"""Constants (env-overridable)."""

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

# Data directory layout
DATA_DIR: str = os.getenv("DATA_DIR", "data")
RAW_DATA_DIRNAME: str = os.getenv("RAW_DATA_DIRNAME", "raw_data")
PROCESSED_DATA_DIRNAME: str = os.getenv("PROCESSED_DATA_DIRNAME", "processed_data")

# MongoDB
MONGODB_URI: str = os.getenv("MONGODB_URI", "mongodb://admin:password@localhost:27017/")
MONGODB_DATABASE: str = os.getenv("MONGODB_DATABASE", "crypto_data")
MONGODB_COLLECTION: str = os.getenv("MONGODB_COLLECTION", "klines")
