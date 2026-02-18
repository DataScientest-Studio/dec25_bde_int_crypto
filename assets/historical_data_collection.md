# Binance Klines Collector (BTCUSDT)

Collects Binance Spot **OHLCV (klines)** for **5m / 15m**, stores **raw + processed** datasets on disk, and upserts processed rows into MongoDB.
(Note: OHLCV = Open, High, Low, Close, Volume.)

**Principles**
- **Incremental**: only fetch/process missing ranges
- **Idempotent**: safe to re-run (no duplicates)
- **Traceable**: `print(...)` logs show progress + decisions

---

## Pipeline

1. Load covered windows from `*.range.json`
2. Fetch **missing raw** klines only (async + pagination)
3. Process **missing raw rows** only (validate → typed model)
4. Bulk **upsert** processed rows into MongoDB

---

## Outputs (auto-created)

### Raw (close to Binance payload)
- `data/raw_data/{SYMBOL}_{INTERVAL}.json`
- `data/raw_data/{SYMBOL}_{INTERVAL}.csv`
- `data/raw_data/{SYMBOL}_{INTERVAL}.range.json`

Raw JSON format (array-of-arrays):
```json
[ open_time_ms, "open", "high", "low", "close", "volume",
  close_time_ms, "quote_volume", trades, "taker_buy_base", "taker_buy_quote", "0" ]
````

### Processed (validated schema)

* `data/processed_data/{SYMBOL}_{INTERVAL}.json`
* `data/processed_data/{SYMBOL}_{INTERVAL}.csv`
* `data/processed_data/{SYMBOL}_{INTERVAL}.range.json`

Processed JSON format: array-of-objects (named fields).

---

## MongoDB

Default DB: `crypto_data`

Collections:

* **Historical**: `klines_historical`
* **Streaming**: `klines_streaming`

Idempotency:

### Unique index: `(symbol, interval, open_time_ms)`:
**Why unique index `(symbol, interval, open_time_ms)`?**

Because a candle is uniquely identified by its **symbol + interval + open time**. The unique index:

* **Prevents duplicates** (re-runs, retries, pagination overlap)
* Enables **idempotent upserts** (same candle updates instead of inserts)
* Helps **concurrent writers** (historical + streaming) stay consistent
* Speeds up **time-range queries** for a series

We use a **partial** unique index (`open_time_ms > 0`) to ignore legacy docs missing `open_time_ms`.

### Bulk **upsert** on re-runs prevents duplicates

## Config

```bash
export BINANCE_SYMBOL="BTCUSDT"
export BINANCE_INTERVAL="5m"          # allowed: 5m, 15m
export BINANCE_START_DATE="2026-01-01"
export BINANCE_END_DATE=""            # optional; if unset uses "now"

export MONGODB_URI="mongodb://admin:password@localhost:27017/"
export MONGODB_DATABASE="crypto_data"
export MONGODB_COLLECTION_HISTORICAL="klines_historical"
export MONGODB_COLLECTION_STREAMING="klines_streaming"
```

---

## Run

```bash
uv run python -m src.service.binance_historical_collector
```

---

## Incremental behavior

If disk already contains a superset (e.g., 2023–2026) and you request a subset (e.g., 2025), it should compute `missing=[]` and skip fetch + processing.
