# Binance Historical Klines Collector (BTCUSDT)

Collects Binance Spot **OHLCV (klines)** for **5m / 15m**, writes **raw + processed** datasets to disk, and **upserts** processed records into MongoDB.

Goals:
- **Incremental**: don’t re-download / re-process what already exists
- **Idempotent**: safe to re-run (no duplicates)
- **Traceable**: `print(...)` statements show control flow and progress

---

## Pipeline

On each run:

1) **Read state** from range files (`*.range.json`)
2) **Fetch missing raw data only** (async HTTP + pagination)
3) **Process missing data only** (validate + map raw rows → typed objects)
4) **Upsert new processed rows** into MongoDB (bulk upsert)

You’ll see prints for:
- existing vs requested ranges
- missing ranges
- fetch page progress
- counts fetched/processed/skipped/upserted

---

## Outputs

Created automatically:

### Raw (compact, close to Binance payload)
- `data/raw_data/{SYMBOL}_{INTERVAL}.json`
- `data/raw_data/{SYMBOL}_{INTERVAL}.csv`
- `data/raw_data/{SYMBOL}_{INTERVAL}.range.json`

Raw JSON is stored as **array-of-arrays**:
```json
[ open_time_ms, "open", "high", "low", "close", "volume",
  close_time_ms, "quote_volume", trades, "taker_buy_base", "taker_buy_quote", "0" ]
````

### Processed (validated, stable schema)

* `data/processed_data/{SYMBOL}_{INTERVAL}.json`
* `data/processed_data/{SYMBOL}_{INTERVAL}.csv`
* `data/processed_data/{SYMBOL}_{INTERVAL}.range.json`

Processed JSON is **array-of-objects** with named fields.

---

## MongoDB

Writes to:

* DB: `crypto_data` (default)
* Collection: `klines` (default)

Idempotency:

* Unique index on `(symbol, interval, open_time_ms)`
* Bulk **upsert** prevents duplicates on re-runs

---

## Code layout

* `src/models/binance_models.py`

  * `HistoricalKline` (Option B): timestamps stored as **unix ms ints**
  * Validates OHLC consistency + non-negative volumes
  * `from_binance(...)`, `to_processed_row()`, `to_mongo_doc()`

* `src/service/binance_historical_collector.py`

  * Orchestrates: read state → fetch missing → process missing → upsert

* `src/service/mongo_repository.py`

  * Ensures indexes + bulk upsert

---

## Libraries added

* **httpx**: async HTTP client used to call Binance REST efficiently (connection pooling, timeouts, clean async API).
* **motor**: async MongoDB driver (non-blocking DB writes in an async pipeline; wraps PyMongo cleanly).

---

## Configuration

```bash
export BINANCE_SYMBOL="BTCUSDT"
export BINANCE_INTERVAL="5m"          # allowed: 5m, 15m
export BINANCE_START_DATE="2023-02-08"
export BINANCE_END_DATE="2026-02-08"  # optional; if unset, uses "now"

export MONGODB_URI="mongodb://admin:password@localhost:27017/"
export MONGODB_DATABASE="crypto_data"
export MONGODB_COLLECTION="klines"
```

---

## Run

```bash
uv run python -m src.service.binance_historical_collector
```

---

## Incremental behavior

If you already downloaded a superset (e.g., 2023–2026), requesting a subset (e.g., 2025 only) should show `missing=[]` and **skip refetch/reprocess**.
