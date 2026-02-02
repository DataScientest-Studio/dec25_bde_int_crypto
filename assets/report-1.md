# Stage 1 — Data discovery and organization

## 1) Introduction

We are building a data pipeline that collects crypto market data from Binance.

We will use this data for:

* training a prediction model (later stage)
* showing a live dashboard (later stage)
* serving predictions through an application interface (later stage)

Reference diagram:

![Binance Data ML Pipeline](docs/Binance Data ML Pipeline-2026-01-30-100112.png)

---

## 2) Data source

### Main data source: Binance public market data (free)

We only use public market data. This does not need a key.

Free base addresses:

* For historical requests (batch): `https://data-api.binance.vision`
* For live streaming (real time): `wss://data-stream.binance.vision:443/ws`

---

## 3) Minimum setup for BTCUSDT (1 minute + 5 minutes)

This is the smallest set of calls needed for the first working pipeline.

### A) One-time setup (run once, then rarely)

#### 1) Symbol details and rules

We use this to confirm BTCUSDT is valid and store basic symbol information.

**Call**
* `GET /api/v3/exchangeInfo`

**Example**
```bash
curl -s "https://data-api.binance.vision/api/v3/exchangeInfo?symbol=BTCUSDT"
````

**Why we need it**

* Confirms the symbol exists and is active.
* Provides base and quote assets.
* Helps us keep a correct symbol table in the database.

---

### B) Historical data (main dataset for training and backtesting)

#### 2) Candles (1 minute)

**Call**

* `GET /api/v3/klines` with `interval=1m`

**Example**

```bash
curl -s "https://data-api.binance.vision/api/v3/klines?symbol=BTCUSDT&interval=1m&limit=1000"
```

#### 3) Candles (5 minutes)

**Call**

* `GET /api/v3/klines` with `interval=5m`

**Example**

```bash
curl -s "https://data-api.binance.vision/api/v3/klines?symbol=BTCUSDT&interval=5m&limit=1000"
```

**What we get from candles**
Open time, open, high, low, close, volume, close time, trade count.

**Why we need it**

* It is stable and easy to store.
* It supports common features like price change, moving averages, and volatility.

**Important note**
Binance returns at most 1000 candles per request, so we download history in a loop using `startTime` and `endTime`.

---

### C) Live streaming (real time candles)

We use live candle updates to keep the newest data up to date without calling the historical endpoint all the time.

#### 4) Live candle updates (1 minute)

**Stream**

* `btcusdt@kline_1m`

**Example**

```text
wss://data-stream.binance.vision:443/ws/btcusdt@kline_1m
```

#### 5) Live candle updates (5 minutes)

**Stream**

* `btcusdt@kline_5m`

**Example**

```text
wss://data-stream.binance.vision:443/ws/btcusdt@kline_5m
```

---

## 4) Optional endpoints (nice to have)

These are not required for the first working pipeline, but they are useful later.

### Best bid and ask (closest buy and sell prices)

Use this for spread and mid-price features.

**Call**

* `GET /api/v3/ticker/bookTicker`

**Example**

```bash
curl -s "https://data-api.binance.vision/api/v3/ticker/bookTicker?symbol=BTCUSDT"
```

### Order book snapshot (optional)

An order book snapshot shows the current waiting buy offers (bids) and sell offers (asks) at different prices.
This helps us measure the difference between the best buy and best sell prices (spread) and estimate buyer vs seller pressure (imbalance).

**Endpoint (free):** `GET /api/v3/depth`
**Example:**

```bash
curl -s "https://data-api.binance.vision/api/v3/depth?symbol=BTCUSDT&limit=100"
```

We treat this as optional because it changes very fast and can increase the amount of data we store.

### Live trades (optional, high volume)

Use this only if you want trade-based features.

**Stream**

* `btcusdt@trade`

**Example**

```text
wss://data-stream.binance.vision:443/ws/btcusdt@trade
```

---

## 5) Which data we will use in this project

### Main data (must have)

1. Candles (1 minute and 5 minutes)

* open, high, low, close, volume, trade count, open time, close time

2. Symbol information

* symbol name, base asset, quote asset, trading status

### Extra data (nice to have)

3. Best bid and ask

* best buy price and best sell price (spread)

4. Order book snapshot

* top bids and asks (for imbalance)

5. Trades (streaming)

* trade price, trade size, trade time (high volume)

---

## 6) How we store the data (simple plan)

### Relational database (structured tables)

Store:

* candle history (main training dataset)
* symbol table (metadata)

Keys to link tables:

* `symbol`
* `open_time` + `interval` for candles

### Document store (fast live data)

Store:

* live candle events (for dashboard)
* live trades (if used)

---

## 7) Stage 1 output (what we have decided)

Minimum setup (first working pipeline):

* Use Binance free public endpoints.
* Use BTCUSDT candles as the main dataset (1 minute and 5 minutes).
* Collect history using the candle endpoint.
* Keep the newest candles up to date using live candle streams.
* Store historical candles in a relational database.
* Store live events in a document store for dashboards.

---

## Goal → endpoint → fields → where we store it (BTCUSDT focused)

| Goal                                          | Binance endpoint (free)                            | Main fields we keep                                                                                      | Where we store it                                             |
| --------------------------------------------- | -------------------------------------------------- | -------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------- |
| Confirm symbol and store symbol metadata      | `GET /api/v3/exchangeInfo?symbol=BTCUSDT`          | `symbol`, `status`, `baseAsset`, `quoteAsset`                                                            | Relational table: `symbols`                                   |
| Download historical candles (1 minute)        | `GET /api/v3/klines?symbol=BTCUSDT&interval=1m...` | `symbol`, `interval`, `open_time`, `open`, `high`, `low`, `close`, `volume`, `close_time`, `trade_count` | Relational table: `candles`                                   |
| Download historical candles (5 minutes)       | `GET /api/v3/klines?symbol=BTCUSDT&interval=5m...` | same as above                                                                                            | Relational table: `candles`                                   |
| Keep latest candles updated (1 minute)        | WebSocket: `btcusdt@kline_1m`                      | candle fields (especially `open_time`, `close`, `volume`, `trade_count`)                                 | Document store (live events) + optional upsert into `candles` |
| Keep latest candles updated (5 minutes)       | WebSocket: `btcusdt@kline_5m`                      | same as above                                                                                            | Document store (live events) + optional upsert into `candles` |
| Track current best buy/sell prices (optional) | `GET /api/v3/ticker/bookTicker`                    | `symbol`, `bidPrice`, `bidQty`, `askPrice`, `askQty`, local timestamp                                    | Document store (dashboard)                                    |
| Order book snapshot (optional)                | `GET /api/v3/depth`                                | `symbol`, top `bids[]`, top `asks[]`, `lastUpdateId`, local timestamp                                    | Document store                                                |
| Live trades (optional)                        | WebSocket: `btcusdt@trade`                         | `symbol`, `trade_id`, `price`, `qty`, `trade_time`, `is_buyer_maker`                                     | Document store                                                |

---

## Ready-to-use example calls (copy/paste)

### Symbol metadata

```bash
curl -s "https://data-api.binance.vision/api/v3/exchangeInfo?symbol=BTCUSDT"
```

### Historical candles (1 minute)

```bash
curl -s "https://data-api.binance.vision/api/v3/klines?symbol=BTCUSDT&interval=1m&limit=1000"
```

### Historical candles (5 minutes)

```bash
curl -s "https://data-api.binance.vision/api/v3/klines?symbol=BTCUSDT&interval=5m&limit=1000"
```

### Live candle stream (1 minute)

```text
wss://data-stream.binance.vision:443/ws/btcusdt@kline_1m
```

### Live candle stream (5 minutes)

```text
wss://data-stream.binance.vision:443/ws/btcusdt@kline_5m
```

### Current best bid/ask (optional)

```bash
curl -s "https://data-api.binance.vision/api/v3/ticker/bookTicker?symbol=BTCUSDT"
```

### Order book snapshot (optional)

```bash
curl -s "https://data-api.binance.vision/api/v3/depth?symbol=BTCUSDT&limit=100"
```

### Live trade stream (optional)

```text
wss://data-stream.binance.vision:443/ws/btcusdt@trade
```
