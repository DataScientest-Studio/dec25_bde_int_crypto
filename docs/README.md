# Crypto Data Engineering And ML Pipeline Guide

## 1. Project Goal

This project is a data engineering and machine learning pipeline built around Binance market data.

At a high level, the system does four things:

1. Collects historical market candles from Binance.
2. Stores the data locally in raw and processed form.
3. Loads the processed historical data into MongoDB.
4. Trains and serves a logistic regression model on top of that historical data.

The project also includes a streaming path through Redpanda and a Grafana-based visualization layer.

## 2. System Overview

There are two main data paths in the repository.

### Historical batch path

This is the path used by the current machine learning workflow.

```text
Binance REST API
  -> raw CSV files
  -> processed CSV files
  -> MongoDB klines_historical
  -> model training
  -> prediction API
```

### Streaming path

This path is useful for near-real-time ingestion and inspection, but it is not the current source for logistic regression training.

```text
Binance WebSocket
  -> Redpanda
  -> streaming consumer
  -> MongoDB klines_streaming
```

## 3. Main Services

The core services are defined in [`docker-compose.yml`](../docker-compose.yml).

| Service | Purpose | Port |
|---|---|---|
| `mongodb` | Primary database for historical and streaming data | `27017` |
| `mongo-express` | Browser UI for inspecting MongoDB | `8082` |
| `binance-collector` | Historical data pipeline | none |
| `model-trainer` | One-shot training job for the logistic regression model | none |
| `main` | Main FastAPI application with prediction and Grafana routes | `8000` |
| `prediction-api` | Separate API container that mounts shared model artifacts | `8001` |
| `grafana` | Dashboard UI | `3000` |
| `redpanda-0` | Streaming broker | `19092` externally |
| `console` | Redpanda Console UI | `8080` |

## 4. Repository Structure

These directories matter most when you are trying to understand the codebase:

- `src/service/batch/`
  Historical collection and processing code.
- `src/service/predict/logistic_regression/`
  Model training and service-layer prediction logic.
- `src/service/stream/`
  Streaming consumer and WebSocket-related code.
- `src/api/routers/`
  Thin FastAPI routers.
- `src/api/main.py`
  The active FastAPI application entrypoint used by Docker Compose.
- `src/config/`
  Environment-based configuration, including shared MongoDB settings.
- `src/database/`
  Shared MongoDB helpers and client wrapper.
- `scripts/`
  Operational scripts used by containers.
- `data/raw_data/`
  Raw historical Binance files.
- `data/processed_data/`
  Cleaned and normalized historical rows.
- `grafana/provisioning/`
  Grafana datasource and dashboard provisioning files.

Important note:

- [`src/api/app.py`](../src/api/app.py) still exists, but it is a legacy FastAPI file and is not the application used by the Docker stack.
- The active app is [`src/api/main.py`](../src/api/main.py).

## 5. Configuration And Environment Variables

MongoDB settings are loaded centrally through [`src/config/mongo_settings.py`](../src/config/mongo_settings.py).

That settings module walks upward from its own location to discover the nearest `.env` file, which makes configuration less dependent on the current working directory.

The most important environment variables in the current project are:

- `MONGODB_URI`
- `MONGODB_DATABASE`
- `MONGODB_COLLECTION_HISTORICAL`
- `MONGODB_COLLECTION_STREAMING`
- `BINANCE_SYMBOL`
- `BINANCE_INTERVAL`
- `MODEL_DIR`
- `MODEL_PATH`
- `SCALER_PATH`

General rule:

- avoid introducing new hardcoded MongoDB URIs inside feature code
- reuse the shared settings loader and shared Mongo client instead

## 6. Historical Pipeline

The historical pipeline is implemented in [`src/service/batch/binance_historical_collector.py`](../src/service/batch/binance_historical_collector.py).

This is the most important flow in the project because the current ML pipeline depends on it.

### Step 1: Fetch historical candles

The collector requests historical klines from the Binance REST API.

### Step 2: Save raw files

The raw response is written to `data/raw_data/`.

This preserves the original downloaded form and gives you a local audit trail.

### Step 3: Build processed files

The collector validates and normalizes the raw rows and writes cleaned records into `data/processed_data/`.

This stage is where the project moves from raw exchange data into the internal schema used by the rest of the system.

### Step 4: Upsert into MongoDB

The processed historical rows are written into MongoDB collection `klines_historical`.

That collection is the current source for:

- model training
- model inference
- Grafana historical visualization

### Recovery behavior

The collector includes a useful recovery path:

- if MongoDB is empty but `data/processed_data/` already exists, the collector can backfill MongoDB from the processed files instead of re-downloading all historical data

This is especially helpful when Mongo is reset but the processed dataset is still available on disk.

## 7. Collector Runtime Scripts

The collector container uses:

- [`scripts/collector-entrypoint.sh`](../scripts/collector-entrypoint.sh)
- [`scripts/collector.crontab`](../scripts/collector.crontab)

### What `collector-entrypoint.sh` does

This script exists to make the collector container self-starting and operational.

It performs three tasks:

1. Copies the cron definition into `/etc/cron.d/` inside the container.
2. Runs one immediate historical sync when the container starts.
3. Starts cron in the foreground so the container stays alive.

That startup sync is important because it avoids waiting a full hour before the collector does any useful work.

### What `collector.crontab` does

The cron file runs the historical collector every hour on the hour.

That means the runtime behavior is:

- one immediate sync at container startup
- one scheduled sync every hour after that

## 8. MongoDB Collections

The project currently uses separate collections for historical and streaming data.

### `klines_historical`

This collection stores cleaned historical candles produced by the batch collector.

The historical schema is based on fields such as:

- `open_time_ms`
- `close_time_ms`
- `open`
- `high`
- `low`
- `close`
- `volume`
- `trade_count`
- `taker_buy_base_volume`

### `klines_streaming`

This collection stores real-time or near-real-time data from the streaming path.

It is not the current training source for the logistic regression model.

## 9. Machine Learning Pipeline

### Data source

The logistic regression model trains on historical data from:

- database: `crypto_data`
- collection: `klines_historical`

### Training flow

The training entrypoint is [`src/service/predict/logistic_regression/train.py`](../src/service/predict/logistic_regression/train.py).

The training process is:

1. Read historical rows from MongoDB.
2. Convert Mongo numeric values into floats suitable for pandas and scikit-learn.
3. Rebuild the feature columns.
4. Build the target label.
5. Split the data chronologically.
6. Fit a `StandardScaler`.
7. Train a `LogisticRegression` model.
8. Save the model and scaler artifacts.

### Features used today

The current model uses these features:

- `log_return`
- `volatility`
- `ma_10`
- `ma_30`
- `momentum`
- `buy_ratio`
- `spread`
- `trade_count`

### Label definition

The current target label is based on whether the next candle close is greater than the current candle open.

This behavior was kept during refactoring to avoid silently changing model meaning.

### Artifact output

During Docker-based training, the artifacts are written to:

- `/app/models/logistic_regression_model.pkl`
- `/app/models/logistic_regression_scaler.pkl`

Those files live in the shared `model_artifacts` volume.

## 10. Prediction Architecture

Prediction logic is intentionally split between the service layer and the API layer.

### Service layer

[`src/service/predict/logistic_regression/predictor.py`](../src/service/predict/logistic_regression/predictor.py) is responsible for:

- loading the trained model and scaler
- reading historical data from MongoDB
- rebuilding the same features used during training
- producing prediction results

### Router layer

[`src/api/routers/predict_logistic_regression.py`](../src/api/routers/predict_logistic_regression.py) is responsible for:

- HTTP routing
- request validation
- response formatting

This separation is important. Routers should stay thin, and business logic should stay in the service layer.

### Why there are two API containers

Both `main` and `prediction-api` run the same FastAPI application from [`src/api/main.py`](../src/api/main.py).

The difference is operational:

- `main` is the general project API
- `prediction-api` is the model-artifact-mounted prediction container

So this is not two different API codebases. It is one application deployed with two roles.

## 11. Local Access And URLs

When the Docker stack is running, these are the most useful URLs:

| Service | URL | Notes |
|---|---|---|
| Main API | `http://localhost:8000` | General API, prediction, and Grafana routes |
| Prediction API | `http://localhost:8001` | Dedicated prediction container |
| Grafana | `http://localhost:3000` | Login: `admin` / `admin` |
| Mongo Express | `http://localhost:8082` | Login: `admin` / `password` |
| Redpanda Console | `http://localhost:8080` | Streaming inspection |
| MongoDB | `mongodb://localhost:27017` | Local dev database |

## 12. Main API Endpoints

### General API

- `GET /`
  Basic service health and endpoint summary.
- `GET /docs`
  Swagger UI for the active FastAPI application.

### Prediction

- `GET /predict/logistic/{symbol}?limit=20`
  Returns the latest logistic regression predictions for the requested symbol.
- `GET /predict/logistic/status/check`
  Returns model, scaler, feature, and MongoDB status information.

Examples:

```bash
curl "http://localhost:8000/predict/logistic/BTCUSDT?limit=20"
curl "http://localhost:8001/predict/logistic/BTCUSDT?limit=20"
```

### Grafana support routes

- `GET /grafana/search`
- `POST /grafana/query`
- `GET /grafana/annotations`

These routes exist on the `main` API container and are intended for Grafana integration.

## 13. Grafana Guide

Grafana is intended to visualize historical data through the FastAPI Grafana router.

The intended path is:

```text
Grafana -> FastAPI /grafana/* routes -> MongoDB historical data
```

### How to access Grafana

- URL: `http://localhost:3000`
- username: `admin`
- password: `admin`

### Provisioned dashboard

The main provisioned dashboard file is:

- [`grafana/provisioning/dashboards/crypto-realtime.json`](../grafana/provisioning/dashboards/crypto-realtime.json)

The dashboard title is:

- `Binance Real-Time Crypto Dashboard`

The dashboard UID is:

- `binance-crypto-realtime`

If provisioning is working, this dashboard should already appear in Grafana without manual import.

### Expected internal base URL

Inside the Docker network, Grafana should call:

```text
http://main:8000/grafana
```

### Metrics exposed by the current router

The current router advertises:

- `btcusdt_close`
- `btcusdt_open`
- `btcusdt_high`
- `btcusdt_low`
- `btcusdt_volume`
- `btcusdt_quote_volume`
- `btcusdt_trade_count`

## 14. Quick Start Runbook

This is the simplest newcomer workflow.

### Step 1: Start the core stack

```bash
docker compose up -d mongodb main grafana binance-collector prediction-api
```

### Step 2: Confirm services are up

```bash
docker compose ps
```

### Step 3: Check that historical data exists

```bash
docker exec mongodb mongosh --quiet \
  --username admin \
  --password password \
  --authenticationDatabase admin \
  --eval 'db.getSiblingDB("crypto_data").klines_historical.countDocuments({symbol:"BTCUSDT", interval:"5m"})'
```

### Step 4: Train the model

```bash
docker compose run --rm model-trainer
```

### Step 5: Restart the prediction API after retraining

```bash
docker compose restart prediction-api
```

### Step 6: Check prediction status

```bash
curl http://localhost:8001/predict/logistic/status/check
```

### Step 7: Call the prediction endpoint

```bash
curl "http://localhost:8001/predict/logistic/BTCUSDT?limit=20"
```

### Step 8: Open Grafana

1. Open `http://localhost:3000`.
2. Sign in with `admin` / `admin`.
3. Open `Binance Real-Time Crypto Dashboard`.

## 15. Common Operational Commands

Watch the historical collector:

```bash
docker compose logs -f binance-collector
```

Watch the main API:

```bash
docker compose logs -f main
```

Watch Grafana:

```bash
docker compose logs -f grafana
```

Check Grafana search endpoint:

```bash
curl http://localhost:8000/grafana/search
```

Check Grafana query endpoint:

```bash
curl -X POST http://localhost:8000/grafana/query \
  -H "Content-Type: application/json" \
  -d '{
    "targets": [{"target": "btcusdt_close"}],
    "range": {
      "from": "2026-03-18T00:00:00Z",
      "to": "2026-03-18T23:59:59Z"
    },
    "maxDataPoints": 500
  }'
```

Run the prediction service locally from Python:

```bash
python -m src.service.predict.logistic_regression.predictor
```

## 16. Troubleshooting

### Prediction returns `No data found`

This usually means MongoDB does not contain historical rows for the requested symbol and interval.

Check:

1. the collector ran successfully
2. `klines_historical` contains documents
3. the requested symbol and interval match the available data

If needed, inspect:

```bash
docker compose logs -f binance-collector
```

### Prediction API says the model is not loaded

Usually this means the artifacts have not been trained yet, were not written to the shared volume, or the API container needs to be restarted.

Run:

```bash
docker compose run --rm model-trainer
docker compose restart prediction-api
```

### Grafana opens but panels are empty

Check:

1. `main` is up
2. `mongodb` is up
3. historical data exists in `klines_historical`
4. `GET /grafana/search` works
5. Grafana is pointed at the correct route prefix

### The dashboard is missing from Grafana

Check:

1. Grafana started with the mounted provisioning directory
2. [`grafana/provisioning/dashboards/dashboard.yml`](../grafana/provisioning/dashboards/dashboard.yml) still points at `/etc/grafana/provisioning/dashboards`
3. [`grafana/provisioning/dashboards/crypto-realtime.json`](../grafana/provisioning/dashboards/crypto-realtime.json) is present
4. Grafana logs do not show provisioning errors

## 17. Current Known Gaps

Some parts of the repository are still transitional. Newcomers should know this up front.

### Grafana mismatch

There are still Grafana-related inconsistencies in the current codebase:

1. [`grafana/provisioning/datasources/crypto-api.yml`](../grafana/provisioning/datasources/crypto-api.yml) still points at `http://grafana-api:8000`.
2. [`grafana/provisioning/dashboards/crypto-realtime.json`](../grafana/provisioning/dashboards/crypto-realtime.json) still uses root-level paths such as `/query` instead of `/grafana/query`.
3. [`src/api/routers/grafana.py`](../src/api/routers/grafana.py) currently queries a `timestamp` field, while the historical collection uses `open_time_ms` and `close_time_ms`.

Because of that, Grafana may load successfully while still showing empty panels.

### Legacy API file

[`src/api/app.py`](../src/api/app.py) is still present, but it should be treated as legacy.

For current behavior, trust:

1. [`docker-compose.yml`](../docker-compose.yml)
2. [`src/api/main.py`](../src/api/main.py)
3. [`src/config/mongo_settings.py`](../src/config/mongo_settings.py)
4. the service-layer modules under `src/service/`

## 18. Mental Model For Newcomers

If you remember only one thing, remember this:

The current machine learning pipeline starts with the historical collector.

That means the dependency chain is:

```text
collector healthy
  -> Mongo historical data available
  -> model trainer can run
  -> prediction API can load artifacts
  -> prediction endpoint can respond
```

If the collector or Mongo historical data is broken, the rest of the ML path usually fails downstream.
