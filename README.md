# Cryptocurrency Data Engineering and ML Pipeline

This repository contains an end-to-end cryptocurrency data platform built around Binance market data. It combines batch ingestion, streaming ingestion, MongoDB-backed storage, model training, prediction APIs, and dashboards inside a Docker Compose stack.

## Overview

The project currently supports two complementary data paths:

- **Historical batch path** for the active machine-learning workflow
- **Streaming path** for near-real-time ingestion, inspection, and dashboards

### Historical batch path

```text
Binance REST API
  -> raw CSV files
  -> processed CSV files
  -> MongoDB klines_historical
  -> logistic regression training
  -> prediction API
```

### Streaming path

```text
Binance WebSocket
  -> Redpanda
  -> stream consumer
  -> MongoDB klines_streaming
  -> dashboards and operational inspection
```

## Architecture

![Cryptocurrency pipeline architecture](assets/Binance%20Data%20ML%20Pipeline-2026-01-30-100112.png)

## Main Components

| Component | Purpose | Default access |
|---|---|---|
| `mongodb` | Stores historical and streaming market data | `mongodb://localhost:27017` |
| `mongo-express` | Browser UI for MongoDB | `http://localhost:8082` |
| `redpanda-0` | Streaming broker | `localhost:19092` |
| `console` | Redpanda Console UI | `http://localhost:8080` |
| `binance-collector` | Historical collector and preprocessing job | internal |
| `stream-producer` | Binance WebSocket producer | internal |
| `stream-consumer` | Redpanda to MongoDB consumer | internal |
| `model-trainer` | One-shot logistic regression training job | internal |
| `main` | Main FastAPI app with root, prediction, and Grafana routes | `http://localhost:8000` |
| `prediction-api` | Prediction-focused API with shared model artifacts | `http://localhost:8001` |
| `grafana` | Visualization UI | `http://localhost:3000` |
| `dashboard` | Streamlit dashboard | `http://localhost:8501` |

## How the Project Works

### Historical collection

The batch collector in [`src/service/batch/binance_historical_collector.py`](src/service/batch/binance_historical_collector.py) pulls Binance klines, saves the raw payloads to [`data/raw_data/`](data/raw_data), writes normalized records to [`data/processed_data/`](data/processed_data), and upserts the processed candles into MongoDB collection `klines_historical`.

The collector container is started by [`scripts/collector-entrypoint.sh`](scripts/collector-entrypoint.sh), which performs one sync immediately and then schedules hourly runs through cron. If MongoDB is empty but processed files already exist locally, the collector can backfill MongoDB from those files instead of forcing a full re-download.

### Streaming ingestion

The streaming path sends Binance WebSocket candles through Redpanda and persists them into MongoDB collection `klines_streaming`. This path is useful for live inspection and dashboarding, but it is not the current source used to train the logistic regression model.

### Machine learning and prediction

The active model is a logistic regression pipeline:

- training entrypoint: [`src/service/predict/logistic_regression/train.py`](src/service/predict/logistic_regression/train.py)
- prediction service: [`src/service/predict/logistic_regression/predictor.py`](src/service/predict/logistic_regression/predictor.py)
- HTTP router: [`src/api/routers/predict_logistic_regression.py`](src/api/routers/predict_logistic_regression.py)

Training reads historical candles from MongoDB, rebuilds engineered features, fits a `StandardScaler` and `LogisticRegression`, and writes artifacts into the shared `model_artifacts` volume. The prediction API then loads those artifacts to serve inference.

The current feature set is:

- `log_return`
- `volatility`
- `ma_10`
- `ma_30`
- `momentum`
- `buy_ratio`
- `spread`
- `trade_count`

The target label is based on whether the next candle close is greater than the current candle open.

## Repository Guide

These paths are the most useful when navigating the codebase:

- [`docker-compose.yml`](docker-compose.yml): service definitions and runtime wiring
- [`src/api/main.py`](src/api/main.py): active FastAPI application entrypoint
- [`src/api/routers/`](src/api/routers): HTTP routes
- [`src/service/batch/`](src/service/batch): historical collection and preprocessing
- [`src/service/stream/`](src/service/stream): streaming producer and consumer
- [`src/service/predict/logistic_regression/`](src/service/predict/logistic_regression): training and inference logic
- [`src/config/`](src/config): environment-driven settings
- [`src/database/`](src/database): MongoDB helpers
- [`scripts/`](scripts): operational scripts
- [`grafana/provisioning/`](grafana/provisioning): datasource and dashboard provisioning
- [`data/`](data): raw and processed local data

## Configuration

Copy `.env.example` to `.env` before starting the stack:

```bash
cp .env.example .env
```

The most important environment variables are:

- `BINANCE_SYMBOL`
- `BINANCE_INTERVAL`
- `BINANCE_START_DATE`
- `MONGODB_URI`
- `MONGODB_DATABASE`
- `MONGODB_COLLECTION_HISTORICAL`
- `MONGODB_COLLECTION_STREAMING`
- `MODEL_DIR`
- `MODEL_PATH`
- `SCALER_PATH`

MongoDB configuration is centralized under [`src/config/mongo_settings.py`](src/config/mongo_settings.py). Reuse that shared settings loader instead of hardcoding connection strings inside feature code.

## Quick Start

### 1. Start the Docker stack

```bash
./scripts/docker-up-clean.sh
```

This helper script starts the long-running services, performs safer cleanup, waits for health checks, and can bootstrap prediction artifacts when needed.

### 2. Check service status

```bash
docker compose ps
```

### 3. Confirm historical data exists

```bash
docker exec mongodb mongosh --quiet \
  --username admin \
  --password password \
  --authenticationDatabase admin \
  --eval 'db.getSiblingDB("crypto_data").klines_historical.countDocuments({symbol:"BTCUSDT", interval:"5m"})'
```

### 4. Train or retrain the model manually

```bash
docker compose run --rm model-trainer
docker compose restart prediction-api
```

You can also retrain through the API:

```bash
curl -X POST http://localhost:8001/predict/logistic/admin/retrain \
  -H "Content-Type: application/json" \
  -d '{"symbol":"BTCUSDT","interval":"5m"}'
```

### 5. Check prediction service health

```bash
curl http://localhost:8001/predict/logistic/status/check
```

### 6. Request predictions

```bash
curl "http://localhost:8001/predict/logistic/BTCUSDT?limit=20"
```

## Local URLs

| Service | URL | Notes |
|---|---|---|
| Main API | `http://localhost:8000` | Root endpoint, docs, predictions, Grafana routes |
| Prediction API | `http://localhost:8001` | Shared-artifact inference container |
| FastAPI docs | `http://localhost:8000/docs` | Swagger UI |
| Grafana | `http://localhost:3000` | Login `admin` / `admin` |
| Streamlit dashboard | `http://localhost:8501` | Dashboard UI |
| Mongo Express | `http://localhost:8082` | Login `admin` / `password` |
| Redpanda Console | `http://localhost:8080` | Topic and message inspection |

## API Endpoints

### General

- `GET /`
- `GET /docs`

### Prediction

- `GET /predict/logistic/{symbol}?limit=20`
- `GET /predict/logistic/status/check`
- `POST /predict/logistic/admin/retrain`

Examples:

```bash
curl "http://localhost:8000/predict/logistic/BTCUSDT?limit=20"
curl "http://localhost:8001/predict/logistic/BTCUSDT?limit=20"
```

### Grafana integration

- `GET /grafana/search`
- `POST /grafana/query`
- `GET /grafana/annotations`

Inside the Docker network, Grafana is expected to call:

```text
http://main:8000/grafana
```

## Common Commands

Follow the historical collector:

```bash
docker compose logs -f binance-collector
```

Follow the prediction API:

```bash
docker compose logs -f prediction-api
```

Follow Grafana:

```bash
docker compose logs -f grafana
```

Inspect Grafana metrics:

```bash
curl http://localhost:8000/grafana/search
```

Test a Grafana query:

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

## Troubleshooting

### Prediction returns `No data found`

This usually means MongoDB does not contain historical rows for the requested symbol and interval. Check that:

- the collector ran successfully
- `klines_historical` contains documents
- the requested symbol and interval match the available data

### Prediction API says the model is not loaded

This usually means the model artifacts have not been trained yet, were not written to the shared volume, or the API container needs a restart.

```bash
docker compose run --rm model-trainer
docker compose restart prediction-api
```

## Additional Documentation

- [`GRAFANA_SETUP.md`](assets/GRAFANA_SETUP.md): Grafana setup and verification notes
- [`DASHBOARD_ACCESS.md`](assets/DASHBOARD_ACCESS.md): dashboard access details
- [`tests/README.md`](tests/README.md): test-specific notes
