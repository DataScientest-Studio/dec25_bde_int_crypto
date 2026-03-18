# Project Documentation

## Overview

This project is a end-to-end crypto data engineering and machine learning pipeline built around Binance kline data.

The main flow is:

1. Historical candles are fetched from the Binance REST API.
2. Raw candles are stored under `data/raw_data/`.
3. Validated and normalized candles are stored under `data/processed_data/`.
4. Historical processed candles are upserted into MongoDB.
5. The logistic regression trainer reads historical candles from MongoDB and writes model artifacts.
6. The prediction API reads both historical candles and the saved model artifacts to serve predictions.

## Main Directories

- `src/service/batch/`
  Historical collection pipeline from Binance to files to MongoDB.
- `src/service/predict/logistic_regression/`
  Logistic regression training and prediction service logic.
- `src/api/routers/`
  FastAPI route handlers. These should stay thin and delegate business logic to services.
- `src/database/`
  Shared MongoDB client and repository helpers.
- `src/config/`
  Shared configuration loading from environment variables and `.env`.
- `dockerfiles/`
  Container definitions.
- `scripts/`
  Small operational scripts used by containers, including the collector startup script and cron schedule.
- `data/raw_data/`
  Raw Binance responses persisted as files.
- `data/processed_data/`
  Validated and normalized historical rows used to recover MongoDB state if needed.

## Historical Pipeline

The historical collector is implemented in [binance_historical_collector.py](/Users/aniket/Development/data-engineering/dec25_bde_int_crypto/src/service/batch/binance_historical_collector.py).

Its three phases are:

1. Raw sync
   Missing time gaps are fetched from Binance and merged into `data/raw_data/`.
2. Process sync
   Raw rows are validated and converted into `HistoricalKline` rows, then saved into `data/processed_data/`.
3. Mongo sync
   Processed rows are upserted into the `klines_historical` collection.

Important design note:

- If MongoDB is empty but processed files already exist, the collector backfills MongoDB from `data/processed_data/` first.
- This makes the pipeline resilient after a Mongo reset or volume loss.

## Training Flow

The trainer lives in [train.py](/Users/aniket/Development/data-engineering/dec25_bde_int_crypto/src/service/predict/logistic_regression/train.py).

It does the following:

1. Reads historical candles from MongoDB using the shared Mongo settings/client.
2. Rebuilds the feature set used by the logistic regression model.
3. Splits the data chronologically into train and test sets.
4. Fits a `StandardScaler`.
5. Trains the logistic regression model.
6. Saves:
   `logistic_regression_model.pkl`
   `logistic_regression_scaler.pkl`

Artifacts are written to `/app/models` in Docker and mounted into the prediction API container.

## Prediction Flow

The service-layer prediction logic lives in [predictor.py](/Users/aniket/Development/data-engineering/dec25_bde_int_crypto/src/service/predict/logistic_regression/predictor.py).

Responsibilities of the predictor service:

- load model artifacts
- fetch recent historical candles from MongoDB
- rebuild the same feature transformations used in training
- return scored predictions

The FastAPI router lives in [predict_logistic_regression.py](/Users/aniket/Development/data-engineering/dec25_bde_int_crypto/src/api/routers/predict_logistic_regression.py).

Responsibilities of the router:

- validate HTTP query parameters
- call the predictor service
- convert the scored DataFrame into API response models

This separation is intentional: routing code should not duplicate model loading, Mongo access, or feature engineering.

## MongoDB Collections

Current logical collections:

- `klines_historical`
  Historical candles produced by the batch collector and used by training/prediction.
- `klines_streaming`
  Streaming candles produced by the real-time Kafka/consumer flow.

For the logistic regression path, the important collection is `klines_historical`.

## Environment Variables

Core variables from `.env`:

- `MONGODB_URI`
- `MONGODB_DATABASE`
- `MONGODB_COLLECTION_HISTORICAL`
- `MONGODB_COLLECTION_STREAMING`
- `BINANCE_SYMBOL`
- `BINANCE_INTERVAL`
- `BINANCE_START_DATE`
- `DATA_DIR`
- `RAW_DATA_DIRNAME`
- `PROCESSED_DATA_DIRNAME`

The codebase uses shared settings loaders in `src/config/` so services do not need to hardcode connection details.

## What `collector-entrypoint.sh` Does

The file [collector-entrypoint.sh](/Users/aniket/Development/data-engineering/dec25_bde_int_crypto/scripts/collector-entrypoint.sh) is the startup script for the `binance-collector` container.

Its role is:

1. Copy the collector cron definition into `/etc/cron.d/` inside the container.
2. Run the historical collector immediately when the container starts.
3. Start cron in the foreground.
4. Let cron trigger the historical collector every hour.

Why this exists:

- It ensures MongoDB gets an initial sync immediately instead of waiting for the first schedule.
- It keeps the recurring schedule inside the container explicit and easy to inspect.
- It separates container startup behavior from the Dockerfile itself.

The hourly schedule itself lives in [collector.crontab](/Users/aniket/Development/data-engineering/dec25_bde_int_crypto/scripts/collector.crontab).

## Running the Main Services

Typical services in `docker-compose.yml`:

- `binance-collector`
  Keeps historical files and MongoDB up to date.
- `model-trainer`
  Trains the logistic regression model and writes artifacts.
- `main`
  Main FastAPI service on port `8000`.
- `prediction-api`
  Dedicated prediction service on port `8001`.
- `mongodb`
  Data store for historical and streaming candles.

## Useful Endpoints

- `GET /`
  API root/health info
- `GET /predict/logistic/{symbol}?limit=20`
  Logistic regression prediction endpoint
- `GET /predict/logistic/status/check`
  Runtime status for prediction model loading and Mongo settings

## Maintenance Notes

- If predictions return "No data found", first verify the collector is healthy and MongoDB contains documents in `klines_historical`.
- If `prediction-api` reports missing model artifacts, run the trainer once so `/app/models` is populated.
- If you change the feature list in training, update the predictor in the same commit.
