# Dashboard And Service Access

This file is the quick operational reference for local development.

## Main URLs

When the Docker stack is running, these are the primary entrypoints:

| Service | URL | Notes |
|---|---|---|
| Main API | `http://localhost:8000` | Root API, prediction routes, and Grafana routes |
| Prediction API | `http://localhost:8001` | Separate container using the shared model artifacts volume |
| Grafana | `http://localhost:3000` | Default login: `admin` / `admin` |
| Mongo Express | `http://localhost:8082` | Default login: `admin` / `password` |
| Redpanda Console | `http://localhost:8080` | Streaming inspection UI |
| MongoDB | `mongodb://localhost:27017` | Dev credentials from `docker-compose.yml` |

## API Endpoints You Will Actually Use

### General API

- `GET /`
  Basic service health and endpoint summary.
- `GET /docs`
  Swagger UI for the active FastAPI app.

### Prediction

- `GET /predict/logistic/{symbol}?limit=20`
  Returns the latest logistic regression predictions for the historical series.
- `GET /predict/logistic/status/check`
  Shows whether the model and scaler artifacts are currently loaded.

Example:

```bash
curl "http://localhost:8000/predict/logistic/BTCUSDT?limit=20"
curl "http://localhost:8001/predict/logistic/BTCUSDT?limit=20"
```

### Grafana Support Routes

- `GET /grafana/search`
- `POST /grafana/query`
- `GET /grafana/annotations`

These routes exist on the `main` API container and are intended to be used by Grafana.

## Operational Checks

If something looks wrong, check the stack in this order.

### Prediction returns `No data found`

This usually means `klines_historical` is empty for the requested symbol and interval.

Check MongoDB:

```bash
docker exec mongodb mongosh --quiet \
  --username admin \
  --password password \
  --authenticationDatabase admin \
  --eval 'db.getSiblingDB("crypto_data").klines_historical.countDocuments({symbol:"BTCUSDT", interval:"5m"})'
```

If the count is `0`, inspect the collector:

```bash
docker compose logs -f binance-collector
```

### Prediction API says the model is not loaded

Train the model artifacts, then restart the prediction container:

```bash
docker compose run --rm model-trainer
docker compose restart prediction-api
```

### Grafana opens but panels are empty

Check these items:

1. `main` is reachable on port `8000`.
2. `mongodb` is healthy and contains historical rows.
3. `GET /grafana/search` works.
4. The Grafana datasource is pointing at the current API service and route prefix.

Important note:

- The repository still contains Grafana provisioning files that refer to older service names and endpoint paths.
- The current Grafana router also expects a `timestamp` field that does not match the historical collector schema.

So an empty panel may be caused by a real configuration mismatch, not only by missing data.

## Recommended End-To-End Check

To validate the full batch-to-ML path:

1. Confirm `binance-collector` is healthy.
2. Confirm MongoDB has documents in `klines_historical`.
3. Run `model-trainer`.
4. Check `/predict/logistic/status/check`.
5. Call the prediction endpoint.

## Which API Should You Use?

Use `main` on port `8000` when you want the general project API.

Use `prediction-api` on port `8001` when you specifically want the prediction service backed by the shared `/app/models` volume after training.

Both containers run the same FastAPI application entrypoint. The difference is operational wiring, not separate application code.
