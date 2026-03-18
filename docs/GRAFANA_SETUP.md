# Grafana Setup Guide

## Intended Architecture

The intended dashboard path is:

`Grafana -> FastAPI Grafana router -> MongoDB historical data`

In this project, Grafana is a visualization layer. It is not the source of truth for market data.

The source of truth for historical analysis is:

`Binance historical fetch -> raw CSV -> processed CSV -> MongoDB klines_historical`

## Services Involved

- `grafana`
  Dashboard UI on port `3000`
- `main`
  FastAPI app on port `8000`
- `mongodb`
  Historical data store

## How To Access Grafana

Use the following local access details when the Docker stack is running:

- URL: `http://localhost:3000`
- username: `admin`
- password: `admin`

The provisioned dashboard file is:

- [`grafana/provisioning/dashboards/crypto-realtime.json`](../grafana/provisioning/dashboards/crypto-realtime.json)

The dashboard title in that file is:

- `Binance Real-Time Crypto Dashboard`

The dashboard UID is:

- `binance-crypto-realtime`

In practice, the easiest way to access it is:

1. Open Grafana at `http://localhost:3000`.
2. Sign in with `admin` / `admin`.
3. Open the Dashboards view.
4. Look for `Binance Real-Time Crypto Dashboard`.

If provisioning is working correctly, that dashboard should already be available without manual import.

## Relevant Files

- [`src/api/routers/grafana.py`](../src/api/routers/grafana.py)
  Current FastAPI router used by Grafana.
- [`src/api/main.py`](../src/api/main.py)
  Active FastAPI app entrypoint used by Docker Compose.
- [`grafana/provisioning/datasources/crypto-api.yml`](../grafana/provisioning/datasources/crypto-api.yml)
  Infinity datasource provisioning.
- [`grafana/provisioning/dashboards/dashboard.yml`](../grafana/provisioning/dashboards/dashboard.yml)
  Dashboard provider registration.
- [`grafana/provisioning/dashboards/crypto-realtime.json`](../grafana/provisioning/dashboards/crypto-realtime.json)
  Provisioned dashboard definition.

## Correct Base URL For The Current Stack

Inside the Docker network, Grafana should call the `main` service, not `localhost`.

The current FastAPI route prefix is `/grafana`.

So the correct internal base URL is:

```text
http://main:8000/grafana
```

## Grafana Routes Exposed By FastAPI

The router currently exposes:

- `GET /grafana/search`
- `POST /grafana/query`
- `GET /grafana/annotations`

## Current Known Mismatches

This is the most important section in this document.

The repository still has a few Grafana-related inconsistencies:

1. [`crypto-api.yml`](../grafana/provisioning/datasources/crypto-api.yml) still points at `http://grafana-api:8000`.
2. [`crypto-realtime.json`](../grafana/provisioning/dashboards/crypto-realtime.json) still uses root paths such as `/query` instead of `/grafana/query`.
3. [`src/api/routers/grafana.py`](../src/api/routers/grafana.py) currently queries a `timestamp` field, but historical collector documents use `open_time_ms` and `close_time_ms`.

Because of those mismatches, it is possible for Grafana to load successfully while still showing empty panels.

## Recommended Datasource Pattern

For this repository, the cleanest pattern is:

- Grafana Infinity datasource
- HTTP calls to the FastAPI Grafana router
- FastAPI reading historical data from MongoDB

That keeps the dashboard layer separate from the storage layer.

## Operator Runbook

Use this runbook when you want to bring up Grafana and verify the dashboard path end to end.

### 1. Start the required services

```bash
docker compose up -d mongodb main grafana binance-collector
```

### 2. Confirm the core services are running

```bash
docker compose ps
```

At minimum, you want these services up:

- `mongodb`
- `main`
- `grafana`
- `binance-collector`

### 3. Open Grafana

- open `http://localhost:3000`
- sign in with `admin` / `admin`
- open `Binance Real-Time Crypto Dashboard`

### 4. Confirm the API side is reachable

```bash
curl http://localhost:8000/grafana/search
```

### 5. Confirm historical data exists

```bash
docker exec mongodb mongosh --quiet \
  --username admin \
  --password password \
  --authenticationDatabase admin \
  --eval 'db.getSiblingDB("crypto_data").klines_historical.countDocuments({symbol:"BTCUSDT", interval:"5m"})'
```

### 6. Inspect logs if the dashboard is empty

```bash
docker compose logs -f grafana
docker compose logs -f main
docker compose logs -f binance-collector
```

## Manual Validation

Before debugging Grafana itself, validate the API endpoints directly.

Search endpoint:

```bash
curl http://localhost:8000/grafana/search
```

Query endpoint:

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

## Metrics Exposed Today

The router currently advertises these targets:

- `btcusdt_close`
- `btcusdt_open`
- `btcusdt_high`
- `btcusdt_low`
- `btcusdt_volume`
- `btcusdt_quote_volume`
- `btcusdt_trade_count`

## Practical Troubleshooting

### Grafana is up but has no data

Check:

1. `main` is running.
2. `mongodb` contains documents in `klines_historical`.
3. The datasource points to `main:8000/grafana`.
4. The dashboard JSON is using `/grafana/query`, not `/query`.
5. The router query logic matches the actual historical document schema.

### The API route works but the dashboard still fails

That usually means the Grafana provisioning files are stale rather than the API being down.

### MongoDB has data but charts are still blank

That is expected if the router is filtering on `timestamp` while the stored historical schema uses `open_time_ms`.

### The dashboard is missing from the Grafana UI

Check:

1. Grafana started with the mounted provisioning directory.
2. [`dashboard.yml`](../grafana/provisioning/dashboards/dashboard.yml) is still pointing at `/etc/grafana/provisioning/dashboards`.
3. [`crypto-realtime.json`](../grafana/provisioning/dashboards/crypto-realtime.json) is present inside the mounted folder.
4. Grafana logs do not show dashboard provisioning errors.

## Recommendation

Treat Grafana in this repository as partially wired:

- the service is present
- the dashboard assets are present
- the provisioning intent is clear
- the last alignment step between provisioning, route prefix, and Mongo schema still needs cleanup

Documenting that limitation is better than implying the dashboard is fully correct when it is not.
