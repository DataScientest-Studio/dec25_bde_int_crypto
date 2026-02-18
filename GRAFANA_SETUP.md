# Grafana Real-Time Crypto Dashboard Setup

## Access Grafana

1. Open browser to: http://localhost:3000
2. Login with:
   - Username: `admin`
   - Password: `admin`

## Setup Data Source

Since the MongoDB plugin requires Enterprise license, we'll use the API endpoint directly via Infinity plugin or manual queries.

### Option 1: Use Infinity Plugin (Recommended)

1. Go to **Configuration > Plugins**
2. Search for "Infinity" and install it
3. Go to **Configuration > Data Sources > Add data source**
4. Select **Infinity**
5. Configure:
   - Name: `Crypto API`
   - URL: `http://grafana-api:8000`
6. Click **Save & Test**

### Option 2: Use TestData or Manual Entry

The API is running at `http://localhost:8000` with these endpoints:
- `GET /search` - Returns available metrics
- `POST /query` - Returns time series data

Available metrics:
- `btcusdt_close` - Close price
- `btcusdt_open` - Open price
- `btcusdt_high` - High price
- `btcusdt_low` - Low price
- `btcusdt_volume` - Trading volume
- `btcusdt_quote_volume` - Quote volume
- `btcusdt_trade_count` - Number of trades

## Create Dashboard

### Quick Dashboard Import

1. Go to **Dashboards > Import**
2. Upload the file: `grafana/provisioning/dashboards/binance-realtime.json`
3. Select your configured data source
4. Click **Import**

### Manual Dashboard Creation

1. Click **+ > Create Dashboard**
2. Click **Add visualization**
3. Select your data source
4. Configure query to fetch from MongoDB or API
5. For MongoDB direct access:
   ```javascript
   db.getSiblingDB("crypto_data").klines.aggregate([
     {$match: {symbol: "BTCUSDT"}},
     {$sort: {timestamp: -1}},
     {$limit: 100},
     {$sort: {timestamp: 1}},
     {$project: {timestamp: 1, close: 1, _id: 0}}
   ])
   ```

## Verify Data Flow

Check the API is working:
```bash
# Test API health
curl http://localhost:8000/

# Get available metrics
curl http://localhost:8000/search

# Test query (replace timestamps)
curl -X POST http://localhost:8000/query \
  -H "Content-Type: application/json" \
  -d '{
    "targets": [{"target": "btcusdt_close"}],
    "range": {"from": "2026-02-04T00:00:00Z", "to": "2026-02-04T23:59:59Z"}
  }'
```

## MongoDB Direct Access (Alternative)

If you want to query MongoDB directly from Grafana:

1. Install MongoDB plugin (requires downgrading Grafana or using Enterprise)
2. Or use Mongo Express: http://localhost:8082
   - Username: `admin`
   - Password: `password`

## Troubleshooting

### No Data Showing

1. Check if Binance producer is running:
   ```bash
   docker logs grafana-api
   ```

2. Verify MongoDB has data:
   ```bash
   docker exec mongodb mongosh -u admin -p password \
     --authenticationDatabase admin crypto_data \
     --eval "db.klines.countDocuments({})"
   ```

3. Check API logs:
   ```bash
   docker logs grafana-api
   ```

### Dashboard Not Auto-Loading

The dashboard provisioning may not work with all plugin combinations. Manually import:
1. Copy content from `grafana/provisioning/dashboards/binance-realtime.json`
2. In Grafana: **Dashboards > Import > Paste JSON**

## Current System Status

- **Grafana**: http://localhost:3000 (admin/admin)
- **Grafana API**: http://localhost:8000
- **Mongo Express**: http://localhost:8082 (admin/password)
- **Redpanda Console**: http://localhost:8080
- **MongoDB**: localhost:27017

Dataflow:
```
Binance WebSocket → Redpanda → Consumer → MongoDB → Grafana API → Grafana
```
