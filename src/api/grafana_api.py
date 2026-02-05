"""
FastAPI server to provide Grafana-compatible JSON API for MongoDB data.

This API serves kline data from MongoDB in a format that Grafana can consume
using the Infinity datasource plugin.
"""

import logging
import os
from datetime import datetime, timezone
from typing import List, Optional

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from pymongo import MongoClient

# Get configuration from environment variables
MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://admin:password@mongodb:27017/")
MONGODB_DATABASE = os.getenv("MONGODB_DATABASE", "crypto_data")
MONGODB_COLLECTION = os.getenv("MONGODB_COLLECTION", "klines")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(title="Grafana MongoDB API", version="1.0.0")

# Enable CORS for Grafana
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# MongoDB connection
logger.info(f"Connecting to MongoDB: {MONGODB_URI}")
mongo_client = MongoClient(MONGODB_URI)
db = mongo_client[MONGODB_DATABASE]
collection = db[MONGODB_COLLECTION]


@app.get("/")
def root():
    """Health check endpoint."""
    return {"status": "ok", "message": "Grafana MongoDB API", "mongodb_uri": MONGODB_URI}


@app.get("/search")
def search():
    """
    Return available metrics for Grafana to query.

    This endpoint is called by Grafana to get list of available metrics.
    """
    return [
        "btcusdt_close",
        "btcusdt_open",
        "btcusdt_high",
        "btcusdt_low",
        "btcusdt_volume",
        "btcusdt_quote_volume",
        "btcusdt_trade_count"
    ]


@app.post("/query")
async def query(request: Request):
    """
    Query endpoint for Grafana.

    Grafana sends POST requests with target metrics and time range.
    """
    try:
        payload = await request.json()
    except:
        payload = {}

    logger.info(f"Received payload: {payload}")

    targets = payload.get("targets", [])
    range_data = payload.get("range", {})
    max_data_points = payload.get("maxDataPoints", 1000)

    logger.info(f"Query request: targets={targets}, range={range_data}, max_points={max_data_points}")

    results = []

    for target in targets:
        target_name = target.get("target", "")

        # Build MongoDB query - query ALL data, not just closed candles
        query_filter = {
            "symbol": "BTCUSDT"
        }

        # Add time range filter if provided
        if range_data:
            from_time = range_data.get("from")
            to_time = range_data.get("to")

            if from_time and to_time:
                # Parse ISO format timestamps
                try:
                    from_dt = datetime.fromisoformat(from_time.replace('Z', '+00:00'))
                    to_dt = datetime.fromisoformat(to_time.replace('Z', '+00:00'))
                    query_filter["timestamp"] = {"$gte": from_dt, "$lte": to_dt}
                except Exception as e:
                    logger.error(f"Error parsing timestamps: {e}")

        # Determine which field to query
        field_map = {
            "btcusdt_close": "close",
            "btcusdt_open": "open",
            "btcusdt_high": "high",
            "btcusdt_low": "low",
            "btcusdt_volume": "volume",
            "btcusdt_quote_volume": "quote_volume",
            "btcusdt_trade_count": "trade_count"
        }

        field = field_map.get(target_name, "close")

        # Query MongoDB
        docs = list(collection.find(
            query_filter,
            {"timestamp": 1, field: 1, "_id": 0}
        ).sort("timestamp", 1).limit(max_data_points))

        logger.info(f"Found {len(docs)} documents for {target_name}")

        # Convert to Grafana format: [[value, timestamp_ms], ...]
        datapoints = []
        for doc in docs:
            if "timestamp" in doc and field in doc:
                ts = doc["timestamp"]
                # Convert datetime to Unix timestamp in milliseconds
                if isinstance(ts, datetime):
                    timestamp_ms = int(ts.timestamp() * 1000)
                else:
                    timestamp_ms = int(ts)

                value = doc[field]
                datapoints.append([value, timestamp_ms])

        logger.info(f"Returning {len(datapoints)} datapoints for {target_name}")

        results.append({
            "target": target_name,
            "datapoints": datapoints
        })

    return results


@app.get("/annotations")
def annotations():
    """
    Annotations endpoint for Grafana.

    Can be used to show events/markers on the chart.
    """
    return []


def main():
    """Run the API server."""
    import uvicorn
    logger.info(f"Starting Grafana API server on http://0.0.0.0:8000")
    logger.info(f"MongoDB URI: {MONGODB_URI}")
    logger.info(f"MongoDB Database: {MONGODB_DATABASE}")
    logger.info(f"MongoDB Collection: {MONGODB_COLLECTION}")
    uvicorn.run(app, host="0.0.0.0", port=8000)


if __name__ == "__main__":
    main()
