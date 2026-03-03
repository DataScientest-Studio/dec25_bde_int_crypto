"""
Grafana router for time-series data visualization.

This module provides Grafana-compatible JSON API endpoints for MongoDB data.
Compatible with Grafana Infinity datasource plugin.
"""

import logging
from datetime import datetime

from fastapi import APIRouter, Request
from pymongo import MongoClient

from src.config.mongo_settings import get_settings

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/grafana",
    tags=["grafana"],
    responses={404: {"description": "Not found"}},
)

def get_collection():
    """Lazy-load MongoDB collection to avoid import-time initialization."""
    mongo_settings = get_settings()
    mongo_client = MongoClient(mongo_settings.mongodb_uri)
    db = mongo_client[mongo_settings.mongodb_database]
    return db[mongo_settings.mongodb_collection_historical]


@router.get("/search")
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
        "btcusdt_trade_count",
    ]


@router.post("/query")
async def query(request: Request):
    """
    Query endpoint for Grafana.

    Grafana sends POST requests with target metrics and time range.
    """
    try:
        payload = await request.json()
    except Exception:
        payload = {}

    logger.info(f"Received payload: {payload}")

    targets = payload.get("targets", [])
    range_data = payload.get("range", {})
    max_data_points = payload.get("maxDataPoints", 1000)

    logger.info(
        f"Query request: targets={targets}, range={range_data}, max_points={max_data_points}"
    )

    results = []

    for target in targets:
        target_name = target.get("target", "")

        # Build MongoDB query - query ALL data, not just closed candles
        query_filter = {"symbol": "BTCUSDT"}

        # Add time range filter if provided
        if range_data:
            from_time = range_data.get("from")
            to_time = range_data.get("to")

            if from_time and to_time:
                # Parse ISO format timestamps
                try:
                    from_dt = datetime.fromisoformat(from_time.replace("Z", "+00:00"))
                    to_dt = datetime.fromisoformat(to_time.replace("Z", "+00:00"))
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
            "btcusdt_trade_count": "trade_count",
        }

        field = field_map.get(target_name, "close")

        # Query MongoDB
        collection = get_collection()
        docs = list(
            collection.find(query_filter, {"timestamp": 1, field: 1, "_id": 0})
            .sort("timestamp", 1)
            .limit(max_data_points)
        )

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

        results.append({"target": target_name, "datapoints": datapoints})

    return results


@router.get("/annotations")
def annotations():
    """
    Annotations endpoint for Grafana.

    Can be used to show events/markers on the chart.
    """
    return []
