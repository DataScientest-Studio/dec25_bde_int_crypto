"""
Grafana router for time-series data visualization.

This module provides Grafana-compatible JSON API endpoints for MongoDB data.
Compatible with Grafana Infinity datasource plugin.
"""

import logging
import os
from datetime import datetime, timezone

from fastapi import APIRouter, Request
from bson import Decimal128
from pymongo import MongoClient

from src.config.mongo_settings import get_settings

logger = logging.getLogger(__name__)
INTERVAL = os.getenv("BINANCE_INTERVAL", "5m").strip()

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
    return mongo_client, db[mongo_settings.mongodb_collection_historical]


def _to_epoch_ms(iso_timestamp: str) -> int:
    """Convert an ISO timestamp into UTC epoch milliseconds."""
    dt = datetime.fromisoformat(iso_timestamp.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return int(dt.timestamp() * 1000)


def _to_number(value):
    """Normalize Mongo numeric values into JSON-serializable numbers."""
    if isinstance(value, Decimal128):
        return float(value.to_decimal())
    return float(value)


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

    field_map = {
        "btcusdt_close": "close",
        "btcusdt_open": "open",
        "btcusdt_high": "high",
        "btcusdt_low": "low",
        "btcusdt_volume": "volume",
        "btcusdt_quote_volume": "quote_volume",
        "btcusdt_trade_count": "trade_count",
    }

    mongo_client, collection = get_collection()
    try:
        results = []

        for target in targets:
            target_name = target.get("target", "")
            field = field_map.get(target_name, "close")

            # Historical data is stored by candle open time in milliseconds.
            query_filter = {"symbol": "BTCUSDT", "interval": INTERVAL}

            if range_data:
                from_time = range_data.get("from")
                to_time = range_data.get("to")

                if from_time and to_time:
                    try:
                        query_filter["open_time_ms"] = {
                            "$gte": _to_epoch_ms(from_time),
                            "$lte": _to_epoch_ms(to_time),
                        }
                    except Exception as exc:
                        logger.error(f"Error parsing timestamps: {exc}")

            docs = list(
                collection.find(query_filter, {"open_time_ms": 1, field: 1, "_id": 0})
                .sort("open_time_ms", -1)
                .limit(max_data_points)
            )
            docs.reverse()

            logger.info(f"Found {len(docs)} documents for {target_name}")

            datapoints = []
            for doc in docs:
                if "open_time_ms" not in doc or field not in doc:
                    continue

                try:
                    datapoints.append(
                        [_to_number(doc[field]), int(doc["open_time_ms"])]
                    )
                except Exception as exc:
                    logger.warning(
                        "Skipping Grafana datapoint for %s due to conversion error: %s",
                        target_name,
                        exc,
                    )

            logger.info(f"Returning {len(datapoints)} datapoints for {target_name}")
            results.append({"target": target_name, "datapoints": datapoints})

        return results
    finally:
        mongo_client.close()


@router.get("/annotations")
def annotations():
    """
    Annotations endpoint for Grafana.

    Can be used to show events/markers on the chart.
    """
    return []
