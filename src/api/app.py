
import os
from datetime import datetime
from typing import Optional

from fastapi import FastAPI, Query
from pydantic import BaseModel
from pymongo import MongoClient


# MongoDB connection
MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://admin:password@localhost:27017/")
MONGODB_DATABASE = os.getenv("MONGODB_DATABASE", "crypto_data")
MONGODB_COLLECTION = os.getenv("MONGODB_COLLECTION", "klines")

client = MongoClient(MONGODB_URI)
db = client[MONGODB_DATABASE]
collection = db[MONGODB_COLLECTION]

app = FastAPI()


# Health Endpoint
@app.get("/health")
def health():
    return {"status": "ok"}


# Stats Endpoint
class StatsResponse(BaseModel):
    symbol: str
    total_records: int
    first_timestamp: Optional[datetime]
    last_timestamp: Optional[datetime]

@app.get("/stats", response_model=StatsResponse)
def stats(symbol: str = "BTCUSDT"):
    query_filter = {"symbol": symbol}

    total = collection.count_documents(query_filter)
    first = collection.find_one(query_filter, sort=[("timestamp", 1)])
    last = collection.find_one(query_filter, sort=[("timestamp", -1)])

    return StatsResponse(
        symbol=symbol,
        total_records=total,
        first_timestamp=first["timestamp"] if first else None,
        last_timestamp=last["timestamp"] if last else None,
    )


# Charts Endpoint
@app.get("/charts")
def charts(
    symbol: str = "BTCUSDT",
    field: str = "close",
    limit: int = Query(50, le=5000)
):
    query_filter = {"symbol": symbol}

    docs = list(
        collection.find(
            query_filter,
            {"timestamp": 1, field: 1, "_id": 0}
        )
        .sort([("timestamp", -1)], -1)
        .limit(limit)
    )

    docs.reverse() 
    datapoints = [
        [doc[field], int(doc["timestamp"].timestamp() * 1000)]
        for doc in docs if field in doc
    ]

    return [
        {
            "target": f"{symbol}_{field}",
            "datapoints": datapoints
        }
    ]



# Predict Endpoint
@app.get("/predict")
def predict(symbol: str = "BTCUSDT"):
    docs = list(
        collection.find(
            {"symbol": symbol},
            {"close": 1, "_id": 0}
        )
        .sort([("timestamp", -1)], -1)
        .limit(1)
    )

    if not docs:
        return {"symbol": symbol, "prediction": None}

    last_close = docs[0]["close"]
    prediction = last_close * 1.005
    return {"symbol": symbol, "prediction": round(prediction, 2)}