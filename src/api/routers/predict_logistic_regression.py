import os
import joblib
import pandas as pd
import numpy as np
import logging
from bson import Decimal128
from fastapi import APIRouter, HTTPException, Query
from motor.motor_asyncio import AsyncIOMotorClient
from pydantic import BaseModel
from typing import List

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/predict/logistic",
    tags=["logistic-regression"],
    responses={404: {"description": "Not found"}},
)

# ── Config ────────────────────────────────────────────────────────────────────
MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://admin:password@mongodb-ml:27017/")
MONGODB_DB = os.getenv("MONGODB_DATABASE", "crypto_data")
COLLECTION = os.getenv("MONGODB_COLLECTION_HISTORICAL", "klines_historical")
INTERVAL = os.getenv("BINANCE_INTERVAL", "5m")

# ── Load model & scaler ───────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MODEL_PATH = os.getenv(
    "MODEL_PATH", os.path.join(BASE_DIR, "logistic_regression_model.pkl")
)
SCALER_PATH = os.getenv(
    "SCALER_PATH", os.path.join(BASE_DIR, "logistic_regression_scaler.pkl")
)

try:
    model = joblib.load(MODEL_PATH)
    scaler = joblib.load(SCALER_PATH)
    logger.info("Model & scaler loaded.")
except Exception as e:
    model = None
    scaler = None
    logger.error(f"Failed to load model/scaler: {e}")


# ── Schemas ───────────────────────────────────────────────────────────────────
class PredictionRow(BaseModel):
    timestamp_ms: int
    timestamp_iso: str
    open: float
    close: float
    signal: str
    confidence_pct: float


class LogisticPredictionResponse(BaseModel):
    symbol: str
    interval: str
    latest_signal: str
    latest_confidence: float
    latest_timestamp: str
    predictions: List[PredictionRow]


# ── Constants ─────────────────────────────────────────────────────────────────
FEATURES = [
    "log_return",
    "volatility",
    "ma_10",
    "ma_30",
    "momentum",
    "buy_ratio",
    "spread",
    "trade_count",
]


# ── Helpers ───────────────────────────────────────────────────────────────────
def decimal_to_float(x):
    if isinstance(x, Decimal128):
        return float(x.to_decimal())
    return float(x)


async def fetch_klines(symbol: str, limit: int) -> pd.DataFrame:
    fetch_limit = limit + 50  # extra pour rolling windows

    client = AsyncIOMotorClient(MONGODB_URI)
    try:
        collection = client[MONGODB_DB][COLLECTION]

        # Trouve la dernière bougie disponible
        last_doc = await collection.find_one(
            {"symbol": symbol, "interval": INTERVAL}, sort=[("open_time_ms", -1)]
        )

        if not last_doc:
            return pd.DataFrame()

        last_ts = last_doc["open_time_ms"]
        print(f"[api] Dernière bougie en base : {last_ts}")

        # Fetch les N dernières bougies consécutives depuis cette date
        cursor = (
            collection.find(
                {
                    "symbol": symbol,
                    "interval": INTERVAL,
                    "open_time_ms": {"$lte": last_ts},
                },
                {
                    "_id": 0,
                    "open_time_ms": 1,
                    "open": 1,
                    "high": 1,
                    "low": 1,
                    "close": 1,
                    "volume": 1,
                    "trade_count": 1,
                    "taker_buy_base_volume": 1,
                },
            )
            .sort("open_time_ms", -1)
            .limit(fetch_limit)
        )

        docs = await cursor.to_list(length=None)
        return pd.DataFrame(docs)
    finally:
        client.close()


def run_prediction(df: pd.DataFrame) -> pd.DataFrame:
    decimal_cols = ["open", "high", "low", "close", "volume", "taker_buy_base_volume"]
    for col in decimal_cols:
        df[col] = df[col].apply(decimal_to_float)

    df["trade_count"] = pd.to_numeric(df["trade_count"], errors="coerce")
    df = df.sort_values("open_time_ms").reset_index(drop=True)

    df["return"] = df["close"].pct_change()
    df["log_return"] = np.log(df["close"] / df["close"].shift(1))
    df["volatility"] = df["return"].rolling(12).std()
    df["ma_10"] = df["close"].rolling(10).mean()
    df["ma_30"] = df["close"].rolling(30).mean()
    df["momentum"] = df["close"] - df["close"].shift(10)
    df["buy_ratio"] = df["taker_buy_base_volume"] / df["volume"]
    df["spread"] = df["high"] - df["low"]
    df = df.dropna()

    X_scaled = scaler.transform(df[FEATURES])
    df["prediction"] = model.predict(X_scaled)
    df["probability_up"] = model.predict_proba(X_scaled)[:, 1]
    df["signal"] = df["prediction"].map({0: "DOWN ⬇", 1: "UP ⬆"})
    df["confidence_%"] = (df["probability_up"] * 100).round(2)

    # timestamp en ms et ISO
    df["timestamp_ms"] = df["open_time_ms"].astype(int)
    df["timestamp_iso"] = pd.to_datetime(df["open_time_ms"], unit="ms").dt.strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )

    return df


# ── Endpoints ─────────────────────────────────────────────────────────────────
@router.get("/{symbol}", response_model=LogisticPredictionResponse)
async def predict(
    symbol: str,
    limit: int = Query(default=20, ge=5, le=500, description="Nombre de bougies"),
):
    """
    Fetch les dernières bougies depuis MongoDB et retourne les prédictions avec timestamps.
    """
    if model is None or scaler is None:
        raise HTTPException(status_code=503, detail="Model not loaded.")

    df = await fetch_klines(symbol.upper(), limit)

    if df.empty:
        raise HTTPException(
            status_code=404, detail=f"No data found for {symbol} {INTERVAL} in MongoDB."
        )

    try:
        df = run_prediction(df)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Prediction error: {e}")

    last = df.iloc[-1]
    last_n = df.tail(limit)

    return LogisticPredictionResponse(
        symbol=symbol.upper(),
        interval=INTERVAL,
        latest_signal=last["signal"],
        latest_confidence=last["confidence_%"],
        latest_timestamp=last["timestamp_iso"],
        predictions=[
            PredictionRow(
                timestamp_ms=int(row["timestamp_ms"]),
                timestamp_iso=row["timestamp_iso"],
                open=round(row["open"], 2),
                close=round(row["close"], 2),
                signal=row["signal"],
                confidence_pct=row["confidence_%"],
            )
            for _, row in last_n.iterrows()
        ],
    )


@router.get("/status/check")
async def model_status():
    return {
        "model_loaded": model is not None,
        "scaler_loaded": scaler is not None,
        "features": FEATURES,
        "n_features_expected": model.n_features_in_ if model else None,
        "mongodb": MONGODB_URI,
        "collection": COLLECTION,
        "interval": INTERVAL,
    }
