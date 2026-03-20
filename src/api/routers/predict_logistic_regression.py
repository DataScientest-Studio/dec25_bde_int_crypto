"""Thin FastAPI router for logistic regression predictions.

The router only handles request/response concerns. All model loading,
MongoDB access, and feature engineering live in the service layer.
"""

import asyncio
from typing import List

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from src.config.mongo_settings import get_settings as get_mongo_settings
from src.models.models import SUPPORTED_INTERVALS
from src.service.predict.logistic_regression.predictor import (
    FEATURES,
    INTERVAL,
    MODEL_PATH,
    SCALER_PATH,
    predictor,
)
from src.service.predict.logistic_regression.train import run_training_pipeline

router = APIRouter(
    prefix="/predict/logistic",
    tags=["logistic-regression"],
    responses={404: {"description": "Not found"}},
)

_retrain_lock = asyncio.Lock()


# These response models are API-only. They intentionally stay outside the
# service layer so HTTP formatting concerns do not leak into business logic.
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


class RetrainRequest(BaseModel):
    symbol: str = "BTCUSDT"
    interval: str = INTERVAL


class RetrainResponse(BaseModel):
    status: str
    symbol: str
    interval: str
    rows_fetched: int
    rows_used_for_training: int
    accuracy: float
    model_path: str
    scaler_path: str


@router.get("/{symbol}", response_model=LogisticPredictionResponse)
async def predict(
    symbol: str,
    limit: int = Query(default=20, ge=5, le=500, description="Number of candles"),
):
    """HTTP wrapper around the logistic regression prediction service."""
    if not predictor.is_ready:
        raise HTTPException(status_code=503, detail="Model not loaded.")

    # The service returns a pandas DataFrame; the router reshapes it into a
    # stable API response contract.
    df = await predictor.fetch_klines(symbol.upper(), limit)
    if df.empty:
        raise HTTPException(
            status_code=404, detail=f"No data found for {symbol} {INTERVAL} in MongoDB."
        )

    try:
        predicted_df = predictor.run_prediction(df)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Prediction error: {exc}") from exc

    last = predicted_df.iloc[-1]
    last_n = predicted_df.tail(limit)

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


@router.post("/admin/retrain", response_model=RetrainResponse)
async def retrain_model(request: RetrainRequest):
    """Train fresh artifacts from MongoDB and hot-reload them into the API."""
    symbol = request.symbol.strip().upper()
    interval = request.interval.strip()

    if interval not in SUPPORTED_INTERVALS:
        raise HTTPException(
            status_code=400,
            detail=f"Interval must be one of {sorted(SUPPORTED_INTERVALS)}.",
        )

    async with _retrain_lock:
        try:
            result = await run_training_pipeline(symbol=symbol, interval=interval)
            predictor.reload_artifacts()
        except ValueError as exc:
            raise HTTPException(
                status_code=400, detail=f"Training error: {exc}"
            ) from exc
        except Exception as exc:
            raise HTTPException(
                status_code=500, detail=f"Training error: {exc}"
            ) from exc

    if not predictor.is_ready:
        raise HTTPException(
            status_code=500,
            detail="Retraining finished but model artifacts could not be reloaded.",
        )

    return RetrainResponse(
        status="trained",
        symbol=result.symbol,
        interval=result.interval,
        rows_fetched=result.rows_fetched,
        rows_used_for_training=result.rows_used_for_training,
        accuracy=result.accuracy,
        model_path=str(result.model_path),
        scaler_path=str(result.scaler_path),
    )


@router.get("/status/check")
async def model_status():
    """Expose lightweight runtime status for debugging the prediction service."""
    settings = get_mongo_settings()
    return {
        "model_loaded": predictor.model is not None,
        "scaler_loaded": predictor.scaler is not None,
        "features": FEATURES,
        "n_features_expected": predictor.expected_feature_count,
        "mongodb_database": settings.mongodb_database,
        "collection": settings.mongodb_collection_historical,
        "interval": INTERVAL,
        "model_path": str(MODEL_PATH),
        "scaler_path": str(SCALER_PATH),
    }
