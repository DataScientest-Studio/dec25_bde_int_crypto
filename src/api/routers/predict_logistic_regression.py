# src/routers/logistic_regression.py

import joblib
import pandas as pd
import numpy as np
import logging
from fastapi import APIRouter, UploadFile, File, HTTPException
from pydantic import BaseModel
from typing import List
import io
import os


logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/predict/logistic",
    tags=["logistic-regression"],
    responses={404: {"description": "Not found"}},
)

# ── Load model & scaler once at startup ──────────────────────────────────────
BASE_DIR    = os.path.dirname(os.path.abspath(__file__))
MODEL_PATH  = os.getenv("MODEL_PATH",  os.path.join(BASE_DIR, "logistic_regression_model.pkl"))
SCALER_PATH = os.getenv("SCALER_PATH", os.path.join(BASE_DIR, "logistic_regression_scaler.pkl"))

try:
    model  = joblib.load(MODEL_PATH)
    scaler = joblib.load(SCALER_PATH)
    logger.info("Logistic regression model & scaler loaded.")
except Exception as e:
    model  = None
    scaler = None
    logger.error(f"Failed to load model/scaler: {e}")

# ── Response schema ───────────────────────────────────────────────────────────
class PredictionRow(BaseModel):
    signal:       str
    confidence_pct: float

class LogisticPredictionResponse(BaseModel):
    latest_signal:      str
    latest_confidence:  float
    last_20_predictions: List[PredictionRow]

# ── Helper ────────────────────────────────────────────────────────────────────
FEATURES = [
    "log_return", "volatility", "ma_10", "ma_30",
    "momentum", "buy_ratio", "spread", "trade_count"
]

def run_prediction(df: pd.DataFrame) -> pd.DataFrame:
    df["return"]     = df["close"].pct_change()
    df["log_return"] = np.log(df["close"] / df["close"].shift(1))
    df["volatility"] = df["return"].rolling(12).std()
    df["ma_10"]      = df["close"].rolling(10).mean()
    df["ma_30"]      = df["close"].rolling(30).mean()
    df["momentum"]   = df["close"] - df["close"].shift(10)
    df["buy_ratio"]  = df["taker_buy_base_volume"] / df["volume"]
    df["spread"]     = df["high"] - df["low"]
    df = df.dropna()

    X = df[FEATURES]
    X_scaled = scaler.transform(X)

    df["prediction"]    = model.predict(X_scaled)
    df["probability_up"] = model.predict_proba(X_scaled)[:, 1]
    df["signal"]        = df["prediction"].map({0: "DOWN ⬇", 1: "UP ⬆"})
    df["confidence_%"]  = (df["probability_up"] * 100).round(2)

    return df

# ── Endpoints ─────────────────────────────────────────────────────────────────

@router.post("/", response_model=LogisticPredictionResponse)
async def predict_from_csv(file: UploadFile = File(...)):
    """
    Upload a CSV file and get Bitcoin signal predictions.

    The CSV must contain: close, high, low, volume,
    taker_buy_base_volume, trade_count
    """
    if model is None or scaler is None:
        raise HTTPException(status_code=503, detail="Model not loaded.")

    contents = await file.read()
    try:
        df = pd.read_csv(io.StringIO(contents.decode("utf-8")))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid CSV: {e}")

    missing = [c for c in ["close","high","low","volume","taker_buy_base_volume","trade_count"] if c not in df.columns]
    if missing:
        raise HTTPException(status_code=422, detail=f"Missing columns: {missing}")

    try:
        df = run_prediction(df)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Prediction error: {e}")

    last     = df.iloc[-1]
    last_20  = df[["signal", "confidence_%"]].tail(20)

    return LogisticPredictionResponse(
        latest_signal=last["signal"],
        latest_confidence=last["confidence_%"],
        last_20_predictions=[
            PredictionRow(signal=row["signal"], confidence_pct=row["confidence_%"])
            for _, row in last_20.iterrows()
        ],
    )


@router.get("/status")
async def model_status():
    """Check if the model and scaler are loaded correctly."""
    return {
        "model_loaded":  model is not None,
        "scaler_loaded": scaler is not None,
        "features":      FEATURES,
        "n_features_expected": model.n_features_in_ if model else None,
    }