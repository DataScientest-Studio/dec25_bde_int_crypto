"""Service-layer prediction helpers for the logistic regression model.

This module owns everything that is not HTTP-specific:
- loading model artifacts
- reading historical features from MongoDB
- rebuilding the same features used during training
- returning a scored pandas DataFrame
"""

import logging
import os
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd
from bson import Decimal128

from src.config.mongo_settings import get_settings as get_mongo_settings
from src.database.mongo_client import MongoClient as AppMongoClient

logger = logging.getLogger(__name__)

# Keep inference aligned with the same interval and features used at training time.
INTERVAL = os.getenv("BINANCE_INTERVAL", "5m").strip()

# This list is shared conceptually with training. If features change here,
# `train.py` should be updated in the same commit.
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

DECIMAL_COLUMNS = [
    "open",
    "high",
    "low",
    "close",
    "volume",
    "taker_buy_base_volume",
]

BASE_DIR = Path(__file__).resolve().parent
# Fallback paths make local, non-container testing possible. In Docker, these are
# overridden by MODEL_PATH/SCALER_PATH and point to the shared `/app/models` volume.
MODEL_PATH = Path(
    os.getenv("MODEL_PATH", str(BASE_DIR / "logistic_regression_model.pkl"))
)
SCALER_PATH = Path(
    os.getenv("SCALER_PATH", str(BASE_DIR / "logistic_regression_scaler.pkl"))
)
CSV_PATH = Path(os.getenv("CSV_PATH", str(BASE_DIR / "data.csv")))


def decimal_to_float(value: Any) -> float:
    """Normalize Mongo Decimal128 values into floats for pandas/scikit-learn."""
    if isinstance(value, Decimal128):
        return float(value.to_decimal())
    return float(value)


class LogisticRegressionPredictor:
    """Service-layer helper for loading artifacts, fetching data, and scoring rows."""

    def __init__(self, model_path: Path = MODEL_PATH, scaler_path: Path = SCALER_PATH):
        self.model_path = model_path
        self.scaler_path = scaler_path
        self.model = None
        self.scaler = None
        self.reload_artifacts()

    def reload_artifacts(self) -> None:
        """Load the trained model once and keep the router thin."""
        try:
            self.model = joblib.load(self.model_path)
            self.scaler = joblib.load(self.scaler_path)
            logger.info("Model and scaler loaded successfully.")
        except Exception as exc:
            self.model = None
            self.scaler = None
            logger.error(f"Failed to load model/scaler: {exc}")

    @property
    def is_ready(self) -> bool:
        return self.model is not None and self.scaler is not None

    @property
    def expected_feature_count(self) -> int | None:
        if self.model is None:
            return None
        return getattr(self.model, "n_features_in_", None)

    async def fetch_klines(self, symbol: str, limit: int) -> pd.DataFrame:
        """Fetch the latest historical candles needed for inference."""
        # Rolling features need extra lookback rows before we can safely keep the
        # last `limit` predictions requested by the API caller.
        fetch_limit = limit + 50
        settings = get_mongo_settings()
        client = AppMongoClient(
            uri=settings.mongodb_uri,
            database=settings.mongodb_database,
            collection=settings.mongodb_collection_historical,
        )

        try:
            await client.initialize()
            collection = client.get_collection()

            last_doc = await collection.find_one(
                {"symbol": symbol, "interval": INTERVAL}, sort=[("open_time_ms", -1)]
            )
            if not last_doc:
                return pd.DataFrame()

            last_ts = last_doc["open_time_ms"]
            logger.info(f"[predictor] Latest candle in MongoDB: {last_ts}")

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

            documents = await cursor.to_list(length=None)
            return pd.DataFrame(documents)
        finally:
            await client.close()

    def read_csv_input(self, csv_path: Path = CSV_PATH) -> pd.DataFrame:
        """Optional local entrypoint for scoring a CSV file with the same service code."""
        return pd.read_csv(csv_path)

    def run_prediction(self, df: pd.DataFrame) -> pd.DataFrame:
        """Rebuild the training features and apply the trained model."""
        if not self.is_ready:
            raise RuntimeError("Model artifacts are not loaded.")
        if df.empty:
            raise ValueError("No historical rows available for prediction.")

        prepared_df = df.copy()
        for column in DECIMAL_COLUMNS:
            prepared_df[column] = prepared_df[column].apply(decimal_to_float)

        prepared_df["trade_count"] = pd.to_numeric(
            prepared_df["trade_count"], errors="coerce"
        )
        prepared_df["open_time_ms"] = pd.to_numeric(
            prepared_df["open_time_ms"], errors="coerce"
        )
        prepared_df = prepared_df.sort_values("open_time_ms").reset_index(drop=True)

        safe_volume = prepared_df["volume"].replace(0, np.nan)

        # Recreate the same feature transformations used during training.
        prepared_df["return"] = prepared_df["close"].pct_change()
        prepared_df["log_return"] = np.log(
            prepared_df["close"] / prepared_df["close"].shift(1)
        )
        prepared_df["volatility"] = prepared_df["return"].rolling(12).std()
        prepared_df["ma_10"] = prepared_df["close"].rolling(10).mean()
        prepared_df["ma_30"] = prepared_df["close"].rolling(30).mean()
        prepared_df["momentum"] = prepared_df["close"] - prepared_df["close"].shift(10)
        prepared_df["buy_ratio"] = prepared_df["taker_buy_base_volume"] / safe_volume
        prepared_df["spread"] = prepared_df["high"] - prepared_df["low"]

        prepared_df = prepared_df.replace([np.inf, -np.inf], np.nan)
        prepared_df = prepared_df.dropna(subset=FEATURES).copy()
        if prepared_df.empty:
            raise ValueError("Not enough recent candles to build prediction features.")

        # The scaler must be the one fitted during training; otherwise the model
        # would receive data in a different feature space.
        x_scaled = self.scaler.transform(prepared_df[FEATURES])
        prepared_df["prediction"] = self.model.predict(x_scaled)
        prepared_df["probability_up"] = self.model.predict_proba(x_scaled)[:, 1]
        prepared_df["signal"] = prepared_df["prediction"].map({0: "DOWN ⬇", 1: "UP ⬆"})
        prepared_df["confidence_%"] = (prepared_df["probability_up"] * 100).round(2)

        # API responses use timestamps derived from the historical candle open time.
        prepared_df["timestamp_ms"] = prepared_df["open_time_ms"].astype(int)
        prepared_df["timestamp_iso"] = pd.to_datetime(
            prepared_df["open_time_ms"], unit="ms"
        ).dt.strftime("%Y-%m-%dT%H:%M:%SZ")

        return prepared_df


predictor = LogisticRegressionPredictor()


def main() -> None:
    """Allow local scoring from CSV while reusing the same service-layer logic."""
    if not predictor.is_ready:
        raise RuntimeError("Model not loaded.")

    df = predictor.read_csv_input()
    predicted_df = predictor.run_prediction(df)
    last_row = predicted_df.iloc[-1]

    print("Prediction completed.")
    print(f"Signal: {last_row['signal']}")
    print(f"Probability up: {last_row['confidence_%']}%")
    print(predicted_df[["timestamp_iso", "signal", "confidence_%"]].tail(20))


if __name__ == "__main__":
    main()
