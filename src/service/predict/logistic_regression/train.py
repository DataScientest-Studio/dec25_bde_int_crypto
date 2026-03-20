"""Training entrypoint for the logistic regression model.

The training script reads historical candles from MongoDB, rebuilds the
feature set, trains the model, and persists the model/scaler artifacts for the
prediction API.
"""

import asyncio
import os
from dataclasses import dataclass
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from bson import Decimal128
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, classification_report
from sklearn.preprocessing import StandardScaler

from src.config.mongo_settings import get_settings as get_mongo_settings
from src.database.mongo_client import MongoClient

# Keep this list aligned with `predictor.py`.
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

MODEL_DIR = Path(os.getenv("MODEL_DIR", "/app/models"))
SYMBOL = os.getenv("BINANCE_SYMBOL", "BTCUSDT").strip().upper()
INTERVAL = os.getenv("BINANCE_INTERVAL", "5m").strip()


@dataclass(frozen=True)
class TrainingRunResult:
    symbol: str
    interval: str
    rows_fetched: int
    rows_used_for_training: int
    accuracy: float
    model_path: Path
    scaler_path: Path


def decimal_to_float(value):
    """Normalize Mongo Decimal128 values into floats for pandas/scikit-learn."""
    if isinstance(value, Decimal128):
        return float(value.to_decimal())
    return float(value)


async def fetch_klines(symbol: str, interval: str) -> pd.DataFrame:
    """Read historical candles from MongoDB using the shared app client/settings."""
    settings = get_mongo_settings()
    client = MongoClient(
        uri=settings.mongodb_uri,
        database=settings.mongodb_database,
        collection=settings.mongodb_collection_historical,
    )

    print(f"[trainer] Database : {settings.mongodb_database}")
    print(f"[trainer] Collection: {settings.mongodb_collection_historical}")
    print(f"[trainer] Symbol    : {symbol} {interval}")
    print(f"[trainer] Models    : {MODEL_DIR}")

    try:
        await client.initialize()
        print("[trainer] MongoDB connected")

        collection = client.get_collection()
        # Only fetch the columns needed by the feature engineering step.
        cursor = collection.find(
            {"symbol": symbol, "interval": interval},
            {
                "_id": 0,
                "open_time_ms": 1,
                "close_time_ms": 1,
                "open": 1,
                "high": 1,
                "low": 1,
                "close": 1,
                "volume": 1,
                "trade_count": 1,
                "taker_buy_base_volume": 1,
            },
        ).sort("open_time_ms", 1)

        documents = await cursor.to_list(length=None)
        print(f"[trainer] Documents : {len(documents)}")
        return pd.DataFrame(documents)
    finally:
        await client.close()


def prepare_dataset(df: pd.DataFrame, *, symbol: str, interval: str) -> pd.DataFrame:
    """Rebuild the training frame from MongoDB documents."""
    if df.empty:
        raise ValueError(f"No data found for {symbol} {interval} in MongoDB.")

    for column in DECIMAL_COLUMNS:
        df[column] = df[column].apply(decimal_to_float)

    df["trade_count"] = pd.to_numeric(df["trade_count"], errors="coerce")
    df["open_time_ms"] = pd.to_numeric(df["open_time_ms"], errors="coerce")
    df["close_time_ms"] = pd.to_numeric(df["close_time_ms"], errors="coerce")
    df = df.sort_values("open_time_ms").reset_index(drop=True)

    next_close = df["close"].shift(-1)
    safe_volume = df["volume"].replace(0, np.nan)

    # Keep the original label definition so this refactor does not change model intent.
    df["target"] = (next_close > df["open"]).astype(int)
    # Feature engineering must stay in sync with the predictor service.
    df["return"] = df["close"].pct_change()
    df["log_return"] = np.log(df["close"] / df["close"].shift(1))
    df["volatility"] = df["return"].rolling(12).std()
    df["ma_10"] = df["close"].rolling(10).mean()
    df["ma_30"] = df["close"].rolling(30).mean()
    df["momentum"] = df["close"] - df["close"].shift(10)
    df["buy_ratio"] = df["taker_buy_base_volume"] / safe_volume
    df["spread"] = df["high"] - df["low"]

    df = df.assign(next_close=next_close)
    df = df.dropna(subset=["next_close", *FEATURES]).copy()

    if len(df) < 10:
        raise ValueError(
            "Not enough historical rows after feature engineering to train the model."
        )

    if df["target"].nunique() < 2:
        raise ValueError("Training target must contain at least two classes.")

    print(f"[trainer] Rows after feature engineering: {len(df)}")
    return df


def train_model(df: pd.DataFrame) -> tuple[LogisticRegression, StandardScaler, float]:
    """Split, scale, and train the logistic regression model."""
    train_size = int(len(df) * 0.8)
    if train_size <= 0 or train_size >= len(df):
        raise ValueError("Not enough rows to create both train and test splits.")

    # Use a chronological split so future rows never leak into training.
    train = df.iloc[:train_size]
    test = df.iloc[train_size:]

    scaler = StandardScaler()
    x_train = scaler.fit_transform(train[FEATURES])
    x_test = scaler.transform(test[FEATURES])
    y_train = train["target"]
    y_test = test["target"]

    print("[trainer] Training logistic regression...")
    model = LogisticRegression(max_iter=1000)
    model.fit(x_train, y_train)

    predictions = model.predict(x_test)
    accuracy = accuracy_score(y_test, predictions)

    print(f"[trainer] Accuracy : {accuracy:.4f}")
    print(classification_report(y_test, predictions, zero_division=0))

    return model, scaler, accuracy


def save_artifacts(
    model: LogisticRegression, scaler: StandardScaler
) -> tuple[Path, Path]:
    """Persist the model artifacts for the prediction API container."""
    MODEL_DIR.mkdir(parents=True, exist_ok=True)

    model_path = MODEL_DIR / "logistic_regression_model.pkl"
    scaler_path = MODEL_DIR / "logistic_regression_scaler.pkl"

    joblib.dump(model, model_path)
    joblib.dump(scaler, scaler_path)

    print(f"[trainer] Model saved  : {model_path}")
    print(f"[trainer] Scaler saved : {scaler_path}")
    return model_path, scaler_path


async def run_training_pipeline(
    symbol: str = SYMBOL, interval: str = INTERVAL
) -> TrainingRunResult:
    """Run the training pipeline end-to-end and return a compact summary."""
    df = await fetch_klines(symbol, interval)
    prepared_df = await asyncio.to_thread(
        prepare_dataset, df, symbol=symbol, interval=interval
    )
    model, scaler, accuracy = await asyncio.to_thread(train_model, prepared_df)
    model_path, scaler_path = await asyncio.to_thread(save_artifacts, model, scaler)
    print("[trainer] done")
    return TrainingRunResult(
        symbol=symbol,
        interval=interval,
        rows_fetched=len(df),
        rows_used_for_training=len(prepared_df),
        accuracy=accuracy,
        model_path=model_path,
        scaler_path=scaler_path,
    )


def main() -> None:
    """Run the training pipeline end-to-end."""
    asyncio.run(run_training_pipeline())


if __name__ == "__main__":
    main()
