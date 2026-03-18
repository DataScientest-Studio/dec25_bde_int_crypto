import os
import asyncio
import joblib
import pandas as pd
import numpy as np
from bson import Decimal128
from motor.motor_asyncio import AsyncIOMotorClient
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, classification_report
from pathlib import Path

# ===============================
# Config via env vars
# ===============================
MONGODB_URI = os.getenv("MONGODB_URI",    "mongodb://admin:password@mongodb-ml:27017/")
MONGODB_DB  = os.getenv("MONGODB_DATABASE", "crypto_data")
COLLECTION  = os.getenv("MONGODB_COLLECTION_HISTORICAL", "klines_historical")
MODEL_DIR   = os.getenv("MODEL_DIR", "/app/models")
SYMBOL      = os.getenv("BINANCE_SYMBOL", "BTCUSDT")
INTERVAL    = os.getenv("BINANCE_INTERVAL", "5m")

print(f"[trainer] MongoDB  : {MONGODB_URI}")
print(f"[trainer] Database : {MONGODB_DB} / {COLLECTION}")
print(f"[trainer] Symbol   : {SYMBOL} {INTERVAL}")
print(f"[trainer] Models   : {MODEL_DIR}")

# ===============================
# 1. Fetch data from MongoDB
# ===============================
async def fetch_klines() -> pd.DataFrame:
    client = AsyncIOMotorClient(MONGODB_URI)
    try:
        await client.admin.command("ping")
        print("[trainer] MongoDB connecté ✅")

        collection = client[MONGODB_DB][COLLECTION]
        cursor = collection.find(
            {"symbol": SYMBOL, "interval": INTERVAL},
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
            }
        ).sort("open_time_ms", 1)

        docs = await cursor.to_list(length=None)
        print(f"[trainer] Documents récupérés : {len(docs)}")
        return pd.DataFrame(docs)
    finally:
        client.close()

df = asyncio.run(fetch_klines())

if df.empty:
    raise ValueError(f"Aucune donnée trouvée pour {SYMBOL} {INTERVAL} dans MongoDB !")

# ===============================
# 2. Conversion Decimal128 → float
# ===============================
decimal_cols = ["open", "high", "low", "close", "volume", "taker_buy_base_volume"]
for col in decimal_cols:
    df[col] = df[col].apply(
        lambda x: float(x.to_decimal()) if isinstance(x, Decimal128) else float(x)
    )

df["trade_count"]   = pd.to_numeric(df["trade_count"],   errors="coerce")
df["open_time_ms"]  = pd.to_datetime(df["open_time_ms"],  unit="ms")
df["close_time_ms"] = pd.to_datetime(df["close_time_ms"], unit="ms")

print(f"[trainer] Types après conversion : {df.dtypes.to_dict()}")

# ===============================
# 3. Target
# ===============================
df["target"] = (df["close"].shift(-1) > df["open"]).astype(int)

# ===============================
# 4. Feature Engineering
# ===============================
df["return"]     = df["close"].pct_change()
df["log_return"] = np.log(df["close"] / df["close"].shift(1))
df["volatility"] = df["return"].rolling(12).std()
df["ma_10"]      = df["close"].rolling(10).mean()
df["ma_30"]      = df["close"].rolling(30).mean()
df["momentum"]   = df["close"] - df["close"].shift(10)
df["buy_ratio"]  = df["taker_buy_base_volume"] / df["volume"]
df["spread"]     = df["high"] - df["low"]
df = df.dropna()
df = df.sort_values(by="open_time_ms")

print(f"[trainer] Lignes après feature engineering : {len(df)}")

# ===============================
# 5. Split train/test
# ===============================
train_size = int(len(df) * 0.8)
train = df[:train_size]
test  = df[train_size:]

features = [
    "log_return", "volatility", "ma_10", "ma_30",
    "momentum", "buy_ratio", "spread", "trade_count"
]

scaler  = StandardScaler()
X_train = scaler.fit_transform(train[features])
X_test  = scaler.transform(test[features])
y_train = train["target"]
y_test  = test["target"]

# ===============================
# 6. Entraînement
# ===============================
print("[trainer] Entraînement...")
model = LogisticRegression(max_iter=1000)
model.fit(X_train, y_train)

predictions = model.predict(X_test)
accuracy    = accuracy_score(y_test, predictions)

print(f"[trainer] Accuracy : {accuracy:.4f}")
print(classification_report(y_test, predictions))

# ===============================
# 7. Sauvegarde .pkl
# ===============================
Path(MODEL_DIR).mkdir(parents=True, exist_ok=True)

model_path  = os.path.join(MODEL_DIR, "logistic_regression_model.pkl")
scaler_path = os.path.join(MODEL_DIR, "logistic_regression_scaler.pkl")

joblib.dump(model,  model_path)
joblib.dump(scaler, scaler_path)

print(f"[trainer] Modèle sauvegardé  : {model_path}")
print(f"[trainer] Scaler sauvegardé  : {scaler_path}")
print("[trainer] done")