import os
import joblib
import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, classification_report
from pathlib import Path

# ===============================
# Config via env vars
# ===============================
CSV_PATH  = os.getenv("CSV_PATH",  "/app/data/raw_data/BTCUSDT_5m.csv")
MODEL_DIR = os.getenv("MODEL_DIR", "/app/models")

print(f"[trainer] CSV     : {CSV_PATH}")
print(f"[trainer] Models  : {MODEL_DIR}")

# ===============================
# 1. Chargement CSV
# ===============================
df = pd.read_csv(CSV_PATH)
print(f"[trainer] Lignes chargées : {len(df)}")

# ===============================
# 2. Preprocessing
# ===============================
df["open_time_ms"]  = pd.to_datetime(df["open_time_ms"],  unit="ms")
df["close_time_ms"] = pd.to_datetime(df["close_time_ms"], unit="ms")

if "ignore" in df.columns:
    df = df.drop(columns=["ignore"])

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