# predict.py

import joblib
import pandas as pd
import numpy as np

# ===============================
# 1. Charger le pipeline
# ===============================

loaded_model = joblib.load(
    "src/service/predict/logistic_regression/logistic_regression_model.pkl"
)

scaler = joblib.load(
    "src/service/predict/logistic_regression/logistic_regression_scaler.pkl"
)

print("model chargé avec succès.")

# ===============================
# 2. Charger nouvelles données
# ===============================

df = pd.read_csv("src/service/predict/logistic_regression/data.csv")

# show type of each column
print(df.dtypes)

# Refaire exactement le même Feature Engineering

df["return"] = df["close"].pct_change()
df["log_return"] = np.log(df["close"] / df["close"].shift(1))
df["volatility"] = df["return"].rolling(12).std()
df["ma_10"] = df["close"].rolling(10).mean()
df["ma_30"] = df["close"].rolling(30).mean()
df["momentum"] = df["close"] - df["close"].shift(10)
df["buy_ratio"] = df["taker_buy_base_volume"] / df["volume"]
df["spread"] = df["high"] - df["low"]

df = df.dropna()

features = [
    "log_return",
    "volatility",
    "ma_10",
    "ma_30",
    "momentum",
    "buy_ratio",
    "spread",
    "trade_count",
]

X_new = df[features]

# =====================================
# Scaling (IMPORTANT)
# =====================================

X_scaled = scaler.transform(X_new)

# ===============================
# 5. Vérification cohérence
# ===============================

print("\n🔎 Vérification features")
print("Features attendues :", loaded_model.n_features_in_)
print("Features fournies :", X_new.shape[1])

# ===============================
# 6. Prédictions
# ===============================

predictions = loaded_model.predict(X_scaled)
probabilities = loaded_model.predict_proba(X_scaled)[:, 1]

df["prediction"] = predictions
df["probability_up"] = probabilities

# ===============================
# 7. Affichage utilisateur clair
# ===============================

df["signal"] = df["prediction"].map({0: "DOWN ⬇", 1: "UP ⬆"})

df["confidence_%"] = (df["probability_up"] * 100).round(2)

print("\n" + "=" * 50)
print("        📊 BITCOIN SIGNAL PREDICTION")
print("=" * 50)

last_row = df.iloc[-1]

print(f"Signal : {last_row['signal']}")
print(f"Probabilité de hausse : {last_row['confidence_%']} %")
print("=" * 50)

print("\n📌 Dernières prédictions :\n")
print(df[["signal", "confidence_%"]].tail(20))

print("\n✅ Prédictions terminées.")
