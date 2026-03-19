import os
import streamlit as st
import requests
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import time

# ── API configuration — reads from environment variable set in docker-compose ──
API_URL = os.getenv("API_URL", "http://prediction-api:8000")

# ── Available trading symbols ──────────────────────────────────────────────────
SYMBOLS = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT"]

# ── Streamlit page configuration ───────────────────────────────────────────────
st.set_page_config(page_title="Crypto ML Dashboard", page_icon="📈", layout="wide")

# ── Sidebar — user controls ────────────────────────────────────────────────────
with st.sidebar:
    st.title("⚙️ Settings")

    # Symbol selector
    symbol = st.selectbox("Symbol", SYMBOLS, index=0)

    # Number of candles to display
    limit = st.slider(
        "Number of candles", min_value=10, max_value=200, value=50, step=10
    )

    # Auto-refresh toggle
    auto_ref = st.toggle("Auto-refresh", value=False)

    # Refresh interval in seconds
    interval = st.selectbox("Refresh interval (s)", [10, 30, 60], index=1)

    st.divider()

    # Manual refresh button — clears the cache to force a new API call
    if st.button("🔄 Refresh now", use_container_width=True):
        st.cache_data.clear()


# ── Data fetching — cached to avoid hammering the API on every rerender ────────
@st.cache_data(ttl=30)
def fetch_predictions(symbol: str, limit: int):
    """
    Calls the prediction API and returns the JSON response.
    Returns None if the API is unreachable or returns an error.
    """
    try:
        response = requests.get(
            f"{API_URL}/predict/logistic/{symbol}", params={"limit": limit}, timeout=10
        )
        response.raise_for_status()
        return response.json()
    except Exception:
        return None


data = fetch_predictions(symbol, limit)

# ── Page header ────────────────────────────────────────────────────────────────
st.title("📈 Crypto ML Signal Dashboard")
st.caption(f"Model: Logistic Regression · Symbol: {symbol}")

# ── Error state — stop rendering if API is down ────────────────────────────────
if data is None:
    st.error("❌ Cannot reach the prediction API. Make sure prediction-api is running.")
    st.stop()

# ── Extract key values from API response ──────────────────────────────────────
signal = data["latest_signal"]
conf = data["latest_confidence"]
timestamp = data["latest_timestamp"]
df = pd.DataFrame(data["predictions"])

# ── KPI metric cards ───────────────────────────────────────────────────────────
col1, col2, col3, col4 = st.columns(4)

with col1:
    # Latest signal for the most recent candle
    st.metric("Latest signal", signal)

with col2:
    # Model confidence — delta shows distance from the 50% neutral threshold
    color = "normal" if conf >= 50 else "inverse"
    st.metric(
        "Confidence",
        f"{conf:.2f}%",
        delta=f"{conf - 50:.2f}% vs 50%",
        delta_color=color,
    )

with col3:
    # Count of UP vs DOWN signals in the current window
    up_count = len(df[df["signal"] == "UP ⬆"])
    down_count = len(df[df["signal"] == "DOWN ⬇"])
    st.metric("UP ⬆", up_count, delta=f"{up_count - down_count:+d} vs DOWN")

with col4:
    # Timestamp of the most recent candle in the database
    st.metric("Latest candle", timestamp.replace("T", " ").replace("Z", ""))

st.divider()

# ── Multi-panel chart ──────────────────────────────────────────────────────────
# 3 stacked panels sharing the same x-axis (timestamp)
fig = make_subplots(
    rows=3,
    cols=1,
    shared_xaxes=True,
    subplot_titles=("Price (Open / Close)", "Signal UP ⬆ / DOWN ⬇", "Confidence %"),
    row_heights=[0.45, 0.25, 0.30],
    vertical_spacing=0.08,
)

# Color each bar green for UP, red for DOWN
colors = ["#1D9E75" if s == "UP ⬆" else "#E24B4A" for s in df["signal"]]

# Panel 1 — open price (dotted) and close price (solid line)
fig.add_trace(
    go.Scatter(
        x=df["timestamp_iso"],
        y=df["open"],
        name="Open",
        line=dict(color="#888780", width=1, dash="dot"),
    ),
    row=1,
    col=1,
)

fig.add_trace(
    go.Scatter(
        x=df["timestamp_iso"],
        y=df["close"],
        name="Close",
        line=dict(color="#378ADD", width=2),
    ),
    row=1,
    col=1,
)

# Panel 2 — one colored bar per candle showing UP or DOWN signal
fig.add_trace(
    go.Bar(
        x=df["timestamp_iso"],
        y=[1] * len(df),
        marker_color=colors,
        name="Signal",
        showlegend=False,
        hovertext=df["signal"],
        hoverinfo="text+x",
    ),
    row=2,
    col=1,
)

# Panel 3 — confidence percentage as a filled area chart
fig.add_trace(
    go.Scatter(
        x=df["timestamp_iso"],
        y=df["confidence_pct"],
        name="Confidence",
        fill="tozeroy",
        line=dict(color="#7F77DD", width=2),
        fillcolor="rgba(127, 119, 221, 0.15)",
    ),
    row=3,
    col=1,
)

# Dashed line at 50% — neutral threshold for the model
fig.add_hline(y=50, line_dash="dash", line_color="#888780", row=3, col=1)

# Global layout settings
fig.update_layout(
    height=700,
    template="plotly_dark",
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    legend=dict(orientation="h", y=1.02),
    margin=dict(l=0, r=0, t=40, b=0),
)

st.plotly_chart(fig, use_container_width=True)

# ── Predictions table ──────────────────────────────────────────────────────────
st.subheader("📋 Predictions table")

# Keep only relevant columns and rename for display
df_display = df[["timestamp_iso", "open", "close", "signal", "confidence_pct"]].copy()
df_display.columns = ["Timestamp", "Open", "Close", "Signal", "Confidence %"]
df_display = df_display.sort_values("Timestamp", ascending=False)


# Color UP green, DOWN red
def color_signal(val):
    if "UP" in str(val):
        return "color: #1D9E75; font-weight: bold"
    elif "DOWN" in str(val):
        return "color: #E24B4A; font-weight: bold"
    return ""


# Color confidence: high = green, low = red, mid = amber
def color_confidence(val):
    if val >= 70:
        return "color: #1D9E75"
    elif val <= 30:
        return "color: #E24B4A"
    return "color: #EF9F27"


st.dataframe(
    df_display.style.applymap(color_signal, subset=["Signal"]).applymap(
        color_confidence, subset=["Confidence %"]
    ),
    use_container_width=True,
    hide_index=True,
)

# ── Auto-refresh logic ─────────────────────────────────────────────────────────
# If enabled, wait N seconds, clear the data cache, then rerun the whole app
if auto_ref:
    st.caption(f"⏱ Auto-refreshing in {interval}s...")
    time.sleep(interval)
    st.cache_data.clear()
    st.rerun()
