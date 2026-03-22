import os
import re
import time
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import requests
import streamlit as st
from plotly.subplots import make_subplots

# ── App configuration ─────────────────────────────────────────────────────────
API_URL = os.getenv("API_URL", "http://prediction-api:8000")
SYMBOLS = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT"]
SLIDES_PATH = (
    Path(__file__).resolve().parents[2]
    / "docs"
    / "project_presentation_slides.md"
)
IMG_TAG_PATTERN = re.compile(
    r'<img\s+[^>]*src="([^"]+)"(?:[^>]*width="([^"]+)")?[^>]*>',
    re.IGNORECASE,
)

st.set_page_config(page_title="Crypto ML Dashboard", page_icon="📈", layout="wide")


# ── Shared helpers ────────────────────────────────────────────────────────────
@st.cache_data(ttl=30)
def fetch_predictions(symbol: str, limit: int):
    """Call the prediction API and return a small status envelope for the UI."""
    try:
        response = requests.get(
            f"{API_URL}/predict/logistic/{symbol}", params={"limit": limit}, timeout=10
        )
        if response.ok:
            return {"ok": True, "data": response.json(), "detail": None}

        detail = response.text
        try:
            detail = response.json().get("detail", detail)
        except ValueError:
            pass

        return {
            "ok": False,
            "data": None,
            "detail": detail,
            "status": response.status_code,
        }
    except requests.RequestException as exc:
        return {"ok": False, "data": None, "detail": str(exc), "status": None}


@st.cache_data
def load_presentation_slides() -> list[str]:
    """Load the markdown deck and split it into individual slides."""
    if not SLIDES_PATH.exists():
        return []

    content = SLIDES_PATH.read_text()
    return [slide.strip() for slide in content.split("\n---\n") if slide.strip()]


def resolve_slide_asset_path(src: str) -> Path:
    """Resolve an image path from the markdown deck relative to the slide file."""
    return (SLIDES_PATH.parent / src).resolve()


def render_slide_image(image_path: Path, width_hint: str | None) -> None:
    """Render a local slide image with optional percentage-based centering."""
    if width_hint and width_hint.endswith("%"):
        try:
            percent = max(10, min(100, int(width_hint[:-1])))
        except ValueError:
            percent = 100

        side = max(0, (100 - percent) // 2)
        if side > 0:
            left, center, right = st.columns([side, percent, side])
            with center:
                st.image(str(image_path), use_container_width=True)
            return

    if width_hint and width_hint.isdigit():
        st.image(str(image_path), width=int(width_hint))
        return

    st.image(str(image_path), use_container_width=True)


def render_slide_markdown(slide: str) -> None:
    """Render markdown slides and convert local <img> tags into Streamlit images."""
    markdown_buffer: list[str] = []

    for line in slide.splitlines():
        image_match = IMG_TAG_PATTERN.search(line.strip())
        if image_match:
            if markdown_buffer:
                st.markdown("\n".join(markdown_buffer))
                markdown_buffer = []

            image_src, width_hint = image_match.groups()
            image_path = resolve_slide_asset_path(image_src)

            if image_path.exists():
                render_slide_image(image_path, width_hint)
            else:
                st.warning(f"Image not found: {image_src}")
            continue

        markdown_buffer.append(line)

    if markdown_buffer:
        st.markdown("\n".join(markdown_buffer))


def apply_presentation_styles() -> None:
    """Increase slide body text size for presentation readability."""
    st.markdown(
        """
        <style>
        .stMarkdown p,
        .stMarkdown li {
            font-size: 16pt !important;
            line-height: 1.6 !important;
        }

        .stMarkdown code {
            font-size: 15pt !important;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_dashboard() -> None:
    with st.sidebar:
        st.title("⚙️ Dashboard")

        symbol = st.selectbox("Symbol", SYMBOLS, index=0)
        limit = st.slider(
            "Number of candles", min_value=10, max_value=200, value=50, step=10
        )
        auto_ref = st.toggle("Auto-refresh", value=False)
        interval = st.selectbox("Refresh interval (s)", [10, 30, 60], index=1)

        st.divider()
        if st.button("🔄 Refresh now", use_container_width=True):
            st.cache_data.clear()

    result = fetch_predictions(symbol, limit)

    st.title("📈 Crypto ML Signal Dashboard")
    st.caption(f"Model: Logistic Regression · Symbol: {symbol}")

    if not result["ok"]:
        if result["status"] == 503 and "Model not loaded" in str(result["detail"]):
            st.error(
                "❌ Prediction API is running, but the model artifacts are not loaded "
                "yet. Run the trainer or start the stack with "
                "./scripts/docker-up-clean.sh."
            )
        elif result["status"] is not None:
            st.error(f"❌ Prediction API error ({result['status']}): {result['detail']}")
        else:
            st.error(
                "❌ Cannot reach the prediction API. Make sure prediction-api is "
                "running."
            )
        st.stop()

    data = result["data"]
    signal = data["latest_signal"]
    conf = data["latest_confidence"]
    timestamp = data["latest_timestamp"]
    df = pd.DataFrame(data["predictions"])

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Latest signal", signal)

    with col2:
        color = "normal" if conf >= 50 else "inverse"
        st.metric(
            "Confidence",
            f"{conf:.2f}%",
            delta=f"{conf - 50:.2f}% vs 50%",
            delta_color=color,
        )

    with col3:
        up_count = len(df[df["signal"] == "UP ⬆"])
        down_count = len(df[df["signal"] == "DOWN ⬇"])
        st.metric("UP ⬆", up_count, delta=f"{up_count - down_count:+d} vs DOWN")

    with col4:
        st.metric("Latest candle", timestamp.replace("T", " ").replace("Z", ""))

    st.divider()

    fig = make_subplots(
        rows=3,
        cols=1,
        shared_xaxes=True,
        subplot_titles=(
            "Price (Open / Close)",
            "Signal UP ⬆ / DOWN ⬇",
            "Confidence %",
        ),
        row_heights=[0.45, 0.25, 0.30],
        vertical_spacing=0.08,
    )

    colors = ["#1D9E75" if s == "UP ⬆" else "#E24B4A" for s in df["signal"]]

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

    fig.add_hline(y=50, line_dash="dash", line_color="#888780", row=3, col=1)

    fig.update_layout(
        height=700,
        template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        legend=dict(orientation="h", y=1.02),
        margin=dict(l=0, r=0, t=40, b=0),
    )

    st.plotly_chart(fig, use_container_width=True)

    st.subheader("📋 Predictions table")

    df_display = df[
        ["timestamp_iso", "open", "close", "signal", "confidence_pct"]
    ].copy()
    df_display.columns = ["Timestamp", "Open", "Close", "Signal", "Confidence %"]
    df_display = df_display.sort_values("Timestamp", ascending=False)

    def color_signal(val):
        if "UP" in str(val):
            return "color: #1D9E75; font-weight: bold"
        if "DOWN" in str(val):
            return "color: #E24B4A; font-weight: bold"
        return ""

    def color_confidence(val):
        if val >= 70:
            return "color: #1D9E75"
        if val <= 30:
            return "color: #E24B4A"
        return "color: #EF9F27"

    st.dataframe(
        df_display.style.applymap(color_signal, subset=["Signal"]).applymap(
            color_confidence, subset=["Confidence %"]
        ),
        use_container_width=True,
        hide_index=True,
    )

    if auto_ref:
        st.caption(f"⏱ Auto-refreshing in {interval}s...")
        time.sleep(interval)
        st.cache_data.clear()
        st.rerun()


def render_presentation() -> None:
    slides = load_presentation_slides()

    if not slides:
        st.title("🎞 Project Presentation")
        st.error(
            f"Slides file not found at {SLIDES_PATH}. Rebuild the dashboard image or "
            "check that the markdown deck exists."
        )
        st.stop()

    total_slides = len(slides)
    if "presentation_slide" not in st.session_state:
        st.session_state.presentation_slide = 1

    apply_presentation_styles()

    with st.sidebar:
        st.title("🎞 Presentation")
        st.caption("Display the project slide deck inside Streamlit.")

        show_all = st.toggle("Show all slides", value=False)

        if not show_all:
            selected_slide = st.slider(
                "Slide",
                min_value=1,
                max_value=total_slides,
                value=st.session_state.presentation_slide,
            )
            st.session_state.presentation_slide = selected_slide

    st.title("🎞 CryptoBot with Binance")

    if show_all:
        for index, slide in enumerate(slides, start=1):
            st.markdown(f"### Slide {index}")
            render_slide_markdown(slide)
            if index < total_slides:
                st.divider()
        return

    current_slide = st.session_state.presentation_slide
    prev_col, info_col, next_col = st.columns([1, 3, 1])

    with prev_col:
        if st.button("⬅ Previous", use_container_width=True, disabled=current_slide == 1):
            st.session_state.presentation_slide -= 1
            st.rerun()

    with info_col:
        st.markdown(
            f"<div style='text-align:center; padding-top:0.5rem;'>"
            f"<strong>Slide {current_slide} of {total_slides}</strong>"
            f"</div>",
            unsafe_allow_html=True,
        )

    with next_col:
        if st.button(
            "Next ➡",
            use_container_width=True,
            disabled=current_slide == total_slides,
        ):
            st.session_state.presentation_slide += 1
            st.rerun()

    st.divider()
    render_slide_markdown(slides[current_slide - 1])


# ── Top-level navigation ──────────────────────────────────────────────────────
with st.sidebar:
    st.title("🧭 View")
    view_mode = st.radio("Mode", ["Presentation", "Dashboard"], index=0)
    st.divider()

if view_mode == "Presentation":
    render_presentation()
else:
    render_dashboard()
