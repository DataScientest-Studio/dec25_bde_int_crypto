"""
Prediction router for ML-based cryptocurrency price forecasting.

This module provides endpoints for predicting future prices.
Currently uses mock data until the actual ML model is trained.
"""

import logging
import random
from datetime import datetime, timezone, timedelta

from fastapi import APIRouter, Query
from pydantic import BaseModel

from src.models.models import PredictionRequest, PredictionResponse

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/predict",
    tags=["prediction"],
    responses={404: {"description": "Not found"}},
)


@router.post("/")
async def predict(request: PredictionRequest) -> PredictionResponse:
    """
    Predict future prices using ML model (currently using mock data).

    This endpoint will be used to forecast cryptocurrency prices.
    Currently returns mock data until the actual model is trained.

    Args:
        request: PredictionRequest with symbol, interval, and steps

    Returns:
        PredictionResponse with predicted prices
    """
    logger.info(f"Prediction request: symbol={request.symbol}, interval={request.interval}, steps={request.steps}")

    # Mock current price (in production, this would come from latest data in MongoDB)
    current_price = 50000.0 + random.uniform(-1000, 1000)

    # Generate mock predictions
    predictions = []
    base_time = datetime.now(tz=timezone.utc)

    # Simulate realistic price movements with trend and noise
    trend = random.choice([-1, 1])  # Random upward or downward trend

    for i in range(1, request.steps + 1):
        # Calculate timestamp for this prediction
        # 5m interval means 5 minutes per step
        interval_minutes = int(request.interval.replace('m', ''))
        prediction_time = base_time + timedelta(minutes=interval_minutes * i)

        # Generate mock price with trend and random walk
        price_change = trend * random.uniform(0, 200) + random.uniform(-150, 150)
        predicted_price = current_price + price_change * i * 0.1

        # Add some confidence decay (predictions further out are less confident)
        confidence = max(0.5, 0.95 - (i * 0.03))

        predictions.append({
            "timestamp": prediction_time.isoformat(),
            "step": i,
            "predicted_price": round(predicted_price, 2),
            "confidence": round(confidence, 3),
            "lower_bound": round(predicted_price * 0.98, 2),
            "upper_bound": round(predicted_price * 1.02, 2)
        })

    response = PredictionResponse(
        symbol=request.symbol,
        interval=request.interval,
        current_price=round(current_price, 2),
        predictions=predictions,
        model_name="MockModel_v1.0",
        confidence=0.75,
        generated_at=datetime.now(tz=timezone.utc)
    )

    logger.info(f"Generated {len(predictions)} predictions for {request.symbol}")

    return response


@router.get("/{symbol}")
async def predict_get(
    symbol: str,
    interval: str = Query(default="5m", description="Kline interval (e.g., 5m, 15m, 1h)"),
    steps: int = Query(default=12, description="Number of future steps to predict")
) -> PredictionResponse:
    """
    GET endpoint for predictions (convenience wrapper).

    Example: GET /predict/BTCUSDT?interval=5m&steps=12
    """
    request = PredictionRequest(symbol=symbol, interval=interval, steps=steps)
    return await predict(request)
