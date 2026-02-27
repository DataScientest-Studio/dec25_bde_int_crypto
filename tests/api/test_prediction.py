"""Integration tests for prediction endpoints."""

import pytest
from fastapi.testclient import TestClient
from datetime import datetime

from src.api.main import app


@pytest.fixture
def client():
    """Create a test client for the FastAPI app."""
    return TestClient(app)


class TestPredictionEndpoints:
    """Test suite for prediction API endpoints."""

    def test_predict_post_default_parameters(self, client):
        """Test POST /predict/ with default parameters."""
        response = client.post(
            "/predict/",
            json={"symbol": "BTCUSDT", "interval": "5m", "steps": 12}
        )

        assert response.status_code == 200
        data = response.json()

        # Verify response structure
        assert data["symbol"] == "BTCUSDT"
        assert data["interval"] == "5m"
        assert "current_price" in data
        assert isinstance(data["current_price"], float)
        assert data["current_price"] > 0

        # Verify predictions
        assert "predictions" in data
        assert len(data["predictions"]) == 12

        # Verify first prediction structure
        first_pred = data["predictions"][0]
        assert "timestamp" in first_pred
        assert "step" in first_pred
        assert first_pred["step"] == 1
        assert "predicted_price" in first_pred
        assert "confidence" in first_pred
        assert "lower_bound" in first_pred
        assert "upper_bound" in first_pred

        # Verify confidence is reasonable
        assert 0 <= first_pred["confidence"] <= 1

        # Verify bounds make sense
        assert first_pred["lower_bound"] < first_pred["predicted_price"]
        assert first_pred["predicted_price"] < first_pred["upper_bound"]

        # Verify metadata
        assert data["model_name"] == "MockModel_v1.0"
        assert 0 <= data["confidence"] <= 1
        assert "generated_at" in data

    def test_predict_post_custom_steps(self, client):
        """Test POST /predict/ with custom number of steps."""
        response = client.post(
            "/predict/",
            json={"symbol": "BTCUSDT", "interval": "5m", "steps": 24}
        )

        assert response.status_code == 200
        data = response.json()

        # Verify we get exactly 24 predictions
        assert len(data["predictions"]) == 24

        # Verify steps are sequential
        for i, pred in enumerate(data["predictions"], start=1):
            assert pred["step"] == i

    def test_predict_post_different_symbol(self, client):
        """Test POST /predict/ with different symbol."""
        response = client.post(
            "/predict/",
            json={"symbol": "ETHUSDT", "interval": "5m", "steps": 6}
        )

        assert response.status_code == 200
        data = response.json()

        assert data["symbol"] == "ETHUSDT"
        assert len(data["predictions"]) == 6

    def test_predict_post_different_interval(self, client):
        """Test POST /predict/ with different interval."""
        response = client.post(
            "/predict/",
            json={"symbol": "BTCUSDT", "interval": "15m", "steps": 8}
        )

        assert response.status_code == 200
        data = response.json()

        assert data["interval"] == "15m"
        assert len(data["predictions"]) == 8

    def test_predict_get_default_parameters(self, client):
        """Test GET /predict/{symbol} with default query parameters."""
        response = client.get("/predict/BTCUSDT")

        assert response.status_code == 200
        data = response.json()

        # Verify defaults
        assert data["symbol"] == "BTCUSDT"
        assert data["interval"] == "5m"
        assert len(data["predictions"]) == 12

    def test_predict_get_custom_parameters(self, client):
        """Test GET /predict/{symbol} with custom query parameters."""
        response = client.get(
            "/predict/ETHUSDT",
            params={"interval": "15m", "steps": 10}
        )

        assert response.status_code == 200
        data = response.json()

        assert data["symbol"] == "ETHUSDT"
        assert data["interval"] == "15m"
        assert len(data["predictions"]) == 10

    def test_predict_confidence_decay(self, client):
        """Test that confidence decreases for predictions further in the future."""
        response = client.post(
            "/predict/",
            json={"symbol": "BTCUSDT", "interval": "5m", "steps": 20}
        )

        assert response.status_code == 200
        data = response.json()

        predictions = data["predictions"]

        # Check that confidence generally decreases over time
        # (with some tolerance for randomness)
        first_half_confidence = sum(p["confidence"] for p in predictions[:10]) / 10
        second_half_confidence = sum(p["confidence"] for p in predictions[10:]) / 10

        # Second half should have lower average confidence
        assert second_half_confidence <= first_half_confidence

    def test_predict_timestamps_are_sequential(self, client):
        """Test that prediction timestamps are properly ordered."""
        response = client.post(
            "/predict/",
            json={"symbol": "BTCUSDT", "interval": "5m", "steps": 10}
        )

        assert response.status_code == 200
        data = response.json()

        predictions = data["predictions"]
        timestamps = [datetime.fromisoformat(p["timestamp"]) for p in predictions]

        # Verify timestamps are strictly increasing
        for i in range(len(timestamps) - 1):
            assert timestamps[i] < timestamps[i + 1]

    def test_predict_response_time_reasonable(self, client):
        """Test that prediction endpoint responds quickly."""
        import time

        start = time.time()
        response = client.post(
            "/predict/",
            json={"symbol": "BTCUSDT", "interval": "5m", "steps": 12}
        )
        elapsed = time.time() - start

        assert response.status_code == 200
        # Should respond in less than 1 second for mock predictions
        assert elapsed < 1.0

    def test_predict_prices_are_positive(self, client):
        """Test that all predicted prices are positive."""
        response = client.post(
            "/predict/",
            json={"symbol": "BTCUSDT", "interval": "5m", "steps": 15}
        )

        assert response.status_code == 200
        data = response.json()

        # Verify all prices are positive
        for pred in data["predictions"]:
            assert pred["predicted_price"] > 0
            assert pred["lower_bound"] > 0
            assert pred["upper_bound"] > 0

    def test_predict_invalid_json_body(self, client):
        """Test POST /predict/ with invalid JSON body."""
        response = client.post(
            "/predict/",
            json={"invalid_field": "value"}
        )

        # Should still work with defaults since all fields have defaults
        assert response.status_code == 200

    def test_predict_multiple_calls_return_different_results(self, client):
        """Test that multiple calls return different mock predictions (due to randomness)."""
        response1 = client.post(
            "/predict/",
            json={"symbol": "BTCUSDT", "interval": "5m", "steps": 5}
        )
        response2 = client.post(
            "/predict/",
            json={"symbol": "BTCUSDT", "interval": "5m", "steps": 5}
        )

        assert response1.status_code == 200
        assert response2.status_code == 200

        data1 = response1.json()
        data2 = response2.json()

        # Due to randomness, predictions should differ
        prices1 = [p["predicted_price"] for p in data1["predictions"]]
        prices2 = [p["predicted_price"] for p in data2["predictions"]]

        # At least some prices should be different
        assert prices1 != prices2
