from pathlib import Path

from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.api.routers import predict_logistic_regression
from src.service.predict.logistic_regression.train import TrainingRunResult


def build_test_client():
    app = FastAPI()
    app.include_router(predict_logistic_regression.router)
    return TestClient(app)


def test_retrain_endpoint_runs_training_and_reloads_predictor(monkeypatch):
    calls = {}

    async def fake_run_training_pipeline(symbol: str, interval: str):
        calls["args"] = (symbol, interval)
        return TrainingRunResult(
            symbol=symbol,
            interval=interval,
            rows_fetched=240,
            rows_used_for_training=180,
            accuracy=0.73,
            model_path=Path("/tmp/logistic_regression_model.pkl"),
            scaler_path=Path("/tmp/logistic_regression_scaler.pkl"),
        )

    def fake_reload_artifacts():
        calls["reloaded"] = True
        predict_logistic_regression.predictor.model = object()
        predict_logistic_regression.predictor.scaler = object()

    predict_logistic_regression.predictor.model = None
    predict_logistic_regression.predictor.scaler = None
    monkeypatch.setattr(
        predict_logistic_regression,
        "run_training_pipeline",
        fake_run_training_pipeline,
    )
    monkeypatch.setattr(
        predict_logistic_regression.predictor,
        "reload_artifacts",
        fake_reload_artifacts,
    )

    client = build_test_client()
    response = client.post(
        "/predict/logistic/admin/retrain",
        json={"symbol": "btcusdt", "interval": "5m"},
    )

    assert response.status_code == 200
    assert response.json() == {
        "status": "trained",
        "symbol": "BTCUSDT",
        "interval": "5m",
        "rows_fetched": 240,
        "rows_used_for_training": 180,
        "accuracy": 0.73,
        "model_path": "/tmp/logistic_regression_model.pkl",
        "scaler_path": "/tmp/logistic_regression_scaler.pkl",
    }
    assert calls == {"args": ("BTCUSDT", "5m"), "reloaded": True}


def test_retrain_endpoint_rejects_unsupported_interval():
    client = build_test_client()
    response = client.post(
        "/predict/logistic/admin/retrain",
        json={"symbol": "BTCUSDT", "interval": "1m"},
    )

    assert response.status_code == 400
    assert "Interval must be one of" in response.json()["detail"]
