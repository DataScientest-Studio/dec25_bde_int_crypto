import pytest

from src.models.models import UpsertStats
from src.service.batch import binance_historical_collector as collector


@pytest.mark.asyncio
async def test_pipeline_triggers_retraining_when_mongo_changes(monkeypatch):
    pipeline = collector.KlinePipeline(
        symbol="BTCUSDT", interval="5m", start_ms=1, end_ms=2
    )
    triggered = []

    monkeypatch.setattr(collector, "ensure_dirs", lambda paths: None)

    async def fake_sync_raw(self):
        return []

    def fake_sync_processed(self, merged_raw):
        return []

    async def fake_prepare_models(self, new_models):
        return []

    async def fake_sync_mongo(self, models):
        return UpsertStats(requested=1, matched=0, modified=0, upserted=1)

    async def fake_trigger(symbol: str, interval: str):
        triggered.append((symbol, interval))

    monkeypatch.setattr(collector.KlinePipeline, "_sync_raw", fake_sync_raw)
    monkeypatch.setattr(collector.KlinePipeline, "_sync_processed", fake_sync_processed)
    monkeypatch.setattr(
        collector.KlinePipeline, "_prepare_models_for_mongo", fake_prepare_models
    )
    monkeypatch.setattr(collector.KlinePipeline, "_sync_mongo", fake_sync_mongo)
    monkeypatch.setattr(collector, "trigger_retraining", fake_trigger)

    stats = await pipeline.run()

    assert stats == UpsertStats(requested=1, matched=0, modified=0, upserted=1)
    assert triggered == [("BTCUSDT", "5m")]


@pytest.mark.asyncio
async def test_pipeline_skips_retraining_when_mongo_is_unchanged(monkeypatch):
    pipeline = collector.KlinePipeline(
        symbol="BTCUSDT", interval="5m", start_ms=1, end_ms=2
    )
    triggered = []

    monkeypatch.setattr(collector, "ensure_dirs", lambda paths: None)

    async def fake_sync_raw(self):
        return []

    def fake_sync_processed(self, merged_raw):
        return []

    async def fake_prepare_models(self, new_models):
        return []

    async def fake_sync_mongo(self, models):
        return UpsertStats(requested=0, matched=0, modified=0, upserted=0)

    async def fake_trigger(symbol: str, interval: str):
        triggered.append((symbol, interval))

    monkeypatch.setattr(collector.KlinePipeline, "_sync_raw", fake_sync_raw)
    monkeypatch.setattr(collector.KlinePipeline, "_sync_processed", fake_sync_processed)
    monkeypatch.setattr(
        collector.KlinePipeline, "_prepare_models_for_mongo", fake_prepare_models
    )
    monkeypatch.setattr(collector.KlinePipeline, "_sync_mongo", fake_sync_mongo)
    monkeypatch.setattr(collector, "trigger_retraining", fake_trigger)

    stats = await pipeline.run()

    assert stats == UpsertStats(requested=0, matched=0, modified=0, upserted=0)
    assert triggered == []
