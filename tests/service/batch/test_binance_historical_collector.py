import json

import pytest

from src.models.models import DataPaths, UpsertStats
from src.service.batch import binance_historical_collector as collector


@pytest.mark.asyncio
async def test_pipeline_triggers_retraining_when_mongo_changes(monkeypatch):
    pipeline = collector.KlinePipeline(
        symbol="BTCUSDT", interval="5m", start_ms=1, end_ms=2
    )
    triggered = []

    monkeypatch.setattr(
        collector.files, "ensure_output_directories", lambda paths: None
    )

    async def fake_extract_raw_data(self):
        return []

    def fake_transform_raw_data(self, merged_raw):
        return []

    async def fake_prepare_mongo_load_rows(self, new_models):
        return []

    async def fake_load_to_mongo(self, models):
        return UpsertStats(requested=1, matched=0, modified=0, upserted=1)

    async def fake_trigger(symbol: str, interval: str):
        triggered.append((symbol, interval))

    monkeypatch.setattr(
        collector.KlinePipeline, "extract_raw_data", fake_extract_raw_data
    )
    monkeypatch.setattr(
        collector.KlinePipeline, "transform_raw_data", fake_transform_raw_data
    )
    monkeypatch.setattr(
        collector.KlinePipeline,
        "prepare_mongo_load_rows",
        fake_prepare_mongo_load_rows,
    )
    monkeypatch.setattr(collector.KlinePipeline, "load_to_mongo", fake_load_to_mongo)
    monkeypatch.setattr(collector.load, "trigger_retraining", fake_trigger)

    stats = await pipeline.run()

    assert stats == UpsertStats(requested=1, matched=0, modified=0, upserted=1)
    assert triggered == [("BTCUSDT", "5m")]


@pytest.mark.asyncio
async def test_pipeline_skips_retraining_when_mongo_is_unchanged(monkeypatch):
    pipeline = collector.KlinePipeline(
        symbol="BTCUSDT", interval="5m", start_ms=1, end_ms=2
    )
    triggered = []

    monkeypatch.setattr(
        collector.files, "ensure_output_directories", lambda paths: None
    )

    async def fake_extract_raw_data(self):
        return []

    def fake_transform_raw_data(self, merged_raw):
        return []

    async def fake_prepare_mongo_load_rows(self, new_models):
        return []

    async def fake_load_to_mongo(self, models):
        return UpsertStats(requested=0, matched=0, modified=0, upserted=0)

    async def fake_trigger(symbol: str, interval: str):
        triggered.append((symbol, interval))

    monkeypatch.setattr(
        collector.KlinePipeline, "extract_raw_data", fake_extract_raw_data
    )
    monkeypatch.setattr(
        collector.KlinePipeline, "transform_raw_data", fake_transform_raw_data
    )
    monkeypatch.setattr(
        collector.KlinePipeline,
        "prepare_mongo_load_rows",
        fake_prepare_mongo_load_rows,
    )
    monkeypatch.setattr(collector.KlinePipeline, "load_to_mongo", fake_load_to_mongo)
    monkeypatch.setattr(collector.load, "trigger_retraining", fake_trigger)

    stats = await pipeline.run()

    assert stats == UpsertStats(requested=0, matched=0, modified=0, upserted=0)
    assert triggered == []


@pytest.mark.asyncio
async def test_extract_refetches_when_raw_range_exists_but_raw_json_is_missing(
    tmp_path, monkeypatch
):
    pipeline = collector.KlinePipeline(
        symbol="BTCUSDT", interval="5m", start_ms=1, end_ms=1
    )
    pipeline.paths = DataPaths(
        raw_dir=tmp_path / "raw_data",
        processed_dir=tmp_path / "processed_data",
        raw_json=tmp_path / "raw_data" / "BTCUSDT_5m.json",
        raw_csv=tmp_path / "raw_data" / "BTCUSDT_5m.csv",
        raw_range=tmp_path / "raw_data" / "BTCUSDT_5m.range.json",
        processed_json=tmp_path / "processed_data" / "BTCUSDT_5m.json",
        processed_csv=tmp_path / "processed_data" / "BTCUSDT_5m.csv",
        processed_range=tmp_path / "processed_data" / "BTCUSDT_5m.range.json",
    )
    pipeline.paths.raw_dir.mkdir(parents=True)
    pipeline.paths.processed_dir.mkdir(parents=True)
    pipeline.paths.raw_range.write_text(json.dumps({"start_ms": 1, "end_ms": 1}))

    captured_missing_ranges = []

    async def fake_fetch_missing_raw_rows(self, missing_ranges):
        captured_missing_ranges.extend(missing_ranges)
        return [[1, "1", "1", "1", "1", "1", 2, "1", 1, "1", "1", "0"]]

    monkeypatch.setattr(
        collector.KlinePipeline,
        "fetch_missing_raw_rows",
        fake_fetch_missing_raw_rows,
    )

    merged_raw_rows = await pipeline.extract_raw_data()

    assert captured_missing_ranges == [collector.common.MissingRange(1, 1)]
    assert merged_raw_rows == [[1, "1", "1", "1", "1", "1", 2, "1", 1, "1", "1", "0"]]


def test_transform_reprocesses_when_processed_range_exists_but_processed_json_is_missing(
    tmp_path,
):
    pipeline = collector.KlinePipeline(
        symbol="BTCUSDT", interval="5m", start_ms=1, end_ms=1
    )
    pipeline.paths = DataPaths(
        raw_dir=tmp_path / "raw_data",
        processed_dir=tmp_path / "processed_data",
        raw_json=tmp_path / "raw_data" / "BTCUSDT_5m.json",
        raw_csv=tmp_path / "raw_data" / "BTCUSDT_5m.csv",
        raw_range=tmp_path / "raw_data" / "BTCUSDT_5m.range.json",
        processed_json=tmp_path / "processed_data" / "BTCUSDT_5m.json",
        processed_csv=tmp_path / "processed_data" / "BTCUSDT_5m.csv",
        processed_range=tmp_path / "processed_data" / "BTCUSDT_5m.range.json",
    )
    pipeline.paths.raw_dir.mkdir(parents=True)
    pipeline.paths.processed_dir.mkdir(parents=True)
    pipeline.paths.processed_range.write_text(json.dumps({"start_ms": 1, "end_ms": 1}))

    raw_rows = [
        [1, "1", "1", "1", "1", "1", 2, "1", 1, "1", "1", "0"],
    ]

    transformed_klines = pipeline.transform_raw_data(raw_rows)

    assert len(transformed_klines) == 1
    assert transformed_klines[0].open_time_ms == 1
