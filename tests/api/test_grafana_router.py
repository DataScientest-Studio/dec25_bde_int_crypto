import json
from datetime import datetime, timezone

from bson import Decimal128
from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.api.routers import grafana


class FakeCursor:
    def __init__(self, docs):
        self.docs = docs

    def sort(self, field, direction):
        reverse = direction == -1
        self.docs = sorted(self.docs, key=lambda doc: doc[field], reverse=reverse)
        return self

    def limit(self, value):
        self.docs = self.docs[:value]
        return self

    def __iter__(self):
        return iter(self.docs)


class FakeCollection:
    def __init__(self, docs):
        self.docs = docs
        self.last_query_filter = None

    def find(self, query_filter, projection):
        self.last_query_filter = query_filter

        filtered = []
        for doc in self.docs:
            if doc["symbol"] != query_filter["symbol"]:
                continue
            if doc["interval"] != query_filter["interval"]:
                continue

            time_range = query_filter.get("open_time_ms")
            if time_range:
                open_time_ms = doc["open_time_ms"]
                if (
                    open_time_ms < time_range["$gte"]
                    or open_time_ms > time_range["$lte"]
                ):
                    continue

            timestamp_range = query_filter.get("timestamp")
            if timestamp_range:
                timestamp = doc["timestamp"]
                if timestamp.tzinfo is None:
                    timestamp = timestamp.replace(tzinfo=timezone.utc)
                if (
                    timestamp < timestamp_range["$gte"]
                    or timestamp > timestamp_range["$lte"]
                ):
                    continue

            projected = {
                field: value
                for field, value in doc.items()
                if field != "_id" and projection.get(field, 0)
            }
            filtered.append(projected)

        return FakeCursor(filtered)


class FakeMongoClient:
    def __init__(self):
        self.closed = False

    def close(self):
        self.closed = True


def build_test_client():
    app = FastAPI()
    app.include_router(grafana.router)
    return TestClient(app)


def sample_docs():
    return [
        {
            "symbol": "BTCUSDT",
            "interval": "5m",
            "open_time_ms": 1773905700000,
            "open": Decimal128("70110.0"),
            "high": Decimal128("70190.0"),
            "low": Decimal128("70080.0"),
            "close": Decimal128("70181.0"),
            "volume": Decimal128("90.5"),
            "quote_volume": Decimal128("6340000.5"),
            "trade_count": 14000,
        },
        {
            "symbol": "BTCUSDT",
            "interval": "5m",
            "open_time_ms": 1773906000000,
            "open": Decimal128("70080.0"),
            "high": Decimal128("70120.0"),
            "low": Decimal128("70010.0"),
            "close": Decimal128("70097.0"),
            "volume": Decimal128("88.4"),
            "quote_volume": Decimal128("6190000.2"),
            "trade_count": 13200,
        },
        {
            "symbol": "BTCUSDT",
            "interval": "15m",
            "open_time_ms": 1773906300000,
            "open": Decimal128("70020.0"),
            "high": Decimal128("70220.0"),
            "low": Decimal128("69980.0"),
            "close": Decimal128("70238.0"),
            "volume": Decimal128("154.0"),
            "quote_volume": Decimal128("10800000.0"),
            "trade_count": 33300,
        },
    ]


def sample_streaming_docs():
    return [
        {
            "symbol": "BTCUSDT",
            "interval": "5m",
            "timestamp": datetime(2026, 3, 19, 7, 40, tzinfo=timezone.utc),
            "event_time": 1773906001000,
            "open": 70080.0,
            "high": 70140.0,
            "low": 70005.0,
            "close": 70105.0,
            "volume": 89.1,
            "quote_volume": 6201000.0,
            "trade_count": 13250,
        },
        {
            "symbol": "BTCUSDT",
            "interval": "5m",
            "timestamp": datetime(2026, 3, 19, 7, 40, tzinfo=timezone.utc),
            "event_time": 1773906002000,
            "open": 70080.0,
            "high": 70155.0,
            "low": 70005.0,
            "close": 70112.0,
            "volume": 90.2,
            "quote_volume": 6209000.0,
            "trade_count": 13300,
        },
        {
            "symbol": "BTCUSDT",
            "interval": "5m",
            "timestamp": datetime(2026, 3, 19, 7, 45, tzinfo=timezone.utc),
            "event_time": 1773906301000,
            "open": 70112.0,
            "high": 70250.0,
            "low": 70090.0,
            "close": 70210.0,
            "volume": 20.5,
            "quote_volume": 1440000.0,
            "trade_count": 4100,
        },
    ]


def test_query_uses_requested_interval(monkeypatch):
    historical_collection = FakeCollection(sample_docs())
    streaming_collection = FakeCollection([])
    mongo_client = FakeMongoClient()

    monkeypatch.setattr(
        grafana,
        "get_collections",
        lambda: (mongo_client, historical_collection, streaming_collection),
    )

    client = build_test_client()
    response = client.post(
        "/grafana/query",
        json={
            "targets": [{"target": "btcusdt_close", "interval": "15m"}],
            "interval": "15m",
            "range": {
                "from": "2026-03-19T07:00:00Z",
                "to": "2026-03-19T08:00:00Z",
            },
            "maxDataPoints": 100,
        },
    )

    assert response.status_code == 200
    assert response.json() == [
        {"target": "btcusdt_close", "datapoints": [[70238.0, 1773906300000]]}
    ]
    assert historical_collection.last_query_filter == {
        "symbol": "BTCUSDT",
        "interval": "15m",
        "open_time_ms": {"$gte": 1773903600000, "$lte": 1773907200000},
    }
    assert mongo_client.closed is True


def test_query_falls_back_to_default_interval(monkeypatch):
    historical_collection = FakeCollection(sample_docs())
    streaming_collection = FakeCollection([])
    mongo_client = FakeMongoClient()

    monkeypatch.setattr(grafana, "DEFAULT_INTERVAL", "5m")
    monkeypatch.setattr(
        grafana,
        "get_collections",
        lambda: (mongo_client, historical_collection, streaming_collection),
    )

    client = build_test_client()
    response = client.post(
        "/grafana/query",
        json={
            "targets": [{"target": "btcusdt_close", "interval": "1m"}],
            "interval": "1m",
            "range": {
                "from": "2026-03-19T07:00:00Z",
                "to": "2026-03-19T08:00:00Z",
            },
            "maxDataPoints": 100,
        },
    )

    assert response.status_code == 200
    assert response.json() == [
        {
            "target": "btcusdt_close",
            "datapoints": [
                [70181.0, 1773905700000],
                [70097.0, 1773906000000],
            ],
        }
    ]
    assert historical_collection.last_query_filter["interval"] == "5m"
    assert mongo_client.closed is True


def test_query_accepts_form_encoded_wrapped_json(monkeypatch):
    historical_collection = FakeCollection(sample_docs())
    streaming_collection = FakeCollection([])
    mongo_client = FakeMongoClient()

    monkeypatch.setattr(
        grafana,
        "get_collections",
        lambda: (mongo_client, historical_collection, streaming_collection),
    )

    client = build_test_client()
    response = client.post(
        "/grafana/query",
        data={
            "data": json.dumps(
                {
                    "targets": [{"target": "btcusdt_close", "interval": "5m"}],
                    "range": {
                        "from": "2026-03-19T07:00:00Z",
                        "to": "2026-03-19T08:00:00Z",
                    },
                    "maxDataPoints": 2,
                }
            )
        },
    )

    assert response.status_code == 200
    assert response.json() == [
        {
            "target": "btcusdt_close",
            "datapoints": [
                [70181.0, 1773905700000],
                [70097.0, 1773906000000],
            ],
        }
    ]
    assert historical_collection.last_query_filter == {
        "symbol": "BTCUSDT",
        "interval": "5m",
        "open_time_ms": {"$gte": 1773903600000, "$lte": 1773907200000},
    }
    assert mongo_client.closed is True


def test_candles_returns_named_rows_for_debug_panel(monkeypatch):
    historical_collection = FakeCollection(sample_docs())
    streaming_collection = FakeCollection([])
    mongo_client = FakeMongoClient()

    monkeypatch.setattr(
        grafana,
        "get_collections",
        lambda: (mongo_client, historical_collection, streaming_collection),
    )

    client = build_test_client()
    response = client.post(
        "/grafana/candles",
        json={
            "interval": "5m",
            "range": {
                "from": "2026-03-19T07:00:00Z",
                "to": "2026-03-19T08:00:00Z",
            },
            "limit": 2,
        },
    )

    assert response.status_code == 200
    assert response.json() == [
        {
            "time": 1773905700000,
            "open": 70110.0,
            "high": 70190.0,
            "low": 70080.0,
            "close": 70181.0,
            "volume": 90.5,
            "quote_volume": 6340000.5,
            "trade_count": 14000,
        },
        {
            "time": 1773906000000,
            "open": 70080.0,
            "high": 70120.0,
            "low": 70010.0,
            "close": 70097.0,
            "volume": 88.4,
            "quote_volume": 6190000.2,
            "trade_count": 13200,
        },
    ]
    assert mongo_client.closed is True


def test_candles_accepts_form_encoded_wrapped_json(monkeypatch):
    historical_collection = FakeCollection(sample_docs())
    streaming_collection = FakeCollection([])
    mongo_client = FakeMongoClient()

    monkeypatch.setattr(
        grafana,
        "get_collections",
        lambda: (mongo_client, historical_collection, streaming_collection),
    )

    client = build_test_client()
    response = client.post(
        "/grafana/candles",
        data={
            "data": json.dumps(
                {
                    "interval": "5m",
                    "range": {
                        "from": "2026-03-19T07:00:00Z",
                        "to": "2026-03-19T08:00:00Z",
                    },
                    "limit": 2,
                }
            )
        },
    )

    assert response.status_code == 200
    assert response.json() == [
        {
            "time": 1773905700000,
            "open": 70110.0,
            "high": 70190.0,
            "low": 70080.0,
            "close": 70181.0,
            "volume": 90.5,
            "quote_volume": 6340000.5,
            "trade_count": 14000,
        },
        {
            "time": 1773906000000,
            "open": 70080.0,
            "high": 70120.0,
            "low": 70010.0,
            "close": 70097.0,
            "volume": 88.4,
            "quote_volume": 6190000.2,
            "trade_count": 13200,
        },
    ]
    assert historical_collection.last_query_filter == {
        "symbol": "BTCUSDT",
        "interval": "5m",
        "open_time_ms": {"$gte": 1773903600000, "$lte": 1773907200000},
    }
    assert mongo_client.closed is True


def test_query_returns_default_close_series_when_body_is_missing(monkeypatch):
    historical_collection = FakeCollection(sample_docs())
    streaming_collection = FakeCollection([])
    mongo_client = FakeMongoClient()

    monkeypatch.setattr(
        grafana,
        "get_collections",
        lambda: (mongo_client, historical_collection, streaming_collection),
    )

    client = build_test_client()
    response = client.post("/grafana/query")

    assert response.status_code == 200
    assert response.json() == [
        {
            "target": "btcusdt_close",
            "datapoints": [
                [70181.0, 1773905700000],
                [70097.0, 1773906000000],
            ],
        }
    ]
    assert historical_collection.last_query_filter["interval"] == "5m"
    assert mongo_client.closed is True


def test_candles_overlay_latest_streaming_rows_on_historical(monkeypatch):
    historical_collection = FakeCollection(sample_docs())
    streaming_collection = FakeCollection(sample_streaming_docs())
    mongo_client = FakeMongoClient()

    monkeypatch.setattr(
        grafana,
        "get_collections",
        lambda: (mongo_client, historical_collection, streaming_collection),
    )

    client = build_test_client()
    response = client.post(
        "/grafana/candles",
        json={
            "interval": "5m",
            "range": {
                "from": "2026-03-19T07:00:00Z",
                "to": "2026-03-19T08:00:00Z",
            },
            "limit": 5,
        },
    )

    assert response.status_code == 200
    assert response.json() == [
        {
            "time": 1773905700000,
            "open": 70110.0,
            "high": 70190.0,
            "low": 70080.0,
            "close": 70181.0,
            "volume": 90.5,
            "quote_volume": 6340000.5,
            "trade_count": 14000,
        },
        {
            "time": 1773906000000,
            "open": 70080.0,
            "high": 70155.0,
            "low": 70005.0,
            "close": 70112.0,
            "volume": 90.2,
            "quote_volume": 6209000.0,
            "trade_count": 13300,
        },
        {
            "time": 1773906300000,
            "open": 70112.0,
            "high": 70250.0,
            "low": 70090.0,
            "close": 70210.0,
            "volume": 20.5,
            "quote_volume": 1440000.0,
            "trade_count": 4100,
        },
    ]
    assert streaming_collection.last_query_filter == {
        "symbol": "BTCUSDT",
        "interval": "5m",
        "timestamp": {
            "$gte": datetime(2026, 3, 19, 7, 0, tzinfo=timezone.utc),
            "$lte": datetime(2026, 3, 19, 8, 0, tzinfo=timezone.utc),
        },
    }
    assert mongo_client.closed is True


def test_annotations_get_returns_empty_list_body():
    client = build_test_client()

    response = client.get("/grafana/annotations")

    assert response.status_code == 200
    assert response.json() == []


def test_annotations_post_returns_empty_list_body():
    client = build_test_client()

    response = client.post(
        "/grafana/annotations",
        json={
            "range": {
                "from": "2026-03-19T07:00:00Z",
                "to": "2026-03-19T08:00:00Z",
            }
        },
    )

    assert response.status_code == 200
    assert response.json() == []
