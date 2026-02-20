"""Tests for AsyncKlineStore using MongoDB testcontainers."""
from decimal import Decimal

import pytest

from src.models.models import HistoricalKline, UpsertStats
from src.database.mongo_repository import AsyncKlineStore


@pytest.fixture
def sample_klines():
    """Fixture providing sample klines for testing."""
    return [
        HistoricalKline(
            symbol="BTCUSDT",
            interval="5m",
            open_time_ms=1000000,
            close_time_ms=1000300,
            open=Decimal("50000.00"),
            high=Decimal("50100.00"),
            low=Decimal("49900.00"),
            close=Decimal("50050.00"),
            volume=Decimal("100.5"),
            quote_volume=Decimal("5000000.00"),
            trade_count=500,
            taker_buy_base_volume=Decimal("60.0"),
            taker_buy_quote_volume=Decimal("3000000.00"),
        ),
        HistoricalKline(
            symbol="BTCUSDT",
            interval="5m",
            open_time_ms=2000000,
            close_time_ms=2000300,
            open=Decimal("50050.00"),
            high=Decimal("50200.00"),
            low=Decimal("50000.00"),
            close=Decimal("50150.00"),
            volume=Decimal("150.5"),
            quote_volume=Decimal("7500000.00"),
            trade_count=600,
            taker_buy_base_volume=Decimal("90.0"),
            taker_buy_quote_volume=Decimal("4500000.00"),
        ),
    ]


@pytest.mark.asyncio
async def test_upsert_many_inserts_new_klines(kline_store: AsyncKlineStore, sample_klines):
    """Test that upsert_many successfully inserts new klines."""
    stats = await kline_store.upsert_many(sample_klines)

    assert stats.requested == 2
    assert stats.upserted == 2
    assert stats.matched == 0
    assert stats.modified == 0


@pytest.mark.asyncio
async def test_upsert_many_updates_existing_klines(kline_store: AsyncKlineStore, sample_klines):
    """Test that upsert_many updates existing klines with the same key."""
    # First insert
    await kline_store.upsert_many(sample_klines)

    # Modify the klines
    modified_klines = [
        HistoricalKline(
            symbol="BTCUSDT",
            interval="5m",
            open_time_ms=1000000,
            close_time_ms=1000300,
            open=Decimal("51000.00"),  # Changed
            high=Decimal("51100.00"),  # Changed
            low=Decimal("50900.00"),  # Changed
            close=Decimal("51050.00"),  # Changed
            volume=Decimal("200.5"),  # Changed
            quote_volume=Decimal("10000000.00"),  # Changed
            trade_count=800,  # Changed
            taker_buy_base_volume=Decimal("120.0"),  # Changed
            taker_buy_quote_volume=Decimal("6000000.00"),  # Changed
        ),
    ]

    # Second insert (should update)
    stats = await kline_store.upsert_many(modified_klines)

    assert stats.requested == 1
    assert stats.matched == 1
    assert stats.modified == 1
    assert stats.upserted == 0

    # Verify the data was updated
    doc = await kline_store.collection.find_one({
        "symbol": "BTCUSDT",
        "interval": "5m",
        "open_time_ms": 1000000
    })
    assert doc is not None
    assert str(doc["open"]) == "51000.00"


@pytest.mark.asyncio
async def test_upsert_many_empty_list(kline_store: AsyncKlineStore):
    """Test that upsert_many handles empty list gracefully."""
    stats = await kline_store.upsert_many([])

    assert stats.requested == 0
    assert stats.upserted == 0
    assert stats.matched == 0
    assert stats.modified == 0


@pytest.mark.asyncio
async def test_ensure_indexes_creates_unique_index(kline_store: AsyncKlineStore):
    """Test that ensure_indexes creates the unique index."""
    await kline_store.ensure_indexes()

    # Get index information
    indexes = await kline_store.collection.list_indexes().to_list(length=None)
    index_names = [idx["name"] for idx in indexes]

    assert "uniq_symbol_interval_open_time_ms" in index_names

    # Verify the index is unique
    unique_index = next(
        idx for idx in indexes if idx["name"] == "uniq_symbol_interval_open_time_ms"
    )
    assert unique_index["unique"] is True


@pytest.mark.asyncio
async def test_ensure_indexes_prevents_duplicate_keys(kline_store: AsyncKlineStore, sample_klines):
    """Test that the unique index prevents duplicate (symbol, interval, open_time_ms)."""
    await kline_store.ensure_indexes()

    # Insert first kline
    await kline_store.upsert_many([sample_klines[0]])

    # Try to insert a different kline with the same key
    duplicate_kline = HistoricalKline(
        symbol="BTCUSDT",
        interval="5m",
        open_time_ms=1000000,  # Same as sample_klines[0]
        close_time_ms=1000300,
        open=Decimal("99999.00"),  # Different value
        high=Decimal("99999.00"),
        low=Decimal("99999.00"),
        close=Decimal("99999.00"),
        volume=Decimal("999.0"),
        quote_volume=Decimal("999999.00"),
        trade_count=999,
        taker_buy_base_volume=Decimal("999.0"),
        taker_buy_quote_volume=Decimal("999999.00"),
    )

    # This should update (upsert), not fail
    stats = await kline_store.upsert_many([duplicate_kline])
    assert stats.matched == 1
    assert stats.modified == 1


@pytest.mark.asyncio
async def test_ensure_indexes_idempotent(kline_store: AsyncKlineStore):
    """Test that ensure_indexes can be called multiple times safely."""
    await kline_store.ensure_indexes()
    await kline_store.ensure_indexes()
    await kline_store.ensure_indexes()

    # Should not raise any errors and index should still exist
    indexes = await kline_store.collection.list_indexes().to_list(length=None)
    index_names = [idx["name"] for idx in indexes]
    assert "uniq_symbol_interval_open_time_ms" in index_names


@pytest.mark.asyncio
async def test_upsert_many_calls_ensure_indexes_automatically(kline_store: AsyncKlineStore, sample_klines):
    """Test that upsert_many automatically ensures indexes."""
    # Don't manually call ensure_indexes, just upsert
    await kline_store.upsert_many(sample_klines)

    # Verify index was created
    indexes = await kline_store.collection.list_indexes().to_list(length=None)
    index_names = [idx["name"] for idx in indexes]
    assert "uniq_symbol_interval_open_time_ms" in index_names


@pytest.mark.asyncio
async def test_upsert_many_mixed_insert_and_update(kline_store: AsyncKlineStore, sample_klines):
    """Test upsert_many with a mix of new and existing klines."""
    # Insert first kline
    await kline_store.upsert_many([sample_klines[0]])

    # Prepare a list with one update and one new insert
    updated_kline = HistoricalKline(
        symbol="BTCUSDT",
        interval="5m",
        open_time_ms=1000000,  # Same as sample_klines[0]
        close_time_ms=1000300,
        open=Decimal("60000.00"),  # Updated value
        high=Decimal("60100.00"),
        low=Decimal("59900.00"),
        close=Decimal("60050.00"),
        volume=Decimal("300.0"),
        quote_volume=Decimal("18000000.00"),
        trade_count=900,
        taker_buy_base_volume=Decimal("180.0"),
        taker_buy_quote_volume=Decimal("10800000.00"),
    )

    stats = await kline_store.upsert_many([updated_kline, sample_klines[1]])

    assert stats.requested == 2
    assert stats.matched == 1  # One matched (updated)
    assert stats.modified == 1  # One modified
    assert stats.upserted == 1  # One new insert


@pytest.mark.asyncio
async def test_collection_isolation_between_tests(kline_store: AsyncKlineStore, sample_klines):
    """Test that the collection is clean for each test (isolation)."""
    # This test should start with an empty collection
    count = await kline_store.collection.count_documents({})
    assert count == 0

    # Insert data
    await kline_store.upsert_many(sample_klines)

    # Verify data was inserted
    count = await kline_store.collection.count_documents({})
    assert count == 2
