"""Shared pytest fixtures for all tests."""
import pytest
from testcontainers.mongodb import MongoDbContainer

from src.database.mongo_client import MongoClient
from src.database.mongo_repository import AsyncKlineStore


@pytest.fixture(scope="session")
def mongo_container():
    """Session-scoped MongoDB container for all tests."""
    with MongoDbContainer("mongo:7") as container:
        yield container


@pytest.fixture
async def mongo_client(mongo_container):
    """Fixture providing an initialized MongoDB client."""
    client = MongoClient(
        uri=mongo_container.get_connection_url(),
        database="test_db",
        collection="test_klines"
    )
    await client.initialize()

    yield client

    await client.close()


@pytest.fixture
async def kline_store(mongo_client):
    """Fixture providing an initialized AsyncKlineStore."""
    store = AsyncKlineStore(mongo_client)
    await store.initialize()

    yield store

    # Clean up collection after each test
    await store.collection.delete_many({})
