from typing import AsyncGenerator

from src.config.mongo_settings import get_settings
from src.database.mongo_client import MongoClient


async def get_historical_mongo_client() -> AsyncGenerator[MongoClient, None]:
    """FastAPI dependency for historical data MongoDB client."""
    settings = get_settings()
    client = MongoClient(
        uri=settings.mongodb_uri,
        database=settings.mongodb_database,
        collection=settings.mongodb_collection_historical,
    )
    await client.initialize()
    try:
        yield client
    finally:
        await client.close()


async def get_streaming_mongo_client() -> AsyncGenerator[MongoClient, None]:
    """FastAPI dependency for streaming data MongoDB client."""
    settings = get_settings()
    client = MongoClient(
        uri=settings.mongodb_uri,
        database=settings.mongodb_database,
        collection=settings.mongodb_collection_streaming,
    )
    await client.initialize()
    try:
        yield client
    finally:
        await client.close()
