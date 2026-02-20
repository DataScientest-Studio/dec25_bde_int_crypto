import asyncio
from dataclasses import dataclass, field
from typing import Optional

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection

from src.config.mongo_settings import MongoSettings


@dataclass
class MongoClient:
    uri: str
    database: str
    collection: str
    _client: Optional[AsyncIOMotorClient] = field(default=None, init=False)
    _collection: Optional[AsyncIOMotorCollection] = field(default=None, init=False)
    _initialized: bool = field(default=False, init=False)

    async def initialize(self) -> None:
        """Initialize the MongoDB connection and ping the server."""
        if self._initialized:
            return

        self._client = AsyncIOMotorClient(self.uri)
        db = self._client[self.database]
        self._collection = db[self.collection]

        # Ping the server to verify connection
        await self._client.admin.command('ping')
        self._initialized = True

    def build_collection(self) -> AsyncIOMotorCollection:
        """Legacy method - creates collection without verification."""
        if not self._initialized:
            self._client = AsyncIOMotorClient(self.uri)
            db = self._client[self.database]
            self._collection = db[self.collection]
        return self._collection

    def get_collection(self) -> AsyncIOMotorCollection:
        """Get the initialized collection."""
        if not self._initialized:
            raise RuntimeError("MongoClient not initialized. Call 'await client.initialize()' first.")
        return self._collection

    async def close(self) -> None:
        """Close the MongoDB connection."""
        if self._client is not None:
            self._client.close()
            self._client = None
            self._collection = None
            self._initialized = False