from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Sequence

from src.models.binance_models import HistoricalKline


@dataclass(frozen=True)
class UpsertStats:
    requested: int
    matched: int
    modified: int
    upserted: int


class AsyncKlineStore:
    """Async Mongo store using Motor.
    Why:
    - Non-blocking writes while we are fetching pages from Binance.
    - Bulk upsert keeps Mongo calls efficient (idempotent reruns).
    """

    def __init__(self, *, uri: str, database: str, collection: str) -> None:
        try:
            from motor.motor_asyncio import AsyncIOMotorClient  # type: ignore
        except Exception as e:  # pragma: no cover
            raise RuntimeError("Motor is required: uv add motor pymongo") from e

        self.client = AsyncIOMotorClient(uri)
        self.collection = self.client[database][collection]

    async def close(self) -> None:
        self.client.close()

    async def ensure_indexes(self) -> None:
        # Unique index makes upserts idempotent: reruns don't create duplicates.
        await self.collection.create_index(
            [("symbol", 1), ("interval", 1), ("open_time_ms", 1)],
            unique=True,
            name="uniq_symbol_interval_open_time_ms",
        )

    async def upsert_many(self, klines: Sequence[HistoricalKline]) -> UpsertStats:
        if not klines:
            return UpsertStats(0, 0, 0, 0)

        from pymongo import UpdateOne  # type: ignore

        ops: list[Any] = []
        for k in klines:
            ops.append(UpdateOne(k.key(), {"$set": k.to_mongo_doc()}, upsert=True))

        result = await self.collection.bulk_write(ops, ordered=False)

        return UpsertStats(
            requested=len(ops),
            matched=getattr(result, "matched_count", 0),
            modified=getattr(result, "modified_count", 0),
            upserted=len(getattr(result, "upserted_ids", {}) or {}),
        )
