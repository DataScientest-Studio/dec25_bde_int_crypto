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

    Notes
    - We enforce idempotency with a UNIQUE index on (symbol, interval, open_time_ms).
    - In real repos, you may already have older documents that *don't* have open_time_ms.
      Those docs look like open_time_ms = null to Mongo's index builder and can break a
      UNIQUE index.

    Fix:
    - Use a *partial* UNIQUE index so the constraint only applies when open_time_ms exists.
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
        """Create indexes (safe to call repeatedly).

        Uses a partial unique index to avoid failures when old documents have
        open_time_ms missing/null.
        """

        index_name = "uniq_symbol_interval_open_time_ms"
        keys = [("symbol", 1), ("interval", 1), ("open_time_ms", 1)]
        options = {
            "unique": True,
            "name": index_name,
            "partialFilterExpression": {"open_time_ms": {"$exists": True, "$ne": None}},
        }

        print(f"[mongo] ensuring index {index_name} (partial unique)")

        try:
            await self.collection.create_index(keys, **options)
            return
        except Exception as e:
            msg = str(e)

            # If the index already exists with different options, drop & recreate.
            if (
                "already exists" in msg
                or "IndexOptionsConflict" in msg
                or "different options" in msg
                or "conflicts with" in msg
            ):
                print(f"[mongo] index conflict for {index_name}; dropping and recreating")
                try:
                    await self.collection.drop_index(index_name)
                except Exception:
                    pass
                await self.collection.create_index(keys, **options)
                return

            # If it still fails due to true duplicates (same open_time_ms), surface a helpful hint.
            if "E11000" in msg or "duplicate key" in msg:
                print(
                    "[mongo] UNIQUE index build failed due to duplicate keys. "
                    "This usually means you have duplicate (symbol, interval, open_time_ms) records. "
                    "Consider deduping the collection for that key."
                )

            raise

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
