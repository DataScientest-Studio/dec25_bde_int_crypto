from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Sequence

from src.models.models import HistoricalKline


@dataclass(frozen=True)
class UpsertStats:
    requested: int
    matched: int
    modified: int
    upserted: int


class AsyncKlineStore:
    """Async Mongo store using Motor.

    Why the partial unique index?
    - We want a UNIQUE constraint on (symbol, interval, open_time_ms).
    - Older/legacy docs may have open_time_ms missing/null.
      Mongo treats missing as null for indexing, so many docs collapse to the same key
      and UNIQUE index creation fails.

    Compatibility note:
    - Some Mongo versions/providers don't allow `$ne: null` in `partialFilterExpression`
      (it is internally rewritten to `$not: {$eq: null}` which is rejected).
    - We use `open_time_ms > 0` instead, which is valid for our domain and widely supported.
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
        """Create the unique kline index (safe to call repeatedly)."""
        name = "uniq_symbol_interval_open_time_ms"
        keys = [("symbol", 1), ("interval", 1), ("open_time_ms", 1)]
        opts = {"unique": True, "name": name, "partialFilterExpression": {"open_time_ms": {"$gt": 0}}}

        print(f"[mongo] ensure index {name} (partial unique open_time_ms>0)")

        try:
            await self.collection.create_index(keys, **opts)
            return
        except Exception as e:
            msg = str(e)
            conflict = any(s in msg for s in
                           ("IndexOptionsConflict", "IndexKeySpecsConflict", "same name as the requested index",
                            "different options"))
            if conflict:
                print(f"[mongo] index conflict -> drop & recreate: {name}")
                try:
                    await self.collection.drop_index(name)
                except Exception:
                    pass
                await self.collection.create_index(keys, **opts)
                return

            if "E11000" in msg or "duplicate key" in msg:
                print("[mongo] duplicate keys exist for (symbol, interval, open_time_ms)")

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
