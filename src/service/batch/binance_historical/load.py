from __future__ import annotations

import asyncio
from typing import List

import httpx

from src.constants import TRAINING_TRIGGER_TIMEOUT_S, TRAINING_TRIGGER_URL
from src.database import MongoClient
from src.database.mongo_repository import AsyncKlineStore
from src.models.models import HistoricalKline, UpsertStats


async def load_klines_into_mongo(
    klines: List[HistoricalKline], client: MongoClient
) -> UpsertStats:
    if not klines:
        print("[mongo] nothing to upsert", flush=True)
        return UpsertStats(0, 0, 0, 0)

    store = AsyncKlineStore(client)
    await store.initialize()

    try:
        stats = await store.upsert_many(klines)
        print(
            f"[mongo] upsert done requested={stats.requested} matched={stats.matched} "
            f"modified={stats.modified} upserted={stats.upserted}",
            flush=True,
        )
        return stats
    finally:
        await store.close()


def retraining_is_needed(stats: UpsertStats) -> bool:
    return stats.upserted > 0 or stats.modified > 0


async def trigger_retraining(symbol: str, interval: str) -> None:
    if not TRAINING_TRIGGER_URL:
        print(
            "[train-trigger] skipped: TRAINING_TRIGGER_URL is not configured",
            flush=True,
        )
        return

    payload = {"symbol": symbol, "interval": interval}
    timeout = httpx.Timeout(TRAINING_TRIGGER_TIMEOUT_S)

    async with httpx.AsyncClient(timeout=timeout) as client:
        for attempt in range(1, 6):
            try:
                response = await client.post(TRAINING_TRIGGER_URL, json=payload)
                response.raise_for_status()
                data = response.json()
                print(
                    "[train-trigger] retraining completed "
                    f"rows={data.get('rows_used_for_training')} "
                    f"accuracy={data.get('accuracy')}",
                    flush=True,
                )
                return
            except (httpx.HTTPError, ValueError) as error:
                if attempt == 5:
                    raise RuntimeError(
                        f"Failed to trigger retraining via {TRAINING_TRIGGER_URL}"
                    ) from error

                sleep_seconds = min(2 ** (attempt - 1), 30)
                print(
                    "[train-trigger] failed "
                    f"attempt {attempt}/5: {error!r} sleep {sleep_seconds}s",
                    flush=True,
                )
                await asyncio.sleep(sleep_seconds)
