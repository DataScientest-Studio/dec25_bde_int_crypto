from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from typing import Any, List, Tuple

import httpx

import src.constants as constants
import src.database as database
import src.models.models as model_types

from .binance_historical import common, extract, files, load, transform


# ---------------------------------------------------------------------------
# KlinePipeline
# ---------------------------------------------------------------------------


class KlinePipeline:
    """Historical Binance collector with explicit ETL stages.

    Extract:
    - read saved raw coverage
    - fetch only the missing Binance rows
    - save the merged raw dataset to disk

    Transform:
    - read saved processed coverage
    - validate and convert the missing raw rows
    - save the merged processed dataset to disk

    Load:
    - upsert processed rows into MongoDB
    - backfill MongoDB from processed files when the database is empty
    """

    def __init__(self, symbol: str, interval: str, start_ms: int, end_ms: int) -> None:
        self.symbol = symbol
        self.interval = interval
        self.start_ms = start_ms
        self.end_ms = end_ms
        self.paths = common.build_data_paths(symbol, interval)

    async def run(self) -> model_types.UpsertStats:
        files.ensure_output_directories(self.paths)

        print("[pipeline] stage=extract", flush=True)
        raw_rows = await self.extract_raw_data()

        print("[pipeline] stage=transform", flush=True)
        transformed_klines = self.transform_raw_data(raw_rows)

        print("[pipeline] stage=load", flush=True)
        mongo_load_batch = await self.prepare_mongo_load_rows(transformed_klines)
        mongo_stats = await self.load_to_mongo(mongo_load_batch)

        if load.retraining_is_needed(mongo_stats):
            await load.trigger_retraining(self.symbol, self.interval)
        else:
            print(
                "[train-trigger] skipped: no new historical rows in MongoDB",
                flush=True,
            )

        print("[pipeline] done", flush=True)
        return mongo_stats

    # ------------------------------------------------------------------
    # Extract stage
    # ------------------------------------------------------------------

    async def extract_raw_data(self) -> List[List[Any]]:
        existing_raw_rows = self.load_saved_raw_rows()
        raw_range_store = common.CoverageRangeStore(self.paths.raw_range)
        if self.paths.raw_range.exists() and not existing_raw_rows:
            print(
                "[raw] ignoring saved range metadata because raw json data is missing",
                flush=True,
            )
        covered_range = common.CoverageRangeStore.infer_range_from_rows(
            existing_raw_rows
        )
        missing_ranges = common.find_missing_ranges(
            self.start_ms, self.end_ms, covered_range
        )
        print(
            f"[raw] covered_range={covered_range} missing_ranges={missing_ranges}",
            flush=True,
        )

        new_raw_rows = await self.fetch_missing_raw_rows(missing_ranges)
        merged_raw_rows = files.merge_raw_rows(existing_raw_rows, new_raw_rows)

        if new_raw_rows or not self.paths.raw_json.exists():
            files.save_raw_dataset_files(merged_raw_rows, self.paths)
            if merged_raw_rows:
                raw_range_store.save(
                    int(merged_raw_rows[0][0]), int(merged_raw_rows[-1][0])
                )

        return merged_raw_rows

    async def fetch_missing_raw_rows(
        self, missing_ranges: List[common.MissingRange]
    ) -> List[List[Any]]:
        if not missing_ranges:
            return []
        rows: List[List[Any]] = []
        timeout = httpx.Timeout(15.0)
        async with httpx.AsyncClient(timeout=timeout) as client:
            for missing_range in missing_ranges:
                print(f"[raw] fetching {missing_range}", flush=True)
                rows.extend(
                    await extract.fetch_rows_for_missing_range(
                        client,
                        symbol=self.symbol,
                        interval=self.interval,
                        missing_range=missing_range,
                    )
                )
        return rows

    def load_saved_raw_rows(self) -> List[List[Any]]:
        if not self.paths.raw_json.exists():
            return []
        return json.loads(self.paths.raw_json.read_text())

    # ------------------------------------------------------------------
    # Transform stage
    # ------------------------------------------------------------------

    def transform_raw_data(
        self, raw_rows: List[List[Any]]
    ) -> List[model_types.HistoricalKline]:
        existing_processed_rows = self.load_saved_processed_rows()
        processed_range_store = common.CoverageRangeStore(self.paths.processed_range)
        if self.paths.processed_range.exists() and not existing_processed_rows:
            print(
                "[processed] ignoring saved range metadata because processed json data is missing",
                flush=True,
            )
        covered_range = common.CoverageRangeStore.infer_range_from_rows(
            [[row["open_time_ms"]] for row in existing_processed_rows]
        )
        missing_ranges = common.find_missing_ranges(
            self.start_ms, self.end_ms, covered_range
        )
        print(
            f"[processed] covered_range={covered_range} missing_ranges={missing_ranges}",
            flush=True,
        )

        transformed_klines = transform.transform_raw_rows_in_missing_ranges(
            raw_rows,
            symbol=self.symbol,
            interval=self.interval,
            missing_ranges=missing_ranges,
        )
        new_processed_rows = [kline.to_processed_row() for kline in transformed_klines]

        if new_processed_rows or not self.paths.processed_json.exists():
            merged_processed_rows = files.merge_processed_rows(
                existing_processed_rows, new_processed_rows
            )
            files.save_processed_dataset_files(merged_processed_rows, self.paths)
            if merged_processed_rows:
                processed_range_store.save(
                    int(merged_processed_rows[0]["open_time_ms"]),
                    int(merged_processed_rows[-1]["open_time_ms"]),
                )

        return transformed_klines

    def load_saved_processed_rows(self) -> List[dict]:
        if not self.paths.processed_json.exists():
            return []
        return json.loads(self.paths.processed_json.read_text())

    # ------------------------------------------------------------------
    # Load stage
    # ------------------------------------------------------------------

    async def prepare_mongo_load_rows(
        self, transformed_klines: List[model_types.HistoricalKline]
    ) -> List[model_types.HistoricalKline]:
        """Backfill Mongo from processed files if the DB is empty for this slice."""
        if not await self.mongo_needs_backfill():
            return transformed_klines

        processed_rows = self.load_saved_processed_rows()
        if not processed_rows:
            return transformed_klines

        # This keeps the pipeline recoverable after Mongo resets: if files already
        # exist on disk, we can rebuild the historical collection without refetching
        # the entire history from Binance.
        print(
            f"[mongo] empty collection for {self.symbol} {self.interval} -> backfill from processed file",
            flush=True,
        )
        processed_models = [
            model_types.HistoricalKline(**row) for row in processed_rows
        ]
        if not transformed_klines:
            return processed_models

        return files.merge_historical_klines(processed_models, transformed_klines)

    async def mongo_needs_backfill(self) -> bool:
        async for client in database.get_historical_mongo_client():
            collection = client.get_collection()
            # We only need one document to know whether this symbol/interval slice
            # already exists in MongoDB.
            existing_doc = await collection.find_one(
                {"symbol": self.symbol, "interval": self.interval},
                {"_id": 1},
            )
            return existing_doc is None
        return False

    async def load_to_mongo(
        self, models: List[model_types.HistoricalKline]
    ) -> model_types.UpsertStats:
        async for client in database.get_historical_mongo_client():
            return await load.load_klines_into_mongo(models, client)
        return model_types.UpsertStats(0, 0, 0, 0)


# ---------------------------------------------------------------------------
# Entry points
# ---------------------------------------------------------------------------


def build_requested_time_range() -> Tuple[int, int]:
    start_ms = common.parse_date_to_unix_ms(constants.START_DATE)
    end_ms = (
        common.parse_date_to_unix_ms(constants.END_DATE)
        if constants.END_DATE
        else int(datetime.now(tz=timezone.utc).timestamp() * 1000)
    )
    if start_ms >= end_ms:
        raise ValueError("START_DATE must be < END_DATE")
    return start_ms, end_ms


async def run_pipeline() -> None:
    if constants.INTERVAL not in model_types.SUPPORTED_INTERVALS:
        raise ValueError(
            f"Interval must be one of {sorted(model_types.SUPPORTED_INTERVALS)}"
        )

    symbol = constants.SYMBOL.strip().upper()
    start_ms, end_ms = build_requested_time_range()

    pipeline = KlinePipeline(
        symbol=symbol,
        interval=constants.INTERVAL,
        start_ms=start_ms,
        end_ms=end_ms,
    )
    await pipeline.run()


def main() -> None:
    asyncio.run(run_pipeline())


if __name__ == "__main__":
    main()
