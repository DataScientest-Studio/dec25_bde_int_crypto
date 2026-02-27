from __future__ import annotations

import asyncio
import csv
import io
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, List, Optional, Sequence, Tuple

import httpx

from src.config.data_settings import get_settings as get_data_settings
from src.constants import (
    BASE_URL,
    END_DATE,
    INTERVAL,
    MAX_LIMIT,
    PAGE_SLEEP_S,
    START_DATE,
    SYMBOL,
    RAW_CSV_HEADER,
    PROCESSED_CSV_HEADER,
)
from src.models.models import HistoricalKline, SUPPORTED_INTERVALS, DataPaths
from src.database import get_historical_mongo_client, MongoClient
from src.database.mongo_repository import AsyncKlineStore


# ---------------------------------------------------------------------------
# Logging helper
# ---------------------------------------------------------------------------


def print_step(msg: str) -> None:
    print(msg, flush=True)


# ---------------------------------------------------------------------------
# Gap — replaces bare Tuple[int, int] throughout the codebase
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Gap:
    start_ms: int
    end_ms: int

    def contains(self, ts: int) -> bool:
        return self.start_ms <= ts <= self.end_ms

    def __repr__(self) -> str:
        return f"Gap({self.start_ms}, {self.end_ms})"


# ---------------------------------------------------------------------------
# RangeStore — consolidates all range read/write/infer logic
# ---------------------------------------------------------------------------


class RangeStore:
    def __init__(self, path: Path) -> None:
        self._path = path

    def load(
        self, fallback_rows: Optional[Sequence[Sequence[Any]]] = None
    ) -> Optional[Tuple[int, int]]:
        """Load range from file; fall back to inferring from rows if absent."""
        if self._path.exists():
            data = json.loads(self._path.read_text())
            return int(data["start_ms"]), int(data["end_ms"])
        if fallback_rows:
            return self._infer(fallback_rows)
        return None

    def save(self, start_ms: int, end_ms: int) -> None:
        self._path.write_text(
            json.dumps({"start_ms": start_ms, "end_ms": end_ms}, indent=2)
        )
        print_step(f"[range] saved {self._path} -> ({start_ms}, {end_ms})")

    @staticmethod
    def _infer(rows: Sequence[Sequence[Any]]) -> Optional[Tuple[int, int]]:
        if not rows:
            return None
        return int(rows[0][0]), int(rows[-1][0])


# ---------------------------------------------------------------------------
# Gap computation
# ---------------------------------------------------------------------------


def compute_gaps(
    request_start: int, request_end: int, have: Optional[Tuple[int, int]]
) -> List[Gap]:
    """Return the gaps between what we have and what we need."""
    if have is None:
        return [Gap(request_start, request_end)]

    have_start, have_end = have
    gaps: List[Gap] = []

    if request_start < have_start:
        gaps.append(Gap(request_start, min(request_end, have_start - 1)))

    if request_end > have_end:
        gaps.append(Gap(max(request_start, have_end + 1), request_end))

    return [g for g in gaps if g.start_ms <= g.end_ms]


# ---------------------------------------------------------------------------
# Pure serialization helpers (no I/O — fully testable)
# ---------------------------------------------------------------------------


def serialize_raw_json(rows: Sequence[Sequence[Any]]) -> str:
    return json.dumps(rows, separators=(",", ":"))


def serialize_raw_csv(rows: Sequence[Sequence[Any]]) -> str:
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(RAW_CSV_HEADER)
    for r in rows:
        w.writerow(list(r) + [""] * max(0, len(RAW_CSV_HEADER) - len(r)))
    return buf.getvalue()


def serialize_processed_json(rows: Sequence[dict]) -> str:
    return json.dumps(rows, separators=(",", ":"))


def serialize_processed_csv(rows: Sequence[dict]) -> str:
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=PROCESSED_CSV_HEADER)
    w.writeheader()
    w.writerows(rows)
    return buf.getvalue()


# ---------------------------------------------------------------------------
# File writers (thin I/O wrappers around serializers)
# ---------------------------------------------------------------------------


def ensure_dirs(paths: DataPaths) -> None:
    paths.raw_dir.mkdir(parents=True, exist_ok=True)
    paths.processed_dir.mkdir(parents=True, exist_ok=True)


def write_raw_files(rows: Sequence[Sequence[Any]], paths: DataPaths) -> None:
    print_step(f"[raw] saving json -> {paths.raw_json}")
    paths.raw_json.write_text(serialize_raw_json(rows))

    print_step(f"[raw] saving csv  -> {paths.raw_csv}")
    paths.raw_csv.write_text(serialize_raw_csv(rows))


def write_processed_files(rows: Sequence[dict], paths: DataPaths) -> None:
    print_step(f"[processed] saving json -> {paths.processed_json}")
    paths.processed_json.write_text(serialize_processed_json(rows))

    print_step(f"[processed] saving csv  -> {paths.processed_csv}")
    paths.processed_csv.write_text(serialize_processed_csv(rows))


# ---------------------------------------------------------------------------
# Merge helpers
# ---------------------------------------------------------------------------


def merge_raw(existing: List[List[Any]], new_rows: List[List[Any]]) -> List[List[Any]]:
    by_open: dict[int, List[Any]] = {}
    for r in (existing or []) + (new_rows or []):
        by_open[int(r[0])] = list(r)
    return [by_open[k] for k in sorted(by_open)]


def merge_processed(existing: List[dict], new_rows: List[dict]) -> List[dict]:
    by_open: dict[int, dict] = {}
    for r in existing:
        by_open[int(r["open_time_ms"])] = r
    for r in new_rows:
        by_open[int(r["open_time_ms"])] = r
    return [by_open[k] for k in sorted(by_open)]


# ---------------------------------------------------------------------------
# HTTP fetch helpers (client is now injected)
# ---------------------------------------------------------------------------


async def fetch_page(
    client: httpx.AsyncClient,
    *,
    symbol: str,
    interval: str,
    start_ms: int,
    end_ms: Optional[int],
    limit: int,
) -> List[List[Any]]:
    params: dict[str, Any] = {
        "symbol": symbol,
        "interval": interval,
        "startTime": start_ms,
        "limit": limit,
    }
    if end_ms is not None:
        params["endTime"] = end_ms

    for attempt in range(1, 7):
        try:
            resp = await client.get(BASE_URL, params=params)

            if resp.status_code in (418, 429):
                retry_after = resp.headers.get("Retry-After")
                sleep_s = float(retry_after) if retry_after else min(2**attempt, 30.0)
                print_step(
                    f"[fetch] rate limited (status={resp.status_code}) sleeping {sleep_s}s"
                )
                await asyncio.sleep(sleep_s)
                continue

            if 500 <= resp.status_code < 600:
                raise httpx.HTTPStatusError(
                    "server error", request=resp.request, response=resp
                )

            resp.raise_for_status()
            data = resp.json()
            if not isinstance(data, list):
                raise ValueError(f"Unexpected response type: {type(data)}")
            return data

        except (
            httpx.TimeoutException,
            httpx.TransportError,
            httpx.HTTPStatusError,
            ValueError,
        ) as e:
            sleep_s = min(0.5 * (2 ** (attempt - 1)), 10.0)
            print_step(f"[fetch] failed attempt {attempt}/6: {e!r} sleep {sleep_s}s")
            await asyncio.sleep(sleep_s)

    raise RuntimeError("Failed to fetch klines after retries")


async def fetch_gap(
    client: httpx.AsyncClient,
    *,
    symbol: str,
    interval: str,
    gap: Gap,
    limit: int = MAX_LIMIT,
) -> List[List[Any]]:
    """Fetch all klines for a single Gap with pagination."""
    rows: List[List[Any]] = []
    next_start = gap.start_ms
    page = 0

    while True:
        page += 1
        print_step(f"[fetch] page={page} start_ms={next_start} gap={gap}")

        data = await fetch_page(
            client,
            symbol=symbol,
            interval=interval,
            start_ms=next_start,
            end_ms=gap.end_ms,
            limit=min(limit, MAX_LIMIT),
        )

        if not data:
            print_step("[fetch] no more rows")
            break

        rows.extend(data)
        last_open = int(data[-1][0])
        next_start = last_open + 1

        if len(data) < min(limit, MAX_LIMIT):
            print_step("[fetch] last page (returned < limit)")
            break

        await asyncio.sleep(PAGE_SLEEP_S)

    print_step(f"[fetch] done fetched_rows={len(rows)} for {gap}")
    return rows


# ---------------------------------------------------------------------------
# Processing
# ---------------------------------------------------------------------------


def preprocess_gaps(
    rows: Sequence[Sequence[Any]],
    *,
    symbol: str,
    interval: str,
    gaps: List[Gap],
) -> List[HistoricalKline]:
    """Validate and map only rows that fall inside one of the gaps."""
    if not gaps:
        return []

    # Sort gaps so we can short-circuit the inner loop
    sorted_gaps = sorted(gaps, key=lambda g: g.start_ms)

    def in_gaps(open_ms: int) -> bool:
        for g in sorted_gaps:
            if g.start_ms > open_ms:
                break
            if open_ms <= g.end_ms:
                return True
        return False

    print_step("[process] start validation/mapping for gaps")
    out: List[HistoricalKline] = []
    skipped = 0

    for r in rows:
        open_ms = int(r[0])
        if not in_gaps(open_ms):
            continue
        try:
            out.append(
                HistoricalKline.from_binance(symbol=symbol, interval=interval, raw=r)
            )
        except Exception as e:
            skipped += 1
            print_step(f"[process] skip open_time_ms={open_ms}: {e}")

    print_step(f"[process] done processed={len(out)} skipped={skipped}")
    return out


# ---------------------------------------------------------------------------
# Mongo upsert
# ---------------------------------------------------------------------------


async def upsert_to_mongo(klines: List[HistoricalKline], client: MongoClient) -> None:
    if not klines:
        print_step("[mongo] nothing to upsert")
        return

    store = AsyncKlineStore(client)
    await store.initialize()
    try:
        stats = await store.upsert_many(klines)
        print_step(
            f"[mongo] upsert done requested={stats.requested} matched={stats.matched} "
            f"modified={stats.modified} upserted={stats.upserted}"
        )
    finally:
        await store.close()


# ---------------------------------------------------------------------------
# Path builder
# ---------------------------------------------------------------------------


def build_paths(symbol: str, interval: str) -> DataPaths:
    data_settings = get_data_settings()
    base = Path(data_settings.data_dir)
    raw_dir = base / data_settings.raw_data_dirname
    processed_dir = base / data_settings.processed_data_dirname
    stem = f"{symbol}_{interval}"
    return DataPaths(
        raw_dir=raw_dir,
        processed_dir=processed_dir,
        raw_json=raw_dir / f"{stem}.json",
        raw_csv=raw_dir / f"{stem}.csv",
        raw_range=raw_dir / f"{stem}.range.json",
        processed_json=processed_dir / f"{stem}.json",
        processed_csv=processed_dir / f"{stem}.csv",
        processed_range=processed_dir / f"{stem}.range.json",
    )


def to_unix_ms(value: str) -> int:
    value = value.strip()
    try:
        dt = datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    except ValueError:
        pass
    iso = value.replace("Z", "+00:00")
    dt = datetime.fromisoformat(iso)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.astimezone(timezone.utc).timestamp() * 1000)


# ---------------------------------------------------------------------------
# KlinePipeline — owns orchestration; each phase is a private method
# ---------------------------------------------------------------------------


class KlinePipeline:
    def __init__(self, symbol: str, interval: str, start_ms: int, end_ms: int) -> None:
        self.symbol = symbol
        self.interval = interval
        self.start_ms = start_ms
        self.end_ms = end_ms
        self.paths = build_paths(symbol, interval)

    async def run(self) -> None:
        ensure_dirs(self.paths)
        merged_raw = await self._sync_raw()
        new_models = self._sync_processed(merged_raw)
        await self._sync_mongo(new_models)
        print_step("[pipeline] done")

    # ------------------------------------------------------------------
    # Phase 1 — raw fetch
    # ------------------------------------------------------------------

    async def _sync_raw(self) -> List[List[Any]]:
        existing = self._load_raw()
        raw_range_store = RangeStore(self.paths.raw_range)
        have = raw_range_store.load(fallback_rows=existing)
        gaps = compute_gaps(self.start_ms, self.end_ms, have)
        print_step(f"[raw] have={have} gaps={gaps}")

        new_rows = await self._fetch_gaps(gaps)
        merged = merge_raw(existing, new_rows)

        if new_rows or not self.paths.raw_json.exists():
            write_raw_files(merged, self.paths)
            if merged:
                raw_range_store.save(int(merged[0][0]), int(merged[-1][0]))

        return merged

    async def _fetch_gaps(self, gaps: List[Gap]) -> List[List[Any]]:
        if not gaps:
            return []
        rows: List[List[Any]] = []
        timeout = httpx.Timeout(15.0)
        async with httpx.AsyncClient(timeout=timeout) as client:
            for gap in gaps:
                print_step(f"[raw] fetching {gap}")
                rows.extend(
                    await fetch_gap(
                        client, symbol=self.symbol, interval=self.interval, gap=gap
                    )
                )
        return rows

    def _load_raw(self) -> List[List[Any]]:
        if not self.paths.raw_json.exists():
            return []
        return json.loads(self.paths.raw_json.read_text())

    # ------------------------------------------------------------------
    # Phase 2 — process
    # ------------------------------------------------------------------

    def _sync_processed(self, merged_raw: List[List[Any]]) -> List[HistoricalKline]:
        # Carry existing processed data in memory — no redundant disk read needed
        # unless the file predates this run.
        existing = self._load_processed()
        proc_range_store = RangeStore(self.paths.processed_range)
        have = proc_range_store.load(
            fallback_rows=[[r["open_time_ms"]] for r in existing] if existing else None
        )
        gaps = compute_gaps(self.start_ms, self.end_ms, have)
        print_step(f"[processed] have={have} gaps={gaps}")

        new_models = preprocess_gaps(
            merged_raw, symbol=self.symbol, interval=self.interval, gaps=gaps
        )
        new_rows = [k.to_processed_row() for k in new_models]

        if new_rows or not self.paths.processed_json.exists():
            merged = merge_processed(existing, new_rows)
            write_processed_files(merged, self.paths)
            if merged:
                proc_range_store.save(
                    int(merged[0]["open_time_ms"]),
                    int(merged[-1]["open_time_ms"]),
                )

        return new_models

    def _load_processed(self) -> List[dict]:
        if not self.paths.processed_json.exists():
            return []
        return json.loads(self.paths.processed_json.read_text())

    # ------------------------------------------------------------------
    # Phase 3 — mongo
    # ------------------------------------------------------------------

    async def _sync_mongo(self, models: List[HistoricalKline]) -> None:
        async for client in get_historical_mongo_client():
            await upsert_to_mongo(models, client)


# ---------------------------------------------------------------------------
# Entry points
# ---------------------------------------------------------------------------


def _pick_request_range() -> Tuple[int, int]:
    start_ms = to_unix_ms(START_DATE)
    end_ms = (
        to_unix_ms(END_DATE)
        if END_DATE
        else int(datetime.now(tz=timezone.utc).timestamp() * 1000)
    )
    if start_ms >= end_ms:
        raise ValueError("START_DATE must be < END_DATE")
    return start_ms, end_ms


async def run_pipeline() -> None:
    if INTERVAL not in SUPPORTED_INTERVALS:
        raise ValueError(f"Interval must be one of {sorted(SUPPORTED_INTERVALS)}")

    symbol = SYMBOL.strip().upper()
    start_ms, end_ms = _pick_request_range()

    pipeline = KlinePipeline(
        symbol=symbol, interval=INTERVAL, start_ms=start_ms, end_ms=end_ms
    )
    await pipeline.run()


def main() -> None:
    asyncio.run(run_pipeline())


if __name__ == "__main__":
    main()
