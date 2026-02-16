from __future__ import annotations

import asyncio
import csv
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, List, Optional, Sequence, Tuple

import httpx

from src.constants import (
    BASE_URL,
    DATA_DIR,
    END_DATE,
    INTERVAL,
    MAX_LIMIT,
    MONGODB_COLLECTION,
    MONGODB_DATABASE,
    MONGODB_URI,
    PAGE_SLEEP_S,
    PROCESSED_DATA_DIRNAME,
    RAW_DATA_DIRNAME,
    START_DATE,
    SYMBOL,
)
from src.models.models import HistoricalKline, SUPPORTED_INTERVALS
from src.service.mongo_repository import AsyncKlineStore


@dataclass(frozen=True)
class DataPaths:
    raw_dir: Path
    processed_dir: Path
    raw_json: Path
    raw_csv: Path
    raw_range: Path
    processed_json: Path
    processed_csv: Path
    processed_range: Path


def print_step(msg: str) -> None:
    # Plain prints make it easy to follow control flow during long backfills.
    print(msg, flush=True)


def ensure_dirs(paths: DataPaths) -> None:
    # We keep raw and processed outputs separate:
    # - raw data is the exact Binance response (audit/debug/reprocess)
    # - processed data is the validated schema we use downstream
    paths.raw_dir.mkdir(parents=True, exist_ok=True)
    paths.processed_dir.mkdir(parents=True, exist_ok=True)


def build_paths(symbol: str, interval: str) -> DataPaths:
    base = Path(DATA_DIR)
    raw_dir = base / RAW_DATA_DIRNAME
    processed_dir = base / PROCESSED_DATA_DIRNAME
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
    """Safe compact converter:
    - supports YYYY-MM-DD (UTC midnight)
    - supports ISO strings with Z or offsets
    - treats naive ISO as UTC
    """
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


def read_range(path: Path) -> Optional[Tuple[int, int]]:
    if not path.exists():
        return None
    data = json.loads(path.read_text())
    return int(data["start_ms"]), int(data["end_ms"])


def write_range(path: Path, start_ms: int, end_ms: int) -> None:
    path.write_text(json.dumps({"start_ms": start_ms, "end_ms": end_ms}, indent=2))


def infer_range_from_raw(rows: Sequence[Sequence[Any]]) -> Optional[Tuple[int, int]]:
    if not rows:
        return None
    return int(rows[0][0]), int(rows[-1][0])


def compute_missing(request_start: int, request_end: int, have: Optional[Tuple[int, int]]) -> List[Tuple[int, int]]:
    """Compute missing open_time_ms ranges we need to fetch/process.

    We assume existing data is a single continuous range [have_start, have_end] (inclusive).
    In here (one symbol, one interval) that's the assumption made for the moment.
    """
    if have is None:
        return [(request_start, request_end)]

    have_start, have_end = have
    missing: List[Tuple[int, int]] = []

    if request_start < have_start:
        missing.append((request_start, min(request_end, have_start - 1)))

    if request_end > have_end:
        missing.append((max(request_start, have_end + 1), request_end))

    return [(a, b) for a, b in missing if a <= b]


async def fetch_page(
    client: httpx.AsyncClient,
    *,
    symbol: str,
    interval: str,
    start_ms: int,
    end_ms: Optional[int],
    limit: int,
) -> List[List[Any]]:
    params: dict[str, Any] = {"symbol": symbol, "interval": interval, "startTime": start_ms, "limit": limit}
    if end_ms is not None:
        params["endTime"] = end_ms

    retries = 6
    for attempt in range(1, retries + 1):
        try:
            resp = await client.get(BASE_URL, params=params)

            if resp.status_code in (418, 429):
                retry_after = resp.headers.get("Retry-After")
                sleep_s = float(retry_after) if retry_after else min(2**attempt, 30.0)
                print_step(f"[fetch] rate limited (status={resp.status_code}) sleeping {sleep_s}s")
                await asyncio.sleep(sleep_s)
                continue

            if 500 <= resp.status_code < 600:
                raise httpx.HTTPStatusError("server error", request=resp.request, response=resp)

            resp.raise_for_status()
            data = resp.json()
            if not isinstance(data, list):
                raise ValueError(f"Unexpected response type: {type(data)}")
            return data

        except (httpx.TimeoutException, httpx.TransportError, httpx.HTTPStatusError, ValueError) as e:
            sleep_s = min(0.5 * (2 ** (attempt - 1)), 10.0)
            print_step(f"[fetch] failed attempt {attempt}/{retries}: {e!r} sleep {sleep_s}s")
            await asyncio.sleep(sleep_s)

    raise RuntimeError("Failed to fetch klines after retries")


async def fetch_range(
    *,
    symbol: str,
    interval: str,
    start_ms: int,
    end_ms: int,
    limit: int = MAX_LIMIT,
) -> List[List[Any]]:
    """Fetch klines for [start_ms, end_ms] (inclusive) with pagination.

    Why sequential pagination:
    - next page depends on last open_time_ms
    - keeps logic simple and avoids hammering the API
    """
    rows: List[List[Any]] = []
    timeout = httpx.Timeout(15.0)
    async with httpx.AsyncClient(timeout=timeout) as client:
        page = 0
        next_start = start_ms

        while True:
            page += 1
            print_step(f"[fetch] page={page} start_ms={next_start}")

            data = await fetch_page(
                client,
                symbol=symbol,
                interval=interval,
                start_ms=next_start,
                end_ms=end_ms,
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

    print_step(f"[fetch] done fetched_rows={len(rows)} for range [{start_ms}, {end_ms}]")
    return rows


def read_raw_json(path: Path) -> List[List[Any]]:
    if not path.exists():
        return []
    return json.loads(path.read_text())


def write_raw_files(rows: Sequence[Sequence[Any]], paths: DataPaths) -> None:
    print_step(f"[raw] saving json -> {paths.raw_json}")
    paths.raw_json.write_text(json.dumps(rows, separators=(",", ":")))

    print_step(f"[raw] saving csv  -> {paths.raw_csv}")
    header = [
        "open_time_ms",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "close_time_ms",
        "quote_volume",
        "trade_count",
        "taker_buy_base_volume",
        "taker_buy_quote_volume",
        "ignore",
    ]
    with paths.raw_csv.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        for r in rows:
            w.writerow(list(r) + ([""] * (len(header) - len(r))))


def merge_raw(existing: List[List[Any]], new_rows: List[List[Any]]) -> List[List[Any]]:
    """Merge raw pages into a single deduplicated, sorted list by open_time_ms."""
    merged = (existing or []) + (new_rows or [])

    by_open: dict[int, List[Any]] = {}
    for r in merged:
        by_open[int(r[0])] = list(r)

    return [by_open[k] for k in sorted(by_open.keys())]


def preprocess_missing(
    rows: Sequence[Sequence[Any]],
    *,
    symbol: str,
    interval: str,
    missing: List[Tuple[int, int]],
) -> List[HistoricalKline]:
    """Validate/map only rows in missing ranges.

    Why:
    - makes runs incremental
    - only new data gets validated + written
    """
    if not missing:
        return []

    def in_missing(open_ms: int) -> bool:
        return any(a <= open_ms <= b for a, b in missing)

    print_step("[process] start validation/mapping for missing ranges")
    out: List[HistoricalKline] = []
    skipped = 0

    for r in rows:
        open_ms = int(r[0])
        if not in_missing(open_ms):
            continue
        try:
            out.append(HistoricalKline.from_binance(symbol=symbol, interval=interval, raw=r))
        except Exception as e:
            skipped += 1
            print_step(f"[process] skip open_time_ms={open_ms}: {e}")

    print_step(f"[process] done processed={len(out)} skipped={skipped}")
    return out


def read_processed_json(path: Path) -> List[dict]:
    if not path.exists():
        return []
    return json.loads(path.read_text())


def merge_processed(existing: List[dict], new_rows: List[dict]) -> List[dict]:
    by_open: dict[int, dict] = {}
    for r in existing:
        by_open[int(r["open_time_ms"])] = r
    for r in new_rows:
        by_open[int(r["open_time_ms"])] = r
    return [by_open[k] for k in sorted(by_open.keys())]


def write_processed_files(processed_rows: List[dict], paths: DataPaths) -> None:
    print_step(f"[processed] saving json -> {paths.processed_json}")
    paths.processed_json.write_text(json.dumps(processed_rows, separators=(",", ":")))

    print_step(f"[processed] saving csv  -> {paths.processed_csv}")
    header = [
        "symbol",
        "interval",
        "open_time_ms",
        "close_time_ms",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "quote_volume",
        "trade_count",
        "taker_buy_base_volume",
        "taker_buy_quote_volume",
    ]
    with paths.processed_csv.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=header)
        w.writeheader()
        for r in processed_rows:
            w.writerow(r)


async def upsert_missing_to_mongo(klines: List[HistoricalKline]) -> None:
    if not klines:
        print_step("[mongo] nothing to upsert")
        return

    store = AsyncKlineStore(uri=MONGODB_URI, database=MONGODB_DATABASE, collection=MONGODB_COLLECTION)
    await store.ensure_indexes()
    try:
        stats = await store.upsert_many(klines)
        print_step(
            f"[mongo] upsert done requested={stats.requested} matched={stats.matched} "
            f"modified={stats.modified} upserted={stats.upserted}"
        )
    finally:
        await store.close()


def pick_request_range() -> Tuple[int, int]:
    # We keep the "requested range" explicit so we can compare it against what we already downloaded.
    start_ms = to_unix_ms(START_DATE)
    end_ms = to_unix_ms(END_DATE) if END_DATE else int(datetime.now(tz=timezone.utc).timestamp() * 1000)
    if start_ms >= end_ms:
        raise ValueError("START_DATE must be < END_DATE")
    return start_ms, end_ms


async def run() -> None:
    if INTERVAL not in SUPPORTED_INTERVALS:
        raise ValueError(f"Interval must be one of {sorted(SUPPORTED_INTERVALS)}")

    symbol = SYMBOL.strip().upper()
    interval = INTERVAL

    paths = build_paths(symbol, interval)
    ensure_dirs(paths)

    request_start, request_end = pick_request_range()
    print_step(f"[main] request range start_ms={request_start} end_ms={request_end}")

    # --- RAW ---
    existing_raw = read_raw_json(paths.raw_json)
    existing_raw_range = read_range(paths.raw_range) or infer_range_from_raw(existing_raw)

    raw_missing = compute_missing(request_start, request_end, existing_raw_range)
    print_step(f"[main] raw have={existing_raw_range} missing={raw_missing}")

    new_raw: List[List[Any]] = []
    for a, b in raw_missing:
        print_step(f"[main] fetching missing raw range [{a}, {b}]")
        new_raw.extend(await fetch_range(symbol=symbol, interval=interval, start_ms=a, end_ms=b))

    merged_raw = merge_raw(existing_raw, new_raw)

    if new_raw or not paths.raw_json.exists():
        write_raw_files(merged_raw, paths)
        inferred = infer_range_from_raw(merged_raw)
        if inferred:
            write_range(paths.raw_range, inferred[0], inferred[1])
            print_step(f"[main] updated raw range -> {inferred}")

    # --- PROCESSED ---
    existing_processed = read_processed_json(paths.processed_json)
    existing_processed_range = read_range(paths.processed_range)
    if existing_processed_range is None and existing_processed:
        existing_processed_range = (
            int(existing_processed[0]["open_time_ms"]),
            int(existing_processed[-1]["open_time_ms"]),
        )

    processed_missing = compute_missing(request_start, request_end, existing_processed_range)
    print_step(f"[main] processed have={existing_processed_range} missing={processed_missing}")

    new_models = preprocess_missing(merged_raw, symbol=symbol, interval=interval, missing=processed_missing)
    new_rows = [k.to_processed_row() for k in new_models]

    if new_rows or not paths.processed_json.exists():
        merged_processed = merge_processed(existing_processed, new_rows)
        write_processed_files(merged_processed, paths)

        if merged_processed:
            write_range(
                paths.processed_range,
                int(merged_processed[0]["open_time_ms"]),
                int(merged_processed[-1]["open_time_ms"]),
            )
            print_step(
                f"[main] updated processed range -> "
                f"({merged_processed[0]['open_time_ms']}, {merged_processed[-1]['open_time_ms']})"
            )

    # --- MONGO ---
    await upsert_missing_to_mongo(new_models)

    print_step("[main] done")


def main() -> None:
    asyncio.run(run())


if __name__ == "__main__":
    main()
