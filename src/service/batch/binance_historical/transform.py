from __future__ import annotations

from typing import Any, List, Sequence

from src.models.models import HistoricalKline
from .common import MissingRange


def raw_row_is_in_missing_ranges(
    open_time_ms: int, missing_ranges: Sequence[MissingRange]
) -> bool:
    for missing_range in missing_ranges:
        if open_time_ms < missing_range.start_ms:
            return False

        if open_time_ms <= missing_range.end_ms:
            return True

    return False


def transform_raw_rows_in_missing_ranges(
    rows: Sequence[Sequence[Any]],
    *,
    symbol: str,
    interval: str,
    missing_ranges: List[MissingRange],
) -> List[HistoricalKline]:
    if not missing_ranges:
        return []

    print("[transform] start validation/mapping for missing ranges", flush=True)
    transformed_klines: List[HistoricalKline] = []
    skipped_rows = 0

    for raw_row in rows:
        open_time_ms = int(raw_row[0])

        if not raw_row_is_in_missing_ranges(open_time_ms, missing_ranges):
            continue

        try:
            transformed_klines.append(
                HistoricalKline.from_binance(
                    symbol=symbol, interval=interval, raw=raw_row
                )
            )
        except Exception as error:
            skipped_rows += 1
            print(f"[transform] skip open_time_ms={open_time_ms}: {error}", flush=True)

    print(
        f"[transform] done processed={len(transformed_klines)} skipped={skipped_rows}",
        flush=True,
    )
    return transformed_klines
