from __future__ import annotations

import csv
import io
import json
from typing import Any, List, Sequence

from src.constants import PROCESSED_CSV_HEADER, RAW_CSV_HEADER
from src.models.models import DataPaths, HistoricalKline


def serialize_raw_json(rows: Sequence[Sequence[Any]]) -> str:
    return json.dumps(rows, separators=(",", ":"))


def serialize_raw_csv(rows: Sequence[Sequence[Any]]) -> str:
    csv_buffer = io.StringIO()
    writer = csv.writer(csv_buffer)
    writer.writerow(RAW_CSV_HEADER)

    for row in rows:
        writer.writerow(list(row) + [""] * max(0, len(RAW_CSV_HEADER) - len(row)))

    return csv_buffer.getvalue()


def serialize_processed_json(rows: Sequence[dict]) -> str:
    return json.dumps(rows, separators=(",", ":"))


def serialize_processed_csv(rows: Sequence[dict]) -> str:
    csv_buffer = io.StringIO()
    writer = csv.DictWriter(csv_buffer, fieldnames=PROCESSED_CSV_HEADER)
    writer.writeheader()
    writer.writerows(rows)
    return csv_buffer.getvalue()


def ensure_output_directories(paths: DataPaths) -> None:
    paths.raw_dir.mkdir(parents=True, exist_ok=True)
    paths.processed_dir.mkdir(parents=True, exist_ok=True)


def save_raw_dataset_files(rows: Sequence[Sequence[Any]], paths: DataPaths) -> None:
    print(f"[raw] saving json -> {paths.raw_json}", flush=True)
    paths.raw_json.write_text(serialize_raw_json(rows))

    print(f"[raw] saving csv  -> {paths.raw_csv}", flush=True)
    paths.raw_csv.write_text(serialize_raw_csv(rows))


def save_processed_dataset_files(rows: Sequence[dict], paths: DataPaths) -> None:
    print(f"[processed] saving json -> {paths.processed_json}", flush=True)
    paths.processed_json.write_text(serialize_processed_json(rows))

    print(f"[processed] saving csv  -> {paths.processed_csv}", flush=True)
    paths.processed_csv.write_text(serialize_processed_csv(rows))


def merge_raw_rows(
    existing_rows: List[List[Any]], new_rows: List[List[Any]]
) -> List[List[Any]]:
    rows_by_open_time: dict[int, List[Any]] = {}

    for row in (existing_rows or []) + (new_rows or []):
        rows_by_open_time[int(row[0])] = list(row)

    return [rows_by_open_time[open_time] for open_time in sorted(rows_by_open_time)]


def merge_processed_rows(existing_rows: List[dict], new_rows: List[dict]) -> List[dict]:
    rows_by_open_time: dict[int, dict] = {}

    for row in existing_rows:
        rows_by_open_time[int(row["open_time_ms"])] = row

    for row in new_rows:
        rows_by_open_time[int(row["open_time_ms"])] = row

    return [rows_by_open_time[open_time] for open_time in sorted(rows_by_open_time)]


def merge_historical_klines(
    existing_klines: List[HistoricalKline], new_klines: List[HistoricalKline]
) -> List[HistoricalKline]:
    klines_by_open_time: dict[int, HistoricalKline] = {}

    for kline in existing_klines:
        klines_by_open_time[kline.open_time_ms] = kline

    for kline in new_klines:
        klines_by_open_time[kline.open_time_ms] = kline

    return [klines_by_open_time[open_time] for open_time in sorted(klines_by_open_time)]
