from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, List, Optional, Sequence, Tuple

from src.config.data_settings import get_settings as get_data_settings
from src.models.models import DataPaths


@dataclass(frozen=True)
class MissingRange:
    start_ms: int
    end_ms: int


class CoverageRangeStore:
    def __init__(self, path: Path) -> None:
        self.path = path

    def load(
        self, fallback_rows: Optional[Sequence[Sequence[Any]]] = None
    ) -> Optional[Tuple[int, int]]:
        """Load saved coverage and infer it from rows when needed."""
        if self.path.exists():
            data = json.loads(self.path.read_text())
            return int(data["start_ms"]), int(data["end_ms"])

        if fallback_rows:
            return self.infer_range_from_rows(fallback_rows)

        return None

    def save(self, start_ms: int, end_ms: int) -> None:
        self.path.write_text(
            json.dumps({"start_ms": start_ms, "end_ms": end_ms}, indent=2)
        )
        print(f"[range] saved {self.path} -> ({start_ms}, {end_ms})", flush=True)

    @staticmethod
    def infer_range_from_rows(
        rows: Sequence[Sequence[Any]],
    ) -> Optional[Tuple[int, int]]:
        if not rows:
            return None

        return int(rows[0][0]), int(rows[-1][0])


def find_missing_ranges(
    request_start_ms: int,
    request_end_ms: int,
    covered_range: Optional[Tuple[int, int]],
) -> List[MissingRange]:
    if covered_range is None:
        return [MissingRange(request_start_ms, request_end_ms)]

    covered_start_ms, covered_end_ms = covered_range
    missing_ranges: List[MissingRange] = []

    if request_start_ms < covered_start_ms:
        missing_ranges.append(
            MissingRange(request_start_ms, min(request_end_ms, covered_start_ms - 1))
        )

    if request_end_ms > covered_end_ms:
        missing_ranges.append(
            MissingRange(max(request_start_ms, covered_end_ms + 1), request_end_ms)
        )

    return [
        missing_range
        for missing_range in missing_ranges
        if missing_range.start_ms <= missing_range.end_ms
    ]


def build_data_paths(symbol: str, interval: str) -> DataPaths:
    data_settings = get_data_settings()
    base_dir = Path(data_settings.data_dir)
    raw_dir = base_dir / data_settings.raw_data_dirname
    processed_dir = base_dir / data_settings.processed_data_dirname
    file_stem = f"{symbol}_{interval}"

    return DataPaths(
        raw_dir=raw_dir,
        processed_dir=processed_dir,
        raw_json=raw_dir / f"{file_stem}.json",
        raw_csv=raw_dir / f"{file_stem}.csv",
        raw_range=raw_dir / f"{file_stem}.range.json",
        processed_json=processed_dir / f"{file_stem}.json",
        processed_csv=processed_dir / f"{file_stem}.csv",
        processed_range=processed_dir / f"{file_stem}.range.json",
    )


def parse_date_to_unix_ms(value: str) -> int:
    cleaned_value = value.strip()

    try:
        date_value = datetime.strptime(cleaned_value, "%Y-%m-%d").replace(
            tzinfo=timezone.utc
        )
        return int(date_value.timestamp() * 1000)
    except ValueError:
        pass

    iso_value = cleaned_value.replace("Z", "+00:00")
    date_value = datetime.fromisoformat(iso_value)

    if date_value.tzinfo is None:
        date_value = date_value.replace(tzinfo=timezone.utc)

    return int(date_value.astimezone(timezone.utc).timestamp() * 1000)
