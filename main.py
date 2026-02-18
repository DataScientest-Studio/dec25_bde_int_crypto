from __future__ import annotations

from datetime import datetime, timezone


def date_range_to_ms_gte_lte(start_date: str, end_date: str) -> tuple[int, int]:
    start = datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end = datetime.strptime(end_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)

    gte_ms = int(start.timestamp() * 1000)
    lte_ms = int((end.timestamp() * 1000) + (24 * 60 * 60 * 1000) - 1)  # end-of-day inclusive
    return gte_ms, lte_ms

def main():
    print("Hello from dec25-bde-int-crypto!")
    # Example:
    gte_ms, lte_ms = date_range_to_ms_gte_lte("2026-01-01", "2026-01-31")
    print({"$gte": gte_ms, "$lte": lte_ms})


if __name__ == "__main__":
    main()
