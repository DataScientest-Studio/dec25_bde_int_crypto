from __future__ import annotations

import asyncio
from typing import Any, List, Optional

import httpx

from src.constants import BASE_URL, MAX_LIMIT, PAGE_SLEEP_S
from .common import MissingRange


async def fetch_kline_page(
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
            response = await client.get(BASE_URL, params=params)

            if response.status_code in (418, 429):
                retry_after = response.headers.get("Retry-After")
                sleep_seconds = (
                    float(retry_after) if retry_after else min(2**attempt, 30.0)
                )
                print(
                    "[fetch] rate limited "
                    f"(status={response.status_code}) sleeping {sleep_seconds}s",
                    flush=True,
                )
                await asyncio.sleep(sleep_seconds)
                continue

            if 500 <= response.status_code < 600:
                raise httpx.HTTPStatusError(
                    "server error", request=response.request, response=response
                )

            response.raise_for_status()
            data = response.json()

            if not isinstance(data, list):
                raise ValueError(f"Unexpected response type: {type(data)}")

            return data
        except (
            httpx.TimeoutException,
            httpx.TransportError,
            httpx.HTTPStatusError,
            ValueError,
        ) as error:
            sleep_seconds = min(0.5 * (2 ** (attempt - 1)), 10.0)
            print(
                f"[fetch] failed attempt {attempt}/6: {error!r} sleep {sleep_seconds}s",
                flush=True,
            )
            await asyncio.sleep(sleep_seconds)

    raise RuntimeError("Failed to fetch klines after retries")


async def fetch_rows_for_missing_range(
    client: httpx.AsyncClient,
    *,
    symbol: str,
    interval: str,
    missing_range: MissingRange,
    limit: int = MAX_LIMIT,
) -> List[List[Any]]:
    rows: List[List[Any]] = []
    next_start_ms = missing_range.start_ms
    page_number = 0

    while True:
        page_number += 1
        print(
            "[fetch] "
            f"page={page_number} start_ms={next_start_ms} missing_range={missing_range}",
            flush=True,
        )

        data = await fetch_kline_page(
            client,
            symbol=symbol,
            interval=interval,
            start_ms=next_start_ms,
            end_ms=missing_range.end_ms,
            limit=min(limit, MAX_LIMIT),
        )

        if not data:
            print("[fetch] no more rows", flush=True)
            break

        rows.extend(data)
        last_open_time_ms = int(data[-1][0])
        next_start_ms = last_open_time_ms + 1

        if len(data) < min(limit, MAX_LIMIT):
            print("[fetch] last page (returned < limit)", flush=True)
            break

        await asyncio.sleep(PAGE_SLEEP_S)

    print(f"[fetch] done fetched_rows={len(rows)} for {missing_range}", flush=True)
    return rows
