import requests
import time
import pandas as pd
from datetime import datetime, timezone
from typing import List, Dict
from src.constants import (
    BASE_URL,
    MAX_LIMIT,
    RATE_LIMIT_SLEEP,
    SYMBOL,
    INTERVAL,
    START_DATE,
    END_DATE,
    DATA_DIR
)


def fetch_klines(
    symbol: str,
    interval: str,
    start_time_ms: int,
    end_time_ms: int | None = None
) -> List[List]:
    """
    Fetch all historical klines from Binance (free public API)
    """
    all_klines = []

    while True:
        params = {
            "symbol": symbol,
            "interval": interval,
            "startTime": start_time_ms,
            "limit": MAX_LIMIT
        }

        if end_time_ms:
            params["endTime"] = end_time_ms

        response = requests.get(BASE_URL, params=params, timeout=10)
        response.raise_for_status()
        klines = response.json()

        if not klines:
            break

        all_klines.extend(klines)

        # Move to next candle
        start_time_ms = klines[-1][0] + 1

        # Stop if less than max returned
        if len(klines) < MAX_LIMIT:
            break

        time.sleep(RATE_LIMIT_SLEEP)

    return all_klines


def transform_klines(
    klines: List[List],
    symbol: str,
    interval: str
) -> pd.DataFrame:
    """
    Transform raw Binance klines into clean DataFrame
    """
    data: List[Dict] = []

    for k in klines:
        data.append({
            "symbol": symbol,
            "interval": interval,
            "timestamp": datetime.fromtimestamp(k[0] / 1000, tz=timezone.utc),
            "open": float(k[1]),
            "high": float(k[2]),
            "low": float(k[3]),
            "close": float(k[4]),
            "volume": float(k[5]),
            "quote_volume": float(k[7]),
            "trade_count": int(k[8])
        })

    df = pd.DataFrame(data)
    df.sort_values("timestamp", inplace=True)
    return df


def save_data(df: pd.DataFrame, symbol: str, interval: str):
    """
    Save data to CSV and JSON in data folder
    """
    base_name = f"{symbol}_{interval}"

    csv_path = f"{DATA_DIR}/{base_name}.csv"
    json_path = f"{DATA_DIR}/{base_name}.json"

    df.to_csv(csv_path, index=False)
    df.to_json(json_path, orient="records", date_format="iso")

    print(f"Saved {len(df)} rows")
    print(f"→ {csv_path}")
    print(f"→ {json_path}")


def to_unix_ms(date_str: str) -> int:
    """
    Convert YYYY-MM-DD to Unix timestamp (ms)
    """
    return int(
        datetime.strptime(date_str, "%Y-%m-%d")
        .replace(tzinfo=timezone.utc)
        .timestamp() * 1000
    )


if __name__ == "__main__":
    print("Starting Binance historical data collection")
    print(f"Symbol   : {SYMBOL}")
    print(f"Interval : {INTERVAL}")
    print(f"From     : {START_DATE}")
    print(f"To       : {END_DATE or 'NOW'}")

    start_ms = to_unix_ms(START_DATE)
    end_ms = to_unix_ms(END_DATE) if END_DATE else None

    raw_klines = fetch_klines(
        symbol=SYMBOL,
        interval=INTERVAL,
        start_time_ms=start_ms,
        end_time_ms=end_ms
    )

    df = transform_klines(
        klines=raw_klines,
        symbol=SYMBOL,
        interval=INTERVAL
    )

    save_data(df, SYMBOL, INTERVAL)

    print("Done ✅")
