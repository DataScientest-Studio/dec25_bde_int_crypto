"""Test Binance streaming with Kafka producer"""

import asyncio

from src.constants import SYMBOL, INTERVAL
from src.service.stream import BinanceWebSocketCollector


async def test_stream():
    """Test streaming with detailed logging"""
    print(f"Starting Binance WebSocket test for {SYMBOL} {INTERVAL}")
    print("Kafka enabled: True")
    print("Press Ctrl+C to stop\n")

    collector = BinanceWebSocketCollector(
        symbol=SYMBOL, interval=INTERVAL, enable_kafka=True
    )

    try:
        await collector.run()
    except KeyboardInterrupt:
        print("\nStopping...")
        await collector.disconnect()
        print("Stopped")


if __name__ == "__main__":
    asyncio.run(test_stream())
