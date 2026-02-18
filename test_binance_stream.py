"""Test Binance streaming with Kafka producer"""

import asyncio
import sys
import os

# Add the project root to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.service.stream import BinanceWebSocketCollector
from src.constants import SYMBOL, INTERVAL

async def test_stream():
    """Test streaming with detailed logging"""
    print(f"Starting Binance WebSocket test for {SYMBOL} {INTERVAL}")
    print("Kafka enabled: True")
    print("Press Ctrl+C to stop\n")
    
    collector = BinanceWebSocketCollector(
        symbol=SYMBOL,
        interval=INTERVAL,
        enable_kafka=True
    )
    
    try:
        await collector.run()
    except KeyboardInterrupt:
        print("\nStopping...")
        await collector.disconnect()
        print("Stopped")

if __name__ == "__main__":
    asyncio.run(test_stream())
