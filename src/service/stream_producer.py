"""
WebSocket Stream Collector for real-time Binance price data.

This module connects to Binance WebSocket API to collect real-time
kline (candlestick) data for cryptocurrency trading pairs.
"""

import asyncio
import json
import logging
import os
import ssl
from datetime import datetime, timezone
from typing import Callable, Optional
import websockets
from quixstreams import Application
from src.models.models import KlineData
from src.mappers import KlineMapper
from src.constants import SYMBOL, INTERVAL, KAFKA_BROKER, KAFKA_TOPIC

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Binance WebSocket base URL
BINANCE_WS_BASE_URL = "wss://stream.binance.com:9443/ws"


class BinanceWebSocketCollector:
    """
    WebSocket collector for real-time Binance kline (candlestick) data.

    Connects to Binance WebSocket stream and processes real-time price updates.
    """

    def __init__(
        self,
        symbol: str = SYMBOL,
        interval: str = INTERVAL,
        callback: Optional[Callable[[KlineData], None]] = None,
        kafka_broker: str = KAFKA_BROKER,
        kafka_topic: str = KAFKA_TOPIC,
        enable_kafka: bool = True,
        mapper: KlineMapper = None
    ):
        """
        Initialize the WebSocket collector.

        Args:
            symbol: Trading pair symbol (e.g., 'BTCUSDT')
            interval: Kline interval (e.g., '1m', '5m', '1h', '1d')
            callback: Optional callback function to process received data
            kafka_broker: Kafka broker address (default: localhost:19092)
            kafka_topic: Kafka topic name (default: binance-klines)
            enable_kafka: Enable Kafka producer (default: True)
            mapper: KlineMapper instance for message conversion (default: creates new instance)
        """
        self.symbol = symbol.lower()
        self.interval = interval
        self.callback = callback
        self.ws_url = f"{BINANCE_WS_BASE_URL}/{self.symbol}@kline_{self.interval}"
        self.websocket = None
        self.running = False
        self.enable_kafka = enable_kafka
        self.mapper = mapper or KlineMapper()

        # Initialize QuixStreams application and producer
        if self.enable_kafka:
            try:
                logger.info(f"Initializing Kafka producer: broker={kafka_broker}, topic={kafka_topic}")
                self.app = Application(
                    broker_address=kafka_broker,
                    loglevel="DEBUG"
                )
                self.topic = self.app.topic(kafka_topic, value_serializer="json")
                self.producer = self.app.get_producer()
                logger.info(f"✓ Kafka producer initialized successfully")
                logger.info(f"  - Broker: {kafka_broker}")
                logger.info(f"  - Topic: {kafka_topic}")
                logger.info(f"  - Producer: {type(self.producer)}")
            except Exception as e:
                logger.error(f"Failed to initialize Kafka producer: {e}", exc_info=True)
                self.app = None
                self.topic = None
                self.producer = None
        else:
            self.app = None
            self.topic = None
            self.producer = None
            logger.info("Kafka producer disabled")

        # Build an explicit SSL context so cert handling is predictable across OS/Python builds.
        # Optionally allow a custom CA bundle (e.g., corporate proxy CA) via BINANCE_CA_FILE.
        self.ssl_context = ssl.create_default_context()
        # Disable certificate verification for self-signed certificates (e.g., corporate proxies)
        self.ssl_context.check_hostname = False
        self.ssl_context.verify_mode = ssl.CERT_NONE
        ca_file = os.getenv("BINANCE_CA_FILE")
        if ca_file:
            self.ssl_context.load_verify_locations(cafile=ca_file)

    async def connect(self):
        """Establish WebSocket connection."""
        try:
            # Only pass an SSL context when using a secure WebSocket URL (wss://).
            ssl_param = self.ssl_context if self.ws_url.startswith("wss://") else None

            self.websocket = await websockets.connect(self.ws_url, ssl=ssl_param)
            self.running = True
            logger.info(f"Connected to Binance WebSocket: {self.symbol}@kline_{self.interval}")
        except ssl.SSLCertVerificationError as e:
            logger.error(
                "TLS certificate verification failed while connecting to Binance.\n"
                "This usually means you're behind a TLS-intercepting proxy/AV, or Python's CA "
                "certificates are not installed correctly on this machine.\n"
                "Fix options:\n"
                "  - If on a corporate network: get the proxy/root CA bundle and set BINANCE_CA_FILE\n"
                "  - On macOS (python.org Python): run 'Install Certificates.command'\n"
                "  - Check if SSL_CERT_FILE/REQUESTS_CA_BUNDLE is set to an unexpected value\n"
                f"Underlying error: {e}"
            )
            raise
        except Exception as e:
            logger.error(f"Failed to connect to WebSocket: {e}")
            raise

    async def disconnect(self):
        """Close WebSocket connection and cleanup Kafka producer."""
        self.running = False
        if self.websocket:
            await self.websocket.close()
            logger.info("WebSocket connection closed")

        # Cleanup Kafka producer
        if self.producer:
            self.producer.flush()
            logger.info("Kafka producer flushed and closed")


    async def stream(self):
        """
        Stream real-time kline data from Binance WebSocket.

        Continuously receives and processes kline updates until stopped.
        """
        if not self.websocket:
            await self.connect()

        try:
            async for message in self.websocket:
                if not self.running:
                    break

                try:
                    data = json.loads(message)

                    # Use mapper to convert WebSocket message to structured models
                    result = self.mapper.websocket_to_kafka_message(data)

                    if result:
                        kline_message, kline, is_closed = result

                        # Log the received data
                        status = "CLOSED" if is_closed else "UPDATE"
                        logger.info(
                            f"[{status}] {kline.symbol} {kline.interval} | "
                            f"Time: {kline.timestamp} | "
                            f"O: {kline.open:.2f} H: {kline.high:.2f} "
                            f"L: {kline.low:.2f} C: {kline.close:.2f} | "
                            f"Vol: {kline.volume:.2f}"
                        )

                        # Publish to Kafka/Redpanda
                        logger.debug(f"Checking Kafka publish - producer: {self.producer is not None}, topic: {self.topic is not None}")
                        if self.producer is not None and self.topic is not None:
                            try:
                                logger.debug(f"Preparing message for Kafka...")

                                # Use symbol as message key for partitioning
                                message_key = kline.symbol

                                logger.debug(f"Serializing message: key={message_key}")
                                # Serialize and produce message to Kafka
                                serialized = self.topic.serialize(
                                    key=message_key,
                                    value=kline_message.model_dump()
                                )

                                logger.debug(f"Producing to topic: {self.topic.name}")
                                self.producer.produce(
                                    topic=self.topic.name,
                                    key=serialized.key,
                                    value=serialized.value
                                )

                                logger.debug(f"Flushing producer...")
                                # Flush immediately to ensure message is sent
                                self.producer.flush()

                                logger.info(f"✓ Published to Kafka: {kline.symbol} -> {status}")
                            except Exception as e:
                                logger.error(f"✗ Failed to publish to Kafka: {e}", exc_info=True)
                        else:
                            logger.warning(f"Kafka not available - producer: {self.producer is not None}, topic: {self.topic is not None}")

                        # Execute callback if provided
                        if self.callback:
                            self.callback(kline)

                except json.JSONDecodeError as e:
                    logger.error(f"Failed to decode message: {e}")
                except Exception as e:
                    logger.error(f"Error processing message: {e}")

        except websockets.exceptions.ConnectionClosed:
            logger.warning("WebSocket connection closed unexpectedly")
        except Exception as e:
            logger.error(f"Stream error: {e}")
        finally:
            await self.disconnect()

    async def run(self):
        """Run the WebSocket collector (connects and streams data)."""
        await self.connect()
        await self.stream()


async def main():
    """Example usage of the WebSocket collector."""

    def on_kline_received(kline: KlineData):
        """Callback function to handle received kline data."""
        # Example: save to database, send to message queue, etc.
        is_closed = getattr(kline, 'is_closed', False)
        if is_closed:
            logger.info(f"✅ Candle closed at {kline.timestamp}: Close price = {kline.close}")

    # Create collector instance
    collector = BinanceWebSocketCollector(
        symbol=SYMBOL,
        interval=INTERVAL,
        callback=on_kline_received
    )

    try:
        # Start streaming
        logger.info(f"Starting WebSocket stream for {SYMBOL} {INTERVAL}")
        await collector.run()
    except KeyboardInterrupt:
        logger.info("Stopping stream...")
        await collector.disconnect()


if __name__ == "__main__":
    # Run the WebSocket collector
    asyncio.run(main())
