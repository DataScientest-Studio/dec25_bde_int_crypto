"""
WebSocket Stream Collector for real-time Binance price data.

This module connects to Binance WebSocket API to collect real-time
kline (candlestick) data for cryptocurrency trading pairs.

Refactored to separate concerns:
- WebSocket connection management (websocket_manager)
- Message processing logic (message_processor)
- Kafka publishing (kafka_client)
"""

import asyncio
import logging
from typing import Callable, Optional
import websockets
from src.models.models import KlineData
from src.mappers import KlineMapper
from src.constants import SYMBOL, INTERVAL, KAFKA_BROKER, KAFKA_TOPIC
from src.service.kafka_client import KafkaConfig, KafkaProducerClient
from src.service.stream.message_processor import MessageProcessor, MessageValidationError
from src.service.stream.websocket_manager import WebSocketManager, WebSocketConnectionError

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class BinanceWebSocketCollector:
    """
    WebSocket collector for real-time Binance kline (candlestick) data.

    Orchestrates WebSocket connection, message processing, and Kafka publishing.
    Business logic is delegated to specialized components for testability.
    """

    def __init__(
        self,
        symbol: str = SYMBOL,
        interval: str = INTERVAL,
        callback: Optional[Callable[[KlineData], None]] = None,
        kafka_broker: str = KAFKA_BROKER,
        kafka_topic: str = KAFKA_TOPIC,
        enable_kafka: bool = True,
        mapper: Optional[KlineMapper] = None,
        ws_base_url: str = "wss://stream.binance.com:9443/ws"
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
            mapper: KlineMapper instance for message conversion
            ws_base_url: WebSocket base URL (for testing)
        """
        self.symbol = symbol
        self.interval = interval
        self.callback = callback
        self.running = False

        # Initialize message processor (pure business logic)
        self.message_processor = MessageProcessor(mapper=mapper)

        # Initialize WebSocket manager (connection handling)
        self.ws_manager = WebSocketManager(base_url=ws_base_url, ssl_verify=False)
        self.ws_url = self.ws_manager.build_stream_url(symbol, interval)

        # Initialize Kafka producer using abstraction
        self.enable_kafka = enable_kafka
        if self.enable_kafka:
            kafka_config = KafkaConfig(
                broker_address=kafka_broker,
                topic=kafka_topic,
                loglevel="DEBUG"
            )
            self.kafka_client = KafkaProducerClient(kafka_config)
            if not self.kafka_client.connect():
                logger.warning("Kafka producer initialization failed, will continue without Kafka")
                self.kafka_client = None
        else:
            self.kafka_client = None
            logger.info("Kafka producer disabled")

    async def connect(self):
        """Establish WebSocket connection."""
        try:
            await self.ws_manager.connect(self.ws_url)
            self.running = True
            logger.info(f"Connected to Binance WebSocket: {self.symbol}@kline_{self.interval}")
        except WebSocketConnectionError as e:
            logger.error(f"Failed to connect: {e}")
            raise

    async def disconnect(self):
        """Close WebSocket connection and cleanup Kafka producer."""
        self.running = False
        await self.ws_manager.disconnect()

        # Cleanup Kafka producer
        if self.kafka_client:
            self.kafka_client.close()
            logger.info("Kafka producer flushed and closed")


    async def stream(self):
        """
        Stream real-time kline data from Binance WebSocket.

        Continuously receives and processes kline updates until stopped.
        """
        if not self.ws_manager.is_connected():
            await self.connect()

        try:
            while self.running:
                try:
                    # Receive raw message
                    raw_message = await self.ws_manager.receive_message()

                    # Parse message
                    message_data = self.message_processor.parse_websocket_message(raw_message)

                    # Process message to domain models
                    result = self.message_processor.process_message(message_data)

                    if result:
                        kline_message, kline, is_closed = result

                        # Format and log
                        log_msg = self.message_processor.format_kline_log(kline, is_closed)
                        logger.info(log_msg)

                        # Publish to Kafka if enabled
                        if self.kafka_client is not None:
                            await self._publish_to_kafka(kline, kline_message, is_closed)

                        # Execute callback if provided
                        if self.callback:
                            self.callback(kline)

                except MessageValidationError as e:
                    logger.error(f"Message validation error: {e}")
                except WebSocketConnectionError as e:
                    logger.error(f"WebSocket error: {e}")
                    break
                except Exception as e:
                    logger.error(f"Error processing message: {e}", exc_info=True)

        except Exception as e:
            logger.error(f"Stream error: {e}", exc_info=True)
        finally:
            await self.disconnect()

    async def _publish_to_kafka(self, kline: KlineData, kline_message, is_closed: bool):
        """
        Publish message to Kafka (helper method).

        Args:
            kline: KlineData instance
            kline_message: KlineMessage instance
            is_closed: Whether candle is closed
        """
        try:
            # Prepare message using business logic
            message_key, message_value = self.message_processor.prepare_kafka_message(
                kline, kline_message
            )

            # Publish
            if self.kafka_client.produce(key=message_key, value=message_value, flush=True):
                status = "CLOSED" if is_closed else "UPDATE"
                logger.info(f"✓ Published to Kafka: {kline.symbol} -> {status}")
            else:
                logger.error(f"✗ Failed to publish to Kafka: {kline.symbol}")

        except Exception as e:
            logger.error(f"✗ Failed to publish to Kafka: {e}", exc_info=True)

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
