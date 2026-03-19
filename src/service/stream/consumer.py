"""
Redpanda Consumer for Binance Kline Messages.

This module consumes real-time kline data from Redpanda using QuixStreams
and persists the messages to MongoDB using Motor (async driver).
"""

import asyncio
import logging
import threading
from typing import Optional
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection
from pymongo import errors
from src.config.kafka_settings import get_settings as get_kafka_settings
from src.config.mongo_settings import get_settings as get_mongo_settings
from src.models.models import KlineMessage
from src.service.kafka_client import KafkaConfig, KafkaConsumerClient

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class BinanceKlineConsumer:
    """
    Consumer for Binance kline messages from Redpanda.

    Consumes messages from the binance-klines topic and persists them to MongoDB
    using Motor (async driver) with proper connection pooling and error handling.
    """

    def __init__(
        self,
        broker_address: Optional[str] = None,
        topic: Optional[str] = None,
        consumer_group: str = "binance-consumer-group",
        mongodb_uri: Optional[str] = None,
        db_name: Optional[str] = None,
        collection_name: Optional[str] = None,
    ):
        """
        Initialize the Redpanda consumer with MongoDB persistence.

        Args:
            broker_address: Kafka broker address (default: localhost:19092)
            topic: Kafka topic name (default: binance-klines)
            consumer_group: Consumer group ID for offset management
            mongodb_uri: MongoDB connection URI
            db_name: MongoDB database name
            collection_name: MongoDB collection name
        """
        kafka_settings = get_kafka_settings()
        mongo_settings = get_mongo_settings()

        self.mongodb_uri = mongodb_uri or mongo_settings.mongodb_uri
        self.db_name = db_name or mongo_settings.mongodb_database
        self.collection_name = (
            collection_name or mongo_settings.mongodb_collection_streaming
        )

        # Motor client (async) - will be initialized in async context
        self.mongo_client: Optional[AsyncIOMotorClient] = None
        self.collection: Optional[AsyncIOMotorCollection] = None
        self._indexes_created = False

        # Event loop for async operations (runs in background thread)
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._loop_thread: Optional[threading.Thread] = None

        # Initialize Kafka consumer using abstraction
        kafka_config = KafkaConfig(
            broker_address=broker_address or kafka_settings.kafka_broker,
            topic=topic or kafka_settings.kafka_topic,
            consumer_group=consumer_group,
            auto_offset_reset="earliest",
            loglevel="INFO",
        )
        self.kafka_client = KafkaConsumerClient(kafka_config)
        if not self.kafka_client.connect():
            raise RuntimeError(
                "Kafka consumer initialization failed; "
                "the streaming consumer cannot run without Redpanda."
            )

        logger.info(
            f"Consumer initialized: broker={kafka_config.broker_address}, "
            f"topic={kafka_config.topic}, group={consumer_group}"
        )

    async def _init_mongo(self):
        """Initialize Motor client and ensure indexes (async)."""
        if self.mongo_client is not None:
            return

        try:
            self.mongo_client = AsyncIOMotorClient(self.mongodb_uri)
            db = self.mongo_client[self.db_name]
            self.collection = db[self.collection_name]

            # Verify connection by pinging the server
            await self.mongo_client.admin.command("ping")

            logger.info(
                f"MongoDB connected: uri={self.mongodb_uri}, "
                f"db={self.db_name}, collection={self.collection_name}"
            )

            # Create indexes for better query performance
            await self._ensure_indexes()

        except Exception as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            raise

    async def _ensure_indexes(self):
        """Create indexes for better query performance."""
        if self._indexes_created or self.collection is None:
            return

        try:
            # Drop the legacy event_time-only unique index so multiple symbols or
            # intervals can coexist even when Binance emits the same event_time.
            index_info = await self.collection.index_information()
            legacy_index = index_info.get("event_time_1")
            if legacy_index and legacy_index.get("unique"):
                await self.collection.drop_index("event_time_1")
                logger.info("Dropped legacy MongoDB index: event_time_1")

            await self.collection.create_index(
                [("symbol", 1), ("interval", 1), ("event_time", 1)],
                unique=True,
                name="uniq_symbol_interval_event_time",
            )
            await self.collection.create_index(
                [("symbol", 1), ("interval", 1), ("timestamp", -1)]
            )
            await self.collection.create_index([("event_timestamp", -1)])
            await self.collection.create_index([("timestamp", -1)])
            await self.collection.create_index([("is_closed", 1)])

            self._indexes_created = True
            logger.info("MongoDB indexes created successfully")
        except Exception as e:
            logger.warning(f"Error creating indexes: {e}")

    def process_message(self, message_value: dict):
        """
        Process a kline message: validate, persist to MongoDB, and print.

        This is a synchronous wrapper that schedules async MongoDB operations.
        Args:
            message_value: Message value (kline data dict)
        """
        try:
            # Validate message using Pydantic model
            kline_msg = KlineMessage(**message_value)

            # Schedule async MongoDB insert in background event loop
            if self._loop is not None:
                asyncio.run_coroutine_threadsafe(
                    self._async_insert(kline_msg), self._loop
                )

            # Emit one log line per consumed message so container logs stay useful.
            status = "CLOSED" if kline_msg.is_closed else "UPDATE"
            logger.info(
                "[%s] %s %s | Time: %s | O: %.2f H: %.2f L: %.2f C: %.2f | Vol: %.2f",
                status,
                kline_msg.symbol,
                kline_msg.interval,
                kline_msg.timestamp,
                kline_msg.open,
                kline_msg.high,
                kline_msg.low,
                kline_msg.close,
                kline_msg.volume,
            )

        except Exception as e:
            logger.error(f"Error processing message: {e}")
            logger.error(f"Message value: {message_value}")

    async def _async_insert(self, kline_msg: KlineMessage):
        """
        Async insert operation to MongoDB with proper error handling.

        Args:
            kline_msg: Validated kline message
        """
        if self.collection is None:
            logger.error("MongoDB collection not initialized")
            return

        try:
            mongo_doc = kline_msg.to_mongo_doc()

            # The composite unique index keeps replays idempotent per stream.
            result = await self.collection.insert_one(mongo_doc)

            logger.debug(
                f"Inserted message: {kline_msg.symbol} @ {kline_msg.timestamp} "
                f"(event_time={kline_msg.event_time}, _id={result.inserted_id})"
            )

        except errors.DuplicateKeyError:
            # Message already exists for this symbol/interval/event_time - skip silently
            logger.debug(
                "Duplicate message skipped: "
                f"symbol={kline_msg.symbol}, interval={kline_msg.interval}, "
                f"event_time={kline_msg.event_time}"
            )
        except Exception as e:
            logger.error(f"MongoDB insert error: {e}")

    async def _cleanup(self):
        """Clean up MongoDB connection (async)."""
        if self.mongo_client is not None:
            self.mongo_client.close()
            logger.info("MongoDB connection closed")

    def _start_event_loop(self):
        """Start event loop in background thread for async operations."""
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)

        # Initialize MongoDB in this loop
        self._loop.run_until_complete(self._init_mongo())

        # Run the loop forever
        self._loop.run_forever()

    def _stop_event_loop(self):
        """Stop background event loop and cleanup."""
        if self._loop is not None:
            # Schedule cleanup
            asyncio.run_coroutine_threadsafe(self._cleanup(), self._loop)

            # Stop the loop
            self._loop.call_soon_threadsafe(self._loop.stop)

            # Wait for thread to finish
            if self._loop_thread is not None:
                self._loop_thread.join(timeout=5)

    def run(self):
        """
        Run the consumer and process messages from Redpanda.

        This method blocks and continuously processes messages.
        Sets up async event loop for MongoDB operations in background thread.
        """
        # Start event loop in background thread
        self._loop_thread = threading.Thread(target=self._start_event_loop, daemon=True)
        self._loop_thread.start()

        # Wait for MongoDB to initialize
        import time

        time.sleep(1)

        # Get Kafka application and topic from client
        app = self.kafka_client.get_application()
        topic = self.kafka_client.get_topic()

        if not app or not topic:
            logger.error("Kafka client not properly initialized")
            return

        logger.info(f"Starting consumer for topic: {self.kafka_client.config.topic}")

        # Create a streaming dataframe
        sdf = app.dataframe(topic)

        # Process each message - apply function to each row
        sdf = sdf.update(lambda row: self.process_message(row))

        # Run the application
        try:
            logger.info("Consumer is running... Press Ctrl+C to stop")
            app.run(sdf)
        except KeyboardInterrupt:
            logger.info("Consumer stopped by user")
        except Exception as e:
            logger.error(f"Consumer error: {e}")
            raise
        finally:
            # Cleanup
            self._stop_event_loop()


def main():
    """Main entry point for the consumer."""
    consumer = BinanceKlineConsumer()
    consumer.run()


if __name__ == "__main__":
    main()
