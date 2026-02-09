"""
Redpanda Consumer for Binance Kline Messages.

This module consumes real-time kline data from Redpanda using QuixStreams
and persists the messages to MongoDB.
"""

import logging
from pymongo import MongoClient, errors
from pymongo.collection import Collection
from quixstreams import Application
from src.constants import KAFKA_BROKER, KAFKA_TOPIC, MONGODB_URI, MONGODB_DATABASE, MONGODB_COLLECTION
from src.models.models import KlineMessage

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class BinanceKlineConsumer:
    """
    Consumer for Binance kline messages from Redpanda.

    Consumes messages from the binance-klines topic and persists them to MongoDB.
    """

    def __init__(
        self,
        broker_address: str = KAFKA_BROKER,
        topic: str = KAFKA_TOPIC,
        consumer_group: str = "binance-consumer-group",
        mongodb_uri: str = MONGODB_URI,
        db_name: str = MONGODB_DATABASE,
        collection_name: str = MONGODB_COLLECTION
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
        self.broker_address = broker_address
        self.topic_name = topic
        self.consumer_group = consumer_group

        # Initialize QuixStreams application
        self.app = Application(
            broker_address=broker_address,
            consumer_group=consumer_group,
            auto_offset_reset="earliest",  # Start from beginning if no offset
            loglevel="INFO"
        )

        self.topic = self.app.topic(topic, value_deserializer="json")
        logger.info(
            f"Consumer initialized: broker={broker_address}, "
            f"topic={topic}, group={consumer_group}"
        )

        # Initialize MongoDB connection
        try:
            self.mongo_client = MongoClient(mongodb_uri)
            self.db = self.mongo_client[db_name]
            self.collection: Collection = self.db[collection_name]

            # Create indexes for better query performance
            # Unique index on event_time to ensure each message is stored only once
            self.collection.create_index([("event_time", 1)], unique=True)
            self.collection.create_index([("symbol", 1), ("interval", 1), ("timestamp", -1)])
            self.collection.create_index([("event_timestamp", -1)])
            self.collection.create_index([("timestamp", -1)])
            self.collection.create_index([("is_closed", 1)])

            logger.info(
                f"MongoDB connected: uri={mongodb_uri}, "
                f"db={db_name}, collection={collection_name}"
            )
        except errors.ConnectionError as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            raise

    def process_message(self, message_value: dict):
        """
        Process a kline message: validate, persist to MongoDB, and print.

        Args:
            message_value: Message value (kline data dict)
        """
        try:
            # Validate message using Pydantic model
            kline_msg = KlineMessage(**message_value)

            # Persist ALL messages to MongoDB (no upsert - insert each message)
            try:
                mongo_doc = kline_msg.to_mongo_doc()

                # Insert the document - unique index on event_time prevents duplicates
                result = self.collection.insert_one(mongo_doc)

                logger.debug(
                    f"Inserted message: {kline_msg.symbol} @ {kline_msg.timestamp} "
                    f"(event_time={kline_msg.event_time}, _id={result.inserted_id})"
                )

            except errors.DuplicateKeyError:
                # Message already exists (duplicate event_time) - skip silently
                logger.debug(f"Duplicate message skipped: event_time={kline_msg.event_time}")
            except errors.PyMongoError as e:
                logger.error(f"MongoDB error: {e}")

            # Print to console
            status = "CLOSED" if kline_msg.is_closed else "UPDATE"
            print(
                f"[{status}] {kline_msg.symbol} {kline_msg.interval} | "
                f"Time: {kline_msg.timestamp} | "
                f"O: {kline_msg.open:.2f} H: {kline_msg.high:.2f} "
                f"L: {kline_msg.low:.2f} C: {kline_msg.close:.2f} | "
                f"Vol: {kline_msg.volume:.2f}"
            )

        except Exception as e:
            logger.error(f"Error processing message: {e}")
            logger.error(f"Message value: {message_value}")

    def cleanup(self):
        """Clean up MongoDB connection."""
        if hasattr(self, 'mongo_client'):
            self.mongo_client.close()
            logger.info("MongoDB connection closed")

    def run(self):
        """
        Run the consumer and process messages from Redpanda.

        This method blocks and continuously processes messages.
        """
        logger.info(f"Starting consumer for topic: {self.topic_name}")

        # Create a streaming dataframe
        sdf = self.app.dataframe(self.topic)

        # Process each message - apply function to each row
        sdf = sdf.update(lambda row: self.process_message(row))

        # Run the application
        try:
            logger.info("Consumer is running... Press Ctrl+C to stop")
            self.app.run(sdf)
        except KeyboardInterrupt:
            logger.info("Consumer stopped by user")
        except Exception as e:
            logger.error(f"Consumer error: {e}")
            raise
        finally:
            self.cleanup()


def main():
    """Main entry point for the consumer."""
    consumer = BinanceKlineConsumer()
    consumer.run()


if __name__ == "__main__":
    main()
