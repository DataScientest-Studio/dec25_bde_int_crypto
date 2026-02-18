"""
Kafka/Redpanda Client Abstraction Layer.

This module provides a unified abstraction for Kafka/Redpanda connections,
used by both producer and consumer implementations.
"""

import logging
from dataclasses import dataclass
from typing import Optional
from quixstreams import Application

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class KafkaConfig:
    """
    Kafka/Redpanda connection configuration.

    Attributes:
        broker_address: Kafka broker address (e.g., 'localhost:19092')
        topic: Topic name (e.g., 'binance-klines')
        consumer_group: Consumer group ID (only for consumers)
        auto_offset_reset: Offset reset strategy ('earliest' or 'latest')
        loglevel: Logging level for QuixStreams
    """
    broker_address: str
    topic: str
    consumer_group: Optional[str] = None
    auto_offset_reset: str = "earliest"
    loglevel: str = "INFO"

    def __post_init__(self):
        """Validate configuration."""
        if not self.broker_address:
            raise ValueError("broker_address is required")
        if not self.topic:
            raise ValueError("topic is required")
        if self.auto_offset_reset not in ("earliest", "latest"):
            raise ValueError("auto_offset_reset must be 'earliest' or 'latest'")


class KafkaProducerClient:
    """
    Kafka producer client wrapper.

    Provides simplified interface for producing messages to Kafka/Redpanda.
    """

    def __init__(self, config: KafkaConfig):
        """
        Initialize Kafka producer.

        Args:
            config: KafkaConfig instance with connection settings
        """
        self.config = config
        self.app: Optional[Application] = None
        self.topic = None
        self._producer_ctx = None
        self.producer = None

    def connect(self):
        """Initialize QuixStreams application and producer."""
        try:
            logger.info(
                f"Initializing Kafka producer: broker={self.config.broker_address}, "
                f"topic={self.config.topic}"
            )

            self.app = Application(
                broker_address=self.config.broker_address,
                loglevel=self.config.loglevel
            )

            self.topic = self.app.topic(self.config.topic, value_serializer="json")
            self._producer_ctx = self.app.get_producer()
            self.producer = self._producer_ctx.__enter__()

            logger.info("Kafka producer initialized successfully")
            return True

        except Exception as e:
            logger.error(f"Failed to initialize Kafka producer: {e}", exc_info=True)
            self.app = None
            self.topic = None
            self._producer_ctx = None
            self.producer = None
            return False

    def produce(self, key: str, value: dict, flush: bool = True):
        """
        Produce a message to Kafka.

        Args:
            key: Message key (used for partitioning)
            value: Message value (dict, will be JSON serialized)
            flush: Whether to flush immediately (default: True)

        Returns:
            bool: True if successful, False otherwise
        """
        if self.producer is None or self.topic is None:
            logger.error("Producer not initialized")
            return False

        try:
            # Serialize message
            serialized = self.topic.serialize(key=key, value=value)

            # Produce to Kafka
            self.producer.produce(
                topic=self.topic.name,
                key=serialized.key,
                value=serialized.value
            )

            # Flush if requested
            if flush:
                self.producer.flush()

            logger.debug(f"Produced message: key={key}")
            return True

        except Exception as e:
            logger.error(f"Failed to produce message: {e}", exc_info=True)
            return False

    def flush(self):
        """Flush producer buffer."""
        if self.producer:
            self.producer.flush()

    def close(self):
        """Close producer and cleanup resources."""
        if self._producer_ctx is not None:
            try:
                self._producer_ctx.__exit__(None, None, None)
                logger.info("Kafka producer flushed and closed")
            except Exception as e:
                logger.warning(f"Error closing Kafka producer: {e}")
            finally:
                self._producer_ctx = None
                self.producer = None


class KafkaConsumerClient:
    """
    Kafka consumer client wrapper.

    Provides simplified interface for consuming messages from Kafka/Redpanda.
    """

    def __init__(self, config: KafkaConfig):
        """
        Initialize Kafka consumer.

        Args:
            config: KafkaConfig instance with connection settings

        Raises:
            ValueError: If consumer_group is not set in config
        """
        if not config.consumer_group:
            raise ValueError("consumer_group is required for consumers")

        self.config = config
        self.app: Optional[Application] = None
        self.topic = None

    def connect(self):
        """Initialize QuixStreams application for consumer."""
        try:
            logger.info(
                f"Initializing Kafka consumer: broker={self.config.broker_address}, "
                f"topic={self.config.topic}, group={self.config.consumer_group}"
            )

            self.app = Application(
                broker_address=self.config.broker_address,
                consumer_group=self.config.consumer_group,
                auto_offset_reset=self.config.auto_offset_reset,
                loglevel=self.config.loglevel
            )

            self.topic = self.app.topic(self.config.topic, value_deserializer="json")

            logger.info("Kafka consumer initialized successfully")
            return True

        except Exception as e:
            logger.error(f"Failed to initialize Kafka consumer: {e}", exc_info=True)
            self.app = None
            self.topic = None
            return False

    def get_application(self) -> Optional[Application]:
        """
        Get QuixStreams Application instance.

        Returns:
            Application instance or None if not connected
        """
        return self.app

    def get_topic(self):
        """
        Get configured topic.

        Returns:
            Topic instance or None if not connected
        """
        return self.topic
