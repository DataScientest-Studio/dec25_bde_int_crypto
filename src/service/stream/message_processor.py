"""
Message Processor for Binance WebSocket Data.

This module contains the core business logic for processing WebSocket messages,
separated from I/O operations to enable unit testing.
"""

import json
import logging
from typing import Optional, Tuple, Dict, Any
from src.models.models import KlineData, KlineMessage
from src.mappers import KlineMapper

logger = logging.getLogger(__name__)


class MessageValidationError(Exception):
    """Raised when message validation fails."""
    pass


class MessageProcessor:
    """
    Processes and validates Binance WebSocket kline messages.

    This class contains pure business logic without I/O dependencies,
    making it easily testable.
    """

    def __init__(self, mapper: Optional[KlineMapper] = None):
        """
        Initialize message processor.

        Args:
            mapper: KlineMapper instance for message conversion
        """
        self.mapper = mapper or KlineMapper()

    def parse_websocket_message(self, raw_message: str) -> Dict[str, Any]:
        """
        Parse raw WebSocket message string to dict.

        Args:
            raw_message: Raw JSON string from WebSocket

        Returns:
            Parsed message as dict

        Raises:
            MessageValidationError: If JSON parsing fails
        """
        try:
            return json.loads(raw_message)
        except json.JSONDecodeError as e:
            raise MessageValidationError(f"Failed to decode JSON message: {e}") from e

    def process_message(
        self,
        message_data: Dict[str, Any]
    ) -> Optional[Tuple[KlineMessage, KlineData, bool]]:
        """
        Process parsed WebSocket message and convert to domain models.

        This is the core business logic that:
        1. Validates the message structure
        2. Converts to domain models (KlineMessage, KlineData)
        3. Extracts metadata (is_closed flag)

        Args:
            message_data: Parsed WebSocket message dict

        Returns:
            Tuple of (KlineMessage, KlineData, is_closed) or None if invalid

        Raises:
            MessageValidationError: If message processing fails
        """
        try:
            return self.mapper.websocket_to_kafka_message(message_data)
        except Exception as e:
            raise MessageValidationError(f"Failed to process message: {e}") from e

    def format_kline_log(
        self,
        kline: KlineData,
        is_closed: bool
    ) -> str:
        """
        Format kline data for logging/display.

        Args:
            kline: KlineData instance
            is_closed: Whether the kline/candle is closed

        Returns:
            Formatted string for logging
        """
        status = "CLOSED" if is_closed else "UPDATE"
        return (
            f"[{status}] {kline.symbol} {kline.interval} | "
            f"Time: {kline.timestamp} | "
            f"O: {kline.open:.2f} H: {kline.high:.2f} "
            f"L: {kline.low:.2f} C: {kline.close:.2f} | "
            f"Vol: {kline.volume:.2f}"
        )

    def prepare_kafka_message(
        self,
        kline: KlineData,
        kline_message: KlineMessage
    ) -> Tuple[str, Dict[str, Any]]:
        """
        Prepare message for Kafka publishing.

        Args:
            kline: KlineData instance
            kline_message: KlineMessage instance

        Returns:
            Tuple of (message_key, message_value) for Kafka
        """
        message_key = kline.symbol
        message_value = kline_message.model_dump()
        return message_key, message_value

    def should_publish_to_kafka(self, is_closed: bool, publish_all: bool = True) -> bool:
        """
        Determine if message should be published to Kafka.

        Business rule: Can be configured to publish all messages or only closed candles.

        Args:
            is_closed: Whether the kline/candle is closed
            publish_all: If True, publish all messages; if False, only closed candles

        Returns:
            True if message should be published
        """
        if publish_all:
            return True
        return is_closed

    def validate_message_structure(self, message_data: Dict[str, Any]) -> bool:
        """
        Validate basic WebSocket message structure.

        Args:
            message_data: Parsed message dict

        Returns:
            True if message has required structure

        Raises:
            MessageValidationError: If validation fails
        """
        # Check for required top-level fields in Binance WebSocket kline message
        required_fields = {'e', 'E', 's', 'k'}

        if not isinstance(message_data, dict):
            raise MessageValidationError("Message must be a dictionary")

        missing_fields = required_fields - set(message_data.keys())
        if missing_fields:
            raise MessageValidationError(
                f"Missing required fields: {missing_fields}"
            )

        # Check kline data structure
        if not isinstance(message_data.get('k'), dict):
            raise MessageValidationError("'k' field must be a dictionary")

        kline_required = {'t', 'o', 'h', 'l', 'c', 'v', 'x'}
        kline_data = message_data['k']
        missing_kline_fields = kline_required - set(kline_data.keys())
        if missing_kline_fields:
            raise MessageValidationError(
                f"Missing required kline fields: {missing_kline_fields}"
            )

        return True

    def extract_symbol_interval(self, message_data: Dict[str, Any]) -> Tuple[str, str]:
        """
        Extract trading symbol and interval from message.

        Args:
            message_data: Parsed message dict

        Returns:
            Tuple of (symbol, interval)

        Raises:
            MessageValidationError: If extraction fails
        """
        try:
            symbol = message_data['s']
            interval = message_data['k']['i']
            return symbol, interval
        except KeyError as e:
            raise MessageValidationError(
                f"Failed to extract symbol/interval: {e}"
            ) from e
