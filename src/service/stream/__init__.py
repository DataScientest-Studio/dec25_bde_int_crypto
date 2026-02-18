"""
Stream Processing Module.

This module contains all stream-related components for real-time data processing:
- Consumer: Consumes messages from Kafka/Redpanda
- Producer: Produces messages from Binance WebSocket to Kafka/Redpanda
- Message Processor: Business logic for message processing
- WebSocket Manager: WebSocket connection management
"""

from src.service.stream.consumer import BinanceKlineConsumer
from src.service.stream.producer import BinanceWebSocketCollector
from src.service.stream.message_processor import MessageProcessor, MessageValidationError
from src.service.stream.websocket_manager import WebSocketManager, WebSocketConnectionError

__all__ = [
    'BinanceKlineConsumer',
    'BinanceWebSocketCollector',
    'MessageProcessor',
    'MessageValidationError',
    'WebSocketManager',
    'WebSocketConnectionError',
]
