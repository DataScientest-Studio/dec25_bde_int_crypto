"""
WebSocket Connection Manager for Binance Streams.

This module handles WebSocket connection lifecycle and SSL configuration,
separated from business logic for better testability.
"""

import logging
import os
import ssl
from typing import Optional
import websockets

logger = logging.getLogger(__name__)


class WebSocketConnectionError(Exception):
    """Raised when WebSocket connection fails."""
    pass


class WebSocketManager:
    """
    Manages WebSocket connections to Binance streams.

    Handles connection lifecycle, SSL configuration, and reconnection logic.
    """

    def __init__(
        self,
        base_url: str = "wss://stream.binance.com:9443/ws",
        ssl_verify: bool = False,
        ca_file: Optional[str] = None
    ):
        """
        Initialize WebSocket manager.

        Args:
            base_url: Base URL for WebSocket connections
            ssl_verify: Whether to verify SSL certificates
            ca_file: Optional path to custom CA bundle
        """
        self.base_url = base_url
        self.ssl_verify = ssl_verify
        self.ca_file = ca_file or os.getenv("BINANCE_CA_FILE")
        self.websocket: Optional[websockets.WebSocketClientProtocol] = None
        self.ssl_context = self._create_ssl_context()

    def _create_ssl_context(self) -> ssl.SSLContext:
        """
        Create SSL context for WebSocket connections.

        Returns:
            Configured SSL context
        """
        ssl_context = ssl.create_default_context()

        if not self.ssl_verify:
            # Disable certificate verification (e.g., for corporate proxies)
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE

        if self.ca_file:
            ssl_context.load_verify_locations(cafile=self.ca_file)

        return ssl_context

    def build_stream_url(self, symbol: str, interval: str) -> str:
        """
        Build WebSocket stream URL for a specific symbol and interval.

        Args:
            symbol: Trading pair symbol (e.g., 'btcusdt')
            interval: Kline interval (e.g., '1m', '5m')

        Returns:
            Full WebSocket URL
        """
        symbol_lower = symbol.lower()
        return f"{self.base_url}/{symbol_lower}@kline_{interval}"

    async def connect(self, url: str) -> websockets.WebSocketClientProtocol:
        """
        Establish WebSocket connection.

        Args:
            url: WebSocket URL to connect to

        Returns:
            Connected WebSocket instance

        Raises:
            WebSocketConnectionError: If connection fails
        """
        try:
            # Only pass SSL context for secure WebSocket URLs
            ssl_param = self.ssl_context if url.startswith("wss://") else None

            self.websocket = await websockets.connect(url, ssl=ssl_param)
            logger.info(f"Connected to WebSocket: {url}")
            return self.websocket

        except ssl.SSLCertVerificationError as e:
            error_msg = (
                "TLS certificate verification failed while connecting to Binance.\n"
                "This usually means you're behind a TLS-intercepting proxy/AV, or Python's CA "
                "certificates are not installed correctly on this machine.\n"
                "Fix options:\n"
                "  - If on a corporate network: get the proxy/root CA bundle and set BINANCE_CA_FILE\n"
                "  - On macOS (python.org Python): run 'Install Certificates.command'\n"
                "  - Check if SSL_CERT_FILE/REQUESTS_CA_BUNDLE is set to an unexpected value\n"
                f"Underlying error: {e}"
            )
            logger.error(error_msg)
            raise WebSocketConnectionError(error_msg) from e

        except Exception as e:
            logger.error(f"Failed to connect to WebSocket: {e}")
            raise WebSocketConnectionError(f"Connection failed: {e}") from e

    async def disconnect(self):
        """Close WebSocket connection."""
        if self.websocket:
            await self.websocket.close()
            logger.info("WebSocket connection closed")
            self.websocket = None

    def is_connected(self) -> bool:
        """
        Check if WebSocket is currently connected.

        Returns:
            True if connected, False otherwise
        """
        return self.websocket is not None and self.websocket.open

    async def receive_message(self) -> str:
        """
        Receive next message from WebSocket.

        Returns:
            Raw message string

        Raises:
            WebSocketConnectionError: If not connected or receive fails
        """
        if not self.websocket:
            raise WebSocketConnectionError("Not connected")

        try:
            message = await self.websocket.recv()
            return message
        except websockets.exceptions.ConnectionClosed as e:
            raise WebSocketConnectionError(f"Connection closed: {e}") from e
        except Exception as e:
            raise WebSocketConnectionError(f"Failed to receive message: {e}") from e

    async def __aenter__(self):
        """Context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        await self.disconnect()
