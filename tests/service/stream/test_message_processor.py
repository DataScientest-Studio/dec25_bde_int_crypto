"""
Unit tests for MessageProcessor.

Tests the business logic for processing WebSocket messages.
"""

import pytest
from src.service.stream.message_processor import MessageProcessor, MessageValidationError
from src.models.models import KlineData, KlineMessage
from src.mappers import KlineMapper

# Import fixtures from separate file
pytest_plugins = ['tests.service.stream.fixtures']


class TestMessageProcessorParseWebSocketMessage:
    """Tests for parse_websocket_message method."""

    def test_parse_valid_json(self, raw_json_valid_message):
        """Test parsing valid JSON string."""
        processor = MessageProcessor()
        result = processor.parse_websocket_message(raw_json_valid_message)

        assert isinstance(result, dict)
        assert result['e'] == 'kline'
        assert result['s'] == 'BTCUSDT'
        assert 'k' in result

    def test_parse_closed_message_json(self, raw_json_closed_message):
        """Test parsing closed candle JSON string."""
        processor = MessageProcessor()
        result = processor.parse_websocket_message(raw_json_closed_message)

        assert isinstance(result, dict)
        assert result['e'] == 'kline'
        assert result['s'] == 'ETHUSDT'
        assert result['k']['x'] is True

    def test_parse_invalid_json_raises_error(self, raw_json_invalid):
        """Test parsing invalid JSON raises MessageValidationError."""
        processor = MessageProcessor()

        with pytest.raises(MessageValidationError) as exc_info:
            processor.parse_websocket_message(raw_json_invalid)

        assert "Failed to decode JSON message" in str(exc_info.value)

    def test_parse_empty_string_raises_error(self):
        """Test parsing empty string raises error."""
        processor = MessageProcessor()

        with pytest.raises(MessageValidationError):
            processor.parse_websocket_message("")


class TestMessageProcessorProcessMessage:
    """Tests for process_message method."""

    def test_process_valid_message(self, parsed_valid_message):
        """Test processing valid parsed message."""
        processor = MessageProcessor()
        result = processor.process_message(parsed_valid_message)

        assert result is not None
        kafka_msg, kline_data, is_closed = result

        assert isinstance(kafka_msg, KlineMessage)
        assert isinstance(kline_data, KlineData)
        assert isinstance(is_closed, bool)
        assert is_closed is False

    def test_process_closed_message(self, parsed_closed_message):
        """Test processing closed candle message."""
        processor = MessageProcessor()
        result = processor.process_message(parsed_closed_message)

        assert result is not None
        kafka_msg, kline_data, is_closed = result

        assert is_closed is True
        assert kafka_msg.is_closed is True

    def test_process_invalid_event_returns_none(self, parsed_invalid_event_type):
        """Test processing message with invalid event type returns None."""
        processor = MessageProcessor()
        result = processor.process_message(parsed_invalid_event_type)

        assert result is None

    def test_process_malformed_message_raises_error(self, parsed_missing_fields):
        """Test processing malformed message raises error."""
        processor = MessageProcessor()

        with pytest.raises(MessageValidationError) as exc_info:
            processor.process_message(parsed_missing_fields)

        assert "Failed to process message" in str(exc_info.value)


class TestMessageProcessorFormatKlineLog:
    """Tests for format_kline_log method."""

    def test_format_open_candle(self, sample_kline_data):
        """Test formatting open candle log message."""
        processor = MessageProcessor()
        log_msg = processor.format_kline_log(sample_kline_data, is_closed=False)

        assert "[UPDATE]" in log_msg
        assert "BTCUSDT" in log_msg
        assert "1m" in log_msg
        assert "O: 50000.00" in log_msg
        assert "H: 50200.00" in log_msg
        assert "L: 49900.00" in log_msg
        assert "C: 50100.00" in log_msg
        assert "Vol: 10.50" in log_msg

    def test_format_closed_candle(self, sample_kline_data):
        """Test formatting closed candle log message."""
        processor = MessageProcessor()
        log_msg = processor.format_kline_log(sample_kline_data, is_closed=True)

        assert "[CLOSED]" in log_msg
        assert "BTCUSDT" in log_msg
        assert "1m" in log_msg

    def test_format_contains_all_fields(self, sample_kline_data):
        """Test formatted message contains all required fields."""
        processor = MessageProcessor()
        log_msg = processor.format_kline_log(sample_kline_data, is_closed=False)

        # Check all components are present
        assert "Time:" in log_msg
        assert "O:" in log_msg
        assert "H:" in log_msg
        assert "L:" in log_msg
        assert "C:" in log_msg
        assert "Vol:" in log_msg


class TestMessageProcessorPrepareKafkaMessage:
    """Tests for prepare_kafka_message method."""

    def test_prepare_kafka_message_returns_tuple(self, sample_kline_data, sample_kline_message):
        """Test preparing Kafka message returns correct tuple."""
        processor = MessageProcessor()
        key, value = processor.prepare_kafka_message(sample_kline_data, sample_kline_message)

        assert isinstance(key, str)
        assert isinstance(value, dict)

    def test_prepare_kafka_message_key_is_symbol(self, sample_kline_data, sample_kline_message):
        """Test Kafka message key is the trading symbol."""
        processor = MessageProcessor()
        key, value = processor.prepare_kafka_message(sample_kline_data, sample_kline_message)

        assert key == "BTCUSDT"
        assert key == sample_kline_data.symbol

    def test_prepare_kafka_message_value_structure(self, sample_kline_data, sample_kline_message):
        """Test Kafka message value contains all fields."""
        processor = MessageProcessor()
        key, value = processor.prepare_kafka_message(sample_kline_data, sample_kline_message)

        # Check value has required fields
        assert 'event_time' in value
        assert 'symbol' in value
        assert 'interval' in value
        assert 'timestamp' in value
        assert 'open' in value
        assert 'high' in value
        assert 'low' in value
        assert 'close' in value
        assert 'volume' in value
        assert 'is_closed' in value


class TestMessageProcessorShouldPublishToKafka:
    """Tests for should_publish_to_kafka method."""

    def test_publish_all_open_candle(self):
        """Test publishing open candle when publish_all=True."""
        processor = MessageProcessor()
        result = processor.should_publish_to_kafka(is_closed=False, publish_all=True)

        assert result is True

    def test_publish_all_closed_candle(self):
        """Test publishing closed candle when publish_all=True."""
        processor = MessageProcessor()
        result = processor.should_publish_to_kafka(is_closed=True, publish_all=True)

        assert result is True

    def test_publish_closed_only_open_candle(self):
        """Test not publishing open candle when publish_all=False."""
        processor = MessageProcessor()
        result = processor.should_publish_to_kafka(is_closed=False, publish_all=False)

        assert result is False

    def test_publish_closed_only_closed_candle(self):
        """Test publishing closed candle when publish_all=False."""
        processor = MessageProcessor()
        result = processor.should_publish_to_kafka(is_closed=True, publish_all=False)

        assert result is True

    def test_default_publish_all_is_true(self):
        """Test default behavior is to publish all messages."""
        processor = MessageProcessor()
        # Not passing publish_all, should default to True
        result = processor.should_publish_to_kafka(is_closed=False)

        assert result is True


class TestMessageProcessorValidateMessageStructure:
    """Tests for validate_message_structure method."""

    def test_validate_valid_message(self, parsed_valid_message):
        """Test validating correct message structure."""
        processor = MessageProcessor()
        result = processor.validate_message_structure(parsed_valid_message)

        assert result is True

    def test_validate_closed_message(self, parsed_closed_message):
        """Test validating closed candle message."""
        processor = MessageProcessor()
        result = processor.validate_message_structure(parsed_closed_message)

        assert result is True

    def test_validate_non_dict_raises_error(self):
        """Test validating non-dict raises error."""
        processor = MessageProcessor()

        with pytest.raises(MessageValidationError) as exc_info:
            processor.validate_message_structure("not a dict")

        assert "Message must be a dictionary" in str(exc_info.value)

    def test_validate_missing_required_fields(self, parsed_missing_fields):
        """Test validating message with missing required fields."""
        processor = MessageProcessor()

        with pytest.raises(MessageValidationError) as exc_info:
            processor.validate_message_structure(parsed_missing_fields)

        assert "Missing required fields" in str(exc_info.value)

    def test_validate_invalid_kline_structure(self, parsed_invalid_kline_structure):
        """Test validating message with invalid kline structure."""
        processor = MessageProcessor()

        with pytest.raises(MessageValidationError) as exc_info:
            processor.validate_message_structure(parsed_invalid_kline_structure)

        assert "'k' field must be a dictionary" in str(exc_info.value)

    def test_validate_missing_kline_fields(self, parsed_missing_kline_fields):
        """Test validating message with missing kline fields."""
        processor = MessageProcessor()

        with pytest.raises(MessageValidationError) as exc_info:
            processor.validate_message_structure(parsed_missing_kline_fields)

        assert "Missing required kline fields" in str(exc_info.value)


class TestMessageProcessorExtractSymbolInterval:
    """Tests for extract_symbol_interval method."""

    def test_extract_from_valid_message(self, parsed_valid_message):
        """Test extracting symbol and interval from valid message."""
        processor = MessageProcessor()
        symbol, interval = processor.extract_symbol_interval(parsed_valid_message)

        assert symbol == "BTCUSDT"
        assert interval == "1m"

    def test_extract_from_closed_message(self, parsed_closed_message):
        """Test extracting from closed candle message."""
        processor = MessageProcessor()
        symbol, interval = processor.extract_symbol_interval(parsed_closed_message)

        assert symbol == "ETHUSDT"
        assert interval == "5m"

    def test_extract_missing_symbol_raises_error(self):
        """Test extracting from message without symbol."""
        processor = MessageProcessor()
        message = {
            "e": "kline",
            "k": {"i": "1m"}
            # Missing 's' field
        }

        with pytest.raises(MessageValidationError) as exc_info:
            processor.extract_symbol_interval(message)

        assert "Failed to extract symbol/interval" in str(exc_info.value)

    def test_extract_missing_interval_raises_error(self):
        """Test extracting from message without interval."""
        processor = MessageProcessor()
        message = {
            "e": "kline",
            "s": "BTCUSDT",
            "k": {}
            # Missing 'i' field in 'k'
        }

        with pytest.raises(MessageValidationError) as exc_info:
            processor.extract_symbol_interval(message)

        assert "Failed to extract symbol/interval" in str(exc_info.value)

    def test_extract_missing_kline_dict_raises_error(self):
        """Test extracting from message without kline dict."""
        processor = MessageProcessor()
        message = {
            "e": "kline",
            "s": "BTCUSDT"
            # Missing 'k' field
        }

        with pytest.raises(MessageValidationError) as exc_info:
            processor.extract_symbol_interval(message)

        assert "Failed to extract symbol/interval" in str(exc_info.value)


class TestMessageProcessorCustomMapper:
    """Tests for MessageProcessor with custom mapper."""

    def test_processor_with_custom_mapper(self):
        """Test MessageProcessor accepts custom KlineMapper."""
        custom_mapper = KlineMapper()
        processor = MessageProcessor(mapper=custom_mapper)

        assert processor.mapper is custom_mapper

    def test_processor_creates_default_mapper(self):
        """Test MessageProcessor creates default mapper when not provided."""
        processor = MessageProcessor()

        assert processor.mapper is not None
        assert isinstance(processor.mapper, KlineMapper)
