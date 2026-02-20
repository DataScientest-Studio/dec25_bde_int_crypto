# Tests

This directory contains unit and integration tests for the project.

## Running Tests

### Run all tests
```bash
.venv/bin/python -m pytest
```

### Run specific test file
```bash
.venv/bin/python -m pytest tests/mappers/test_kline_mapper.py
```

### Run with verbose output
```bash
.venv/bin/python -m pytest -v
```

### Run with coverage
```bash
.venv/bin/python -m pytest --cov=src --cov-report=term-missing
```

### Run specific test class or method
```bash
.venv/bin/python -m pytest tests/mappers/test_kline_mapper.py::TestKlineMapperParseWebSocketMessage
.venv/bin/python -m pytest tests/mappers/test_kline_mapper.py::TestKlineMapperParseWebSocketMessage::test_parse_valid_open_candle
```

## Test Organization

```
tests/
├── __init__.py
├── README.md
├── conftest.py                  # Shared fixtures (MongoDB testcontainers)
├── mappers/
│   ├── __init__.py
│   ├── fixtures.py              # Reusable test fixtures
│   └── test_kline_mapper.py     # Tests for KlineMapper (100% coverage)
└── service/
    ├── __init__.py
    ├── test_mongo_repository.py # Tests for AsyncKlineStore with MongoDB
    └── stream/
        ├── __init__.py
        ├── fixtures.py          # Reusable test fixtures
        └── test_message_processor.py  # Tests for MessageProcessor (100% coverage)
```

### Fixtures

Test fixtures are extracted to separate files for reusability:

#### Shared Fixtures (`tests/conftest.py`)
  - `mongo_container`: Session-scoped MongoDB testcontainer
  - `mongo_client`: Function-scoped initialized MongoClient
  - `kline_store`: Function-scoped initialized AsyncKlineStore

#### Mappers Fixtures (`tests/mappers/fixtures.py`)
  - `valid_websocket_message`: Valid open candle WebSocket message
  - `closed_websocket_message`: Valid closed candle WebSocket message
  - `invalid_event_type_message`: Message with wrong event type
  - `malformed_message`: Malformed message missing required fields

#### Stream Service Fixtures (`tests/service/stream/fixtures.py`)
  - `raw_json_valid_message`: Raw JSON string of valid message
  - `raw_json_closed_message`: Raw JSON string of closed candle
  - `raw_json_invalid`: Invalid JSON string
  - `parsed_valid_message`: Parsed valid message dict
  - `parsed_closed_message`: Parsed closed candle message
  - `parsed_invalid_event_type`: Message with invalid event type
  - `parsed_missing_fields`: Message missing required fields
  - `parsed_invalid_kline_structure`: Invalid kline structure
  - `parsed_missing_kline_fields`: Missing kline fields
  - `sample_kline_data`: Sample KlineData instance
  - `sample_kline_message`: Sample KlineMessage instance

Fixtures are automatically loaded using pytest plugins mechanism.

## Test Coverage

### KlineMapper: **100%** (15 tests)

- ✅ Valid WebSocket message parsing (open and closed candles)
- ✅ Invalid event type handling
- ✅ Malformed message error handling
- ✅ Empty message handling
- ✅ KlineData to KlineMessage conversion
- ✅ Full WebSocket to Kafka message conversion
- ✅ Edge cases (small prices, large volumes, zero volume)

### MessageProcessor: **100%** (32 tests)

- ✅ JSON parsing (valid, invalid, empty strings)
- ✅ Message processing and conversion to domain models
- ✅ Log formatting (open and closed candles)
- ✅ Kafka message preparation
- ✅ Publishing business rules (publish all vs closed only)
- ✅ Message structure validation
- ✅ Symbol and interval extraction
- ✅ Custom mapper dependency injection

### AsyncKlineStore: **9 tests** (with MongoDB Testcontainers)

- ✅ Inserting new klines
- ✅ Updating existing klines (upsert)
- ✅ Handling empty lists
- ✅ Creating unique indexes
- ✅ Preventing duplicate keys
- ✅ Idempotent index creation
- ✅ Automatic index creation on upsert
- ✅ Mixed insert and update operations
- ✅ Test isolation (clean state between tests)

## MongoDB Testcontainers

This project uses [Testcontainers](https://testcontainers.com/) for integration testing with MongoDB.

### Prerequisites
- **Docker** must be installed and running
- Dependencies installed via `uv sync`

### How it works
1. A MongoDB container is started once per test session
2. Each test gets an initialized `MongoClient` and `AsyncKlineStore`
3. Collections are cleaned between tests for isolation
4. Container is automatically stopped after tests complete

### Running MongoDB tests

```bash
# Run MongoDB repository tests
uv run pytest tests/service/test_mongo_repository.py -v

# Run all tests (including MongoDB integration tests)
uv run pytest
```

### Troubleshooting

**Docker not running:**
```
Error: Cannot connect to the Docker daemon
```
Solution: Start Docker Desktop or your Docker daemon

**Slow first run:**
The first test run may be slow as Docker pulls the MongoDB image. Subsequent runs are much faster.

## Installing Test Dependencies

```bash
uv sync
```

This installs all dependencies including:
- `pytest` - Testing framework
- `pytest-asyncio` - Async test support
- `testcontainers[mongodb]` - MongoDB testcontainers
- `pytest-cov` - Coverage reporting
