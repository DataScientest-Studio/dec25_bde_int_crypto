"""Constants for Binance historical data collection"""

# API Configuration
BASE_URL = "https://api.binance.com/api/v3/klines"
MAX_LIMIT = 1000
RATE_LIMIT_SLEEP = 0.5  # seconds

# Data Collection Configuration
SYMBOL = "BTCUSDT"
INTERVAL = "1h"
START_DATE = "2023-01-01"  # YYYY-MM-DD
END_DATE = None            # or "2024-01-01"

# Directory Paths
DATA_DIR = "data"
MODELS_DIR = "models"

# Redpanda/Kafka Configuration
KAFKA_BROKER = "localhost:19092"
KAFKA_TOPIC = "binance-klines"

# MongoDB Configuration
MONGODB_URI = "mongodb://admin:password@localhost:27017/"
MONGODB_DATABASE = "crypto_data"
MONGODB_COLLECTION = "klines"
