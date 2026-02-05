"""Simple test to verify QuixStreams producer works with Redpanda"""

import json
from quixstreams import Application

# Configuration
KAFKA_BROKER = "localhost:19092"
KAFKA_TOPIC = "binance-klines"

print(f"Connecting to Kafka broker: {KAFKA_BROKER}")
print(f"Topic: {KAFKA_TOPIC}")

# Create QuixStreams application
app = Application(
    broker_address=KAFKA_BROKER,
    loglevel="DEBUG"
)

# Get topic
topic = app.topic(KAFKA_TOPIC, value_serializer="json")

# Get producer
producer = app.get_producer()

print("Producer created successfully")

# Send test messages
for i in range(5):
    message = {
        "test_id": i,
        "symbol": "BTCUSDT",
        "price": 50000.0 + i,
        "message": f"Test message {i}"
    }

    key = f"test-{i}"

    print(f"Sending message {i}: {message}")

    # Serialize using topic's serializer
    serialized = topic.serialize(key=key, value=message)

    producer.produce(
        topic=topic.name,
        key=serialized.key,
        value=serialized.value
    )

    print(f"Message {i} sent")

# Flush to ensure all messages are sent
print("Flushing producer...")
producer.flush()
print("All messages sent successfully!")
