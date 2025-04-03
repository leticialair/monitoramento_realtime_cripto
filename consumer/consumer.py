import time
from kafka import KafkaConsumer


def create_consumer():
    for _ in range(5):
        try:
            return KafkaConsumer(
                "test-topic",
                bootstrap_servers=["kafka:9092"],
                auto_offset_reset="earliest",
                api_version=(2, 0, 2),
            )
        except:
            print("Waiting for Kafka...")
            time.sleep(5)
    raise Exception("Failed to connect to Kafka")


consumer = create_consumer()

for message in consumer:
    print(f"Received: {message.value.decode('utf-8')}")
