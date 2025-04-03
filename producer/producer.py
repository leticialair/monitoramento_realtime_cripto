from kafka import KafkaProducer
import time


def create_producer():
    for _ in range(5):
        try:
            return KafkaProducer(
                bootstrap_servers=["kafka:9092"], api_version=(2, 0, 2)
            )
        except:
            print("Waiting for Kafka...")
            time.sleep(5)
    raise Exception("Failed to connect to Kafka")


producer = create_producer()

for i in range(10):
    producer.send("test-topic", value=f"Mensagem {i}".encode("utf-8"))
    print(f"Sent message {i}")
    time.sleep(1)

producer.flush()
