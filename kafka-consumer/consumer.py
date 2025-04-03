from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "cripto-precos",
    bootstrap_servers="kafka:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
)

print("Consumer iniciado!")

for message in consumer:
    print(f"Recebido: {message.value}")
