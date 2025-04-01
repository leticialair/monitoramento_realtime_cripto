from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "cripto-precos",
    bootstrap_servers="localhost:9092",  # TODO: kafka:9092 se for dentro do container
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="teste-consumer",
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
)

print("Aguardando mensagens do Kafka...\n")
for message in consumer:
    print(f"Mensagem recebida: {message.value}")
