from utils.class_binance_websocket import BinanceWebSocket
from kafka import KafkaProducer
import json
import os

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "cripto-precos")
SYMBOL = os.getenv("SYMBOL", "btcusdt")
INTERVAL = os.getenv("INTERVAL", "1m")

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)


def handle_message(message):
    data = json.loads(message)
    payload = {
        "symbol": data["s"],
        "timestamp": data["E"],
        "price": data["k"]["c"],  # preço de fechamento
        "volume": data["k"]["v"],
    }
    print(f"Enviando para o Kafka: {payload}")
    producer.send(TOPIC, value=payload)


if __name__ == "__main__":
    ws = BinanceWebSocket(symbol=SYMBOL, interval=INTERVAL, callback=handle_message)
    ws.start()
