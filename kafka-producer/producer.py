from kafka import KafkaProducer
import json
from utils.class_binance_websocket import BinanceWebSocket

# Conectar ao Kafka dentro do Docker
producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

print("Producer iniciado!")


def processar_mensagem(message):
    """Callback para processar mensagens do WebSocket e enviá-las ao Kafka"""
    data = json.loads(message)
    trade_info = {
        "preco": data["k"]["c"],  # Preço de fechamento do candle
        "volume": data["k"]["v"],  # Volume do candle
        "tempo": data["k"]["t"],  # Timestamp do candle
    }
    print(f"Enviando: {trade_info}")
    producer.send("cripto-precos", value=trade_info)
    producer.flush()


# Criar e iniciar o WebSocket da Binance
ws = BinanceWebSocket(symbol="BTCUSDT", interval="1m", callback=processar_mensagem)
ws.start()
