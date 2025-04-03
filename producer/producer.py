import json
from websocket import WebSocketApp
from kafka import KafkaProducer
import logging

# Configuração básica
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Símbolos para monitorar (BTC e ETH por padrão)
SYMBOLS = ["btcusdt", "ethusdt"]

# Configura o producer Kafka
producer = KafkaProducer(
    bootstrap_servers=["kafka:9092"],
    value_serializer=lambda x: json.dumps(x).encode("utf-8"),
)


def on_message(ws, message):
    """Processa mensagens do WebSocket da Binance"""
    try:
        data = json.loads(message)

        if "e" in data and data["e"] == "trade":  # Filtra apenas trades
            trade = {
                "symbol": data["s"],
                "price": data["p"],
                "quantity": data["q"],
                "time": data["T"],
            }

            logger.info(f"Trade: {trade['symbol']} @ {trade['price']}")
            producer.send("binance-trades", trade)

    except Exception as e:
        logger.error(f"Erro: {e}")


def on_error(ws, error):
    logger.error(f"Erro no WebSocket: {error}")


def on_close(ws, status, msg):
    logger.info("Conexão fechada. Reconectando...")
    connect_websocket()  # Tenta reconectar


def on_open(ws):
    logger.info("Conectado ao WebSocket da Binance")


def connect_websocket():
    """Conecta ao WebSocket da Binance"""
    streams = [f"{s}@trade" for s in SYMBOLS]
    url = f"wss://stream.binance.com:9443/stream?streams={'/'.join(streams)}"

    ws = WebSocketApp(
        url,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
        on_open=on_open,
    )

    ws.run_forever()


if __name__ == "__main__":
    connect_websocket()
