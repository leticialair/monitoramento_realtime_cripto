import json
import time
from kafka import KafkaProducer
from websocket import create_connection

LIST_SYMBOLS = [
    "BTCUSDT",  # Bitcoin
    "ETHUSDT",  # Ethereum
    "BNBUSDT",  # Binance Coin
    "XRPUSDT",  # Ripple
    "ADAUSDT",  # Cardano
    "DOGEUSDT",  # Dogecoin
    "SOLUSDT",  # Solana
    "MATICUSDT",  # Polygon
    "DOTUSDT",  # Polkadot
    "LTCUSDT",  # Litecoin
    "SHIBUSDT",  # Shiba Inu
    "TRXUSDT",  # Tron
    "AVAXUSDT",  # Avalanche
    "LINKUSDT",  # Chainlink
    "ATOMUSDT",  # Cosmos
    "UNIUSDT",  # Uniswap
    "XLMUSDT",  # Stellar
    "FILUSDT",  # Filecoin
    "ETCUSDT",  # Ethereum Classic
    "XMRUSDT",  # Monero
]


def get_connection(cripto_symbol):
    ws = create_connection(f"wss://stream.binance.com:9443/ws/{cripto_symbol}@ticker")
    return ws


def create_producer():
    for _ in range(5):
        try:
            return KafkaProducer(bootstrap_servers="kafka:9092")
        except:
            print("Waiting for Kafka...")
            time.sleep(5)
    raise Exception("Failed to connect to Kafka")


producer = create_producer()

while True:
    for symbol in LIST_SYMBOLS:
        try:
            ws = create_connection(
                f"wss://stream.binance.com:9443/ws/{symbol.lower()}@trade"
            )
            result = ws.recv()
            message = json.dumps({"symbol": symbol, "data": json.loads(result)}).encode(
                "utf-8"
            )
            producer.send("cripto-topic", message)
            print(f"Enviado: {symbol}")

        except Exception as e:
            print(f"Erro com {symbol}: {str(e)}")

        time.sleep(5)  # Intervalo entre símbolos
