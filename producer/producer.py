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


def create_producer(retries: int = 3):
    for _ in range(retries):
        try:
            print("Tentando conectar ao Kafka...")  # <-- Add logging
            producer = KafkaProducer(
                bootstrap_servers="kafka:9092",
                request_timeout_ms=3000,
            )
            print("Conexão com o Kafka realizada com sucesso!")
            return producer
        except Exception as e:
            print(f"Erro ao tentar se conectar com o Kafka: {str(e)}")
            time.sleep(2)
    raise Exception(f"Kafka inacessível após {retries} tentativas.")


print("Criando producer...")
producer = create_producer()
print("Producer criado!")

while True:
    for symbol in LIST_SYMBOLS:
        print(f"Iniciando símbolo {symbol}.")
        try:
            # ws = create_connection(
            #     f"wss://stream.binance.com:9443/ws/{symbol.lower()}@trade"
            # )
            ws = get_connection(symbol.lower())
            result = ws.recv()
            message = json.dumps({"symbol": symbol, "data": json.loads(result)}).encode(
                "utf-8"
            )
            producer.send("cripto-topic", message)
            print(f"Enviado: {symbol}")

        except Exception as e:
            print(f"Erro com {symbol}: {str(e)}")

        time.sleep(5)  # Intervalo entre símbolos
