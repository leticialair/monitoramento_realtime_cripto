import time
from kafka import KafkaProducer
from websocket import create_connection, WebSocketConnectionClosedException


def create_producer(retries: int = 10):
    for _ in range(retries):
        try:
            print("Tentando conectar ao Kafka...")
            producer = KafkaProducer(
                bootstrap_servers="kafka:9092",
                value_serializer=lambda v: v.encode("utf-8"),
            )
            print("Conexão com o Kafka realizada com sucesso!")
            return producer
        except Exception as e:
            print(f"Erro ao tentar se conectar com o Kafka: {str(e)}")
            time.sleep(5)
    raise Exception(f"Kafka inacessível após {retries} tentativas.")


def main():
    symbol = "btcusdt"
    topic = f"{symbol}-topic"

    print("Criando producer...")
    producer = create_producer()
    print("Producer criado!")

    print(f"Conectando ao WebSocket da Binance para {symbol}...")
    try:
        ws = create_connection(f"wss://stream.binance.com:9443/ws/{symbol}@trade")
        print("Conexão com WebSocket estabelecida.")

        while True:
            try:
                result = ws.recv()
                producer.send(topic, result)
                print(f"Enviado para o Kafka: {result[:60]}...")
            except WebSocketConnectionClosedException:
                print("Conexão WebSocket fechada. Tentando reconectar...")
                ws = create_connection(
                    f"wss://stream.binance.com:9443/ws/{symbol}@trade"
                )
            except Exception as e:
                print(f"Erro durante envio ou recebimento: {str(e)}")
                time.sleep(5)

    except Exception as e:
        print(f"Erro na conexão com WebSocket: {str(e)}")


main()
