import time
from kafka import KafkaConsumer
import psycopg2
import json

# Configurações do PostgreSQL
PG_CONFIG = {
    "host": "postgres",
    "database": "postgres",
    "user": "postgres",
    "password": "postgres",
}


def create_db_connection():
    for _ in range(5):
        try:
            conn = psycopg2.connect(**PG_CONFIG)
            conn.autocommit = True
            return conn
        except Exception as e:
            print(f"Waiting for PostgreSQL... Error: {str(e)}")
            time.sleep(5)
    raise Exception("Failed to connect to PostgreSQL")


def create_consumer(symbol: str):
    for _ in range(5):
        try:
            return KafkaConsumer(
                f"{symbol}-topic",
                bootstrap_servers="kafka:9092",
                auto_offset_reset="earliest",
                api_version=(2, 0, 2),
            )
        except:
            print("Waiting for Kafka...")
            time.sleep(5)
    raise Exception("Failed to connect to Kafka")


def setup_database(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
        CREATE TABLE IF NOT EXISTS kafka_messages (
            id SERIAL PRIMARY KEY,
            message JSONB,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        )
        print("Created table if not exists")


def save_message(conn, message):
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO kafka_messages (message) VALUES (%s)", (json.dumps(message),)
        )


def main(symbol: str):
    print("Criando conexão com o postgres...")
    pg_conn = create_db_connection()
    setup_database(pg_conn)
    print("Conexão com o postgres criada!")

    print("Criando consumer...")
    consumer = create_consumer(symbol)
    print("Consumer criado!")

    try:
        for message in consumer:
            try:
                # Decodifica os bytes para string e depois para dict
                decoded = message.value.decode("utf-8")
                json_data = json.loads(decoded)

                save_message(pg_conn, json_data)
                print(f"Mensagem salva: {decoded}")
            except Exception as e:
                print(f"Error processando mensagem: {str(e)}")
    finally:
        consumer.close()
        pg_conn.close()


main("btcusdt")
