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


def create_consumer():
    for _ in range(5):
        try:
            return KafkaConsumer(
                "cripto-topic",
                bootstrap_servers=["kafka:9092"],
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


pg_conn = create_db_connection()
setup_database(pg_conn)
consumer = create_consumer()

print("Consumer ready, waiting for messages...")

try:
    for message in consumer:
        try:
            save_message(pg_conn, message.value)
            print(f"Saved message: {message.value}")
        except Exception as e:
            print(f"Error processing message: {str(e)}")
finally:
    consumer.close()
    pg_conn.close()
