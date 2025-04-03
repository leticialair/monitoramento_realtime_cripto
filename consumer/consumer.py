from kafka import KafkaConsumer
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

consumer = KafkaConsumer(
    "binance-trades",
    bootstrap_servers=["kafka:9092"],
    auto_offset_reset="earliest",
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
)

logger.info("Aguardando mensagens do Kafka...")

for message in consumer:
    trade = message.value
    logger.info(
        f"""
        Recebido: 
        Moeda: {trade['symbol']}
        Preço: {trade['price']}
        Quantidade: {trade['quantity']}
        Hora: {trade['time']}
    """
    )
