from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils


class Spark:

    def __init__(self, segundos_janela: int = 5):
        self.spark = SparkSession.builder.appName("CryptoStream").getOrCreate()
        self.ssc = StreamingContext(self.spark.sparkContext, segundos_janela)
        self.kafka_stream = KafkaUtils.createStream(
            self.ssc, "kafka:9093", "crypto-group", {"cripto-precos": 1}
        )

    # Processamento dos dados
    def process_message(self, message):
        # Processamento da mensagem recebida
        print(message)


if __file__ == "__main__":
    spark_object = Spark()
    spark_object.kafka_stream.foreachRDD(
        lambda rdd: rdd.foreach(spark_object.process_message)
    )
    # Inicia o stream
    spark_object.ssc.start()
    spark_object.ssc.awaitTermination()
