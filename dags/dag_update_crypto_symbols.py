import os
import sys
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Definir ambiente como dev ou prod
ambiente = "dev"

if ambiente == "dev":
    sys.path.append(os.path.dirname(os.path.abspath(__name__)))

from utils import BinanceAPI


def get_top_cryptos():
    """
    Puxa as 20 criptomoedas mais negociadas na Binance e salva em um arquivo
    que será utilizado para a atualização do pipeline.
    """
    top_symbols = BinanceAPI().get_top20_symbols()
    with open(r"/files/symbols.txt", "w") as f:
        f.write(",".join(top_symbols))


default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 3, 26),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "update_crypto_symbols",
    default_args=default_args,
    schedule_interval="0 0 * * *",
)

task_update_list = PythonOperator(
    task_id="get_top_cryptos",
    python_callable=get_top_cryptos,
    dag=dag,
)

task_update_list
