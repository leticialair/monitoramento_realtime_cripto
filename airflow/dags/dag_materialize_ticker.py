from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="materialize_ticker_table",
    default_args=default_args,
    schedule_interval="*/10 * * * *",
    catchup=False,
) as dag:

    materialize_query = PostgresOperator(
        task_id="materialize_ticker",
        postgres_conn_id="postgres_default",
        sql="""
        DROP TABLE IF EXISTS public.ticker;
        CREATE TABLE public.ticker AS
        SELECT
          JSON_VALUE(message, '$.A') AS ask_quantity,
          JSON_VALUE(message, '$.B') AS bid_quantity,
          JSON_VALUE(message, '$.C') AS last_trade_time,
          JSON_VALUE(message, '$.E') AS event_time,
          JSON_VALUE(message, '$.F') AS first_trade_id,
          JSON_VALUE(message, '$.L') AS last_trade_id,
          JSON_VALUE(message, '$.O') AS open_time,
          JSON_VALUE(message, '$.P') AS price_change_percent,
          JSON_VALUE(message, '$.Q') AS last_trade_qty,
          JSON_VALUE(message, '$.a') AS ask_price,
          JSON_VALUE(message, '$.b') AS bid_price,
          JSON_VALUE(message, '$.c') AS close_price,
          JSON_VALUE(message, '$.e') AS event_type,
          JSON_VALUE(message, '$.h') AS high_price,
          JSON_VALUE(message, '$.l') AS low_price,
          JSON_VALUE(message, '$.n') AS total_trades,
          JSON_VALUE(message, '$.o') AS open_price,
          JSON_VALUE(message, '$.p') AS price_change,
          JSON_VALUE(message, '$.q') AS quote_volume,
          JSON_VALUE(message, '$.s') AS symbol,
          JSON_VALUE(message, '$.v') AS base_volume,
          JSON_VALUE(message, '$.w') AS weighted_avg_price,
          JSON_VALUE(message, '$.x') AS prev_close_price
        FROM public.kafka_messages;
        """,
    )
