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
            message->>'A' AS ask_quantity,
            message->>'B' AS bid_quantity,
            message->>'C' AS last_trade_time,
            message->>'E' AS event_time,
            message->>'F' AS first_trade_id,
            message->>'L' AS last_trade_id,
            message->>'O' AS open_time,
            message->>'P' AS price_change_percent,
            message->>'Q' AS last_trade_qty,
            message->>'a' AS ask_price,
            message->>'b' AS bid_price,
            message->>'c' AS close_price,
            message->>'e' AS event_type,
            message->>'h' AS high_price,
            message->>'l' AS low_price,
            message->>'n' AS total_trades,
            message->>'o' AS open_price,
            message->>'p' AS price_change,
            message->>'q' AS quote_volume,
            message->>'s' AS symbol,
            message->>'v' AS base_volume,
            message->>'w' AS weighted_avg_price,
            message->>'x' AS prev_close_price
        FROM public.kafka_messages;
        """,
    )
