from datetime import datetime
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 4, 6),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

dag = DAG(
    "materializacao_tabelas_postgres",
    default_args=default_args,
    description="DAG para materialização de tabela tratada no PostgreSQL a cada minuto.",
    schedule_interval="* * * * *",
    catchup=False,
)

# Tarefa para criar uma tabela materializada
criar_tabela_materializada = PostgresOperator(
    task_id="criar_tabela_materializada",
    postgres_conn_id="postgres_conn",  # ID da conexão configurada no Airflow
    sql="""
    CREATE MATERIALIZED VIEW IF NOT EXISTS treated_kafka_messages AS
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
    dag=dag,
)

# Tarefa para atualizar a tabela materializada
atualizar_tabela_materializada = PostgresOperator(
    task_id="atualizar_tabela_materializada",
    postgres_conn_id="postgres_conn",
    sql="REFRESH MATERIALIZED VIEW minha_tabela_materializada;",
    dag=dag,
)

criar_tabela_materializada >> atualizar_tabela_materializada
