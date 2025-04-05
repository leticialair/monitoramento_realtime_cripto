from dagster import schedule
from jobs.crypto_job import ticker_asset_job


@schedule(
    cron_schedule="*/10 * * * *",
    job=ticker_asset_job,
    execution_timezone="America/Sao_Paulo",
)
def ticker_schedule(_context):
    run_config = {}
    return run_config
