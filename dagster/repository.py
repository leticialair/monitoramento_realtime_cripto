from dagster import Definitions
from assets.crypto_asset import crypto_asset
from jobs.crypto_job import (
    ticker_asset_job,
)
from schedules.crypto_schedule import (
    ticker_schedule,
)

datamart = Definitions(
    assets=[
        crypto_asset,
    ],
    jobs=[
        ticker_asset_job,
    ],
    schedules=[ticker_schedule],
)
