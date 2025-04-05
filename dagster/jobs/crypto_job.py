from dagster import (
    define_asset_job,
)

ticker_asset_job = define_asset_job(
    name="ticker",
    selection=[
        "ticker",
    ],
)
