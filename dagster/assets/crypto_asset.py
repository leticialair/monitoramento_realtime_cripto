from dagster import AssetExecutionContext
from dagster_dbt import (
    DagsterDbtTranslator,
    dbt_assets,
    DagsterDbtTranslatorSettings,
)
from constants.constants import dbt_manifest_path, dbt
from typing import Any, Mapping, Optional


class CustomDagsterDbtTranslator(DagsterDbtTranslator):
    def get_group_name(self, dbt_resource_props: Mapping[str, Any]) -> Optional[str]:
        return "dm_crypto"


@dbt_assets(
    manifest=dbt_manifest_path,
    dagster_dbt_translator=CustomDagsterDbtTranslator(
        settings=DagsterDbtTranslatorSettings(enable_asset_checks=True)
    ),
)
def crypto_asset(context: AssetExecutionContext):
    yield from dbt.cli(["build"], context=context).stream()
