import os
from pathlib import Path
from dagster_dbt import DbtCliResource

dbt_project_dir = Path(__file__).joinpath("..", "..", "..").resolve()
dbt = DbtCliResource(project_dir=os.fspath(f"{dbt_project_dir}/dbt"))

############################################################################################################
# Caso a variável DAGSTER_DBT_PARSE_PROJECT_ON_LOAD esteja definida igual a 1 um arquivo manifest será criado
# em tempo de execução. Caso não, espera-se que o arquivo já esteja criado e o path seja passado.
# Recomendado como prática manter a variável setada e gerar o arquivo manifest em tempo de execução uma vez
# que esteja rodadando a aplicação em um container.
############################################################################################################

if os.getenv("DAGSTER_DBT_PARSE_PROJECT_ON_LOAD"):
    dbt_parse_invocation = dbt.cli(["parse"]).wait()
    dbt_manifest_path = dbt_parse_invocation.target_path.joinpath("manifest.json")
else:
    dbt_manifest_path = dbt_project_dir.joinpath("dbt/target", "manifest.json")
