from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.config import RenderConfig
from cosmos.constants import LoadMode, TestBehavior
from cosmos.operators import DbtDocsS3Operator
from airflow.decorators import dag
from dbt_iceberg_demo.cosmos_config import (
    DBT_PATH,
    DBT_PATHCONFIG,
    PATH_TO_DBT_PROJECT,
    DBT_PROFILE_CONFIG,
    DBT_DOC_PATH,
    MWAA_BUCKET,
)
from dbt_iceberg_demo.utils import DAG_DEFAULT_ARGS
from fx_rate.scripts.utils import get_fx_rate

AWS_CONN_ID = "aws_default"


# define the DAG
@dag(**DAG_DEFAULT_ARGS)
def dbt_iceberg_demo() -> DbtTaskGroup:

    dbt_run = DbtTaskGroup(
        group_id="etl_models",
        **DBT_PATHCONFIG,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=["path:models"],
            dbt_executable_path=DBT_PATH,
            test_behavior=TestBehavior.AFTER_ALL,
            dbt_deps=False,
        ),
    )

    snapshot = DbtTaskGroup(
        group_id="snapshot_models",
        **DBT_PATHCONFIG,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=["path:snapshots"],
            dbt_executable_path=DBT_PATH,
            test_behavior=TestBehavior.AFTER_ALL,
            dbt_deps=False,
        ),
    )

    generate_docs = DbtDocsS3Operator(
        task_id="generate_docs",
        project_dir=PATH_TO_DBT_PROJECT,
        profile_config=DBT_PROFILE_CONFIG,
        connection_id=AWS_CONN_ID,
        bucket_name=MWAA_BUCKET,
        dbt_cmd_flags=["--static"],
        folder_dir=DBT_DOC_PATH,
        trigger_rule="all_done",
    )

    get_fx_rate() >> snapshot >> dbt_run >> generate_docs


dbt_iceberg_demo = dbt_iceberg_demo()
