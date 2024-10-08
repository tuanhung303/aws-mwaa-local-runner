import os
from pathlib import Path
from cosmos.config import ProfileConfig, ProjectConfig
from cosmos.profiles import AthenaAccessKeyProfileMapping
from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.config import RenderConfig
from cosmos.constants import LoadMode, TestBehavior
from airflow.models import Variable

DBT_PROJECT_NAME = "iceberg_demo"
DEFAULT_VARIABLE = DBT_PROJECT_NAME + "_mwaa"


# check if the variable mwaa_bucket is set in the MWAA environment
def generate_default_mwaa_variable():
    default_variable = DEFAULT_VARIABLE
    bucker_name = os.environ.get("AIRFLOW_ENV_NAME", "ht-general-purpose")
    # check if variable exists
    if not Variable.get(default_variable, default_var=None):
        Variable.set(
            key=default_variable,
            value={
                "mwaa_bucket": bucker_name,
                "dbt_path": f"{os.environ['AIRFLOW_HOME']}/python3-virtualenv/dbt-env/bin/dbt",
                "dbt_docs_path": f"temp_dir/{DBT_PROJECT_NAME}",
                "path_to_dbt_project": f"{os.environ['AIRFLOW_HOME']}/dags/dbt/{DBT_PROJECT_NAME}",
                "dbt_schema": "cdm",
                "max_active_tasks": 4,
                "schedule_interval": "0 8,12,14 * * *",
                "dbt_threads": 1,
            },
            serialize_json=True,
        )


generate_default_mwaa_variable()
mwaa_variable = Variable.get(DEFAULT_VARIABLE, deserialize_json=True)

MWAA_BUCKET = mwaa_variable.get("mwaa_bucket")
PATH_TO_DBT_PROJECT = mwaa_variable.get("path_to_dbt_project")
DBT_PATH = mwaa_variable.get("dbt_path")
DBT_DOC_PATH = mwaa_variable.get("dbt_docs_path")
MAX_ACTIVE_TASKS = mwaa_variable.get("max_active_tasks", 4)
SCHEDULE_INTERVAL = mwaa_variable.get("schedule_interval", "0 8,12,14 * * *")

AthenaCredentials = AthenaAccessKeyProfileMapping(
    conn_id="aws_default",
    profile_args={
        "database": "awsdatacatalog",
        "region_name": "ap-southeast-1",
        "s3_staging_dir": "s3://ht-general-purpose/athena-spill/",
        "s3_data_dir": "s3://ht-general-purpose/athena_iceberg/",
        "schema": "iceberg_general",
        "timeout_seconds": 30,
        "threads": 10,
    },
)

DBT_PROFILE_CONFIG = ProfileConfig(
    profile_name="athena",
    target_name="dev",
    profile_mapping=AthenaCredentials,
)

DBT_PROJECT_CONFIG = ProjectConfig(
    dbt_project_path=Path(PATH_TO_DBT_PROJECT),
)

DBT_PATHCONFIG = {
    "project_config": DBT_PROJECT_CONFIG,
    "profile_config": DBT_PROFILE_CONFIG,
}


def get_dbt_task_group(
    group_id: str, select: list, test_behavior: TestBehavior = TestBehavior.AFTER_EACH
) -> DbtTaskGroup:
    """
    Get a dbt task group
    :param group_id: the group id
    :param select: the select list
    :param test_behavior: the test behavior
    :return: the dbt task group
    """
    return DbtTaskGroup(
        group_id=group_id,
        **DBT_PATHCONFIG,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=select,
            dbt_executable_path=DBT_PATH,
            test_behavior=test_behavior,
            dbt_deps=False,
        ),
    )


s3_dag_url = (
    f"https://ap-southeast-1.console.aws.amazon.com/s3/buckets/{MWAA_BUCKET}"
    f"?prefix={DBT_DOC_PATH}/&region=ap-southeast-1&bucketType=general"
)

DOC_MD = f"""
### DEMO iceberg dbt project

### dbt Documentation
The dbt docs can be found url: [{DBT_DOC_PATH}]({s3_dag_url})

"""
