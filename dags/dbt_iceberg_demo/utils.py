""" test dbt docs generation """

from datetime import timedelta
from datetime import datetime
import pendulum
from dbt_iceberg_demo.cosmos_config import DOC_MD, MAX_ACTIVE_TASKS, SCHEDULE_INTERVAL

args = {"owner": "airflow", "retries": 3, "retry_delay": timedelta(seconds=10), "tags": ["ldm_mart"]}

DAG_DEFAULT_ARGS = {
    "default_args": args,
    "catchup": False,
    "max_active_runs": 1,
    "schedule_interval": SCHEDULE_INTERVAL,
    "start_date": datetime(2023, 12, 12, tzinfo=pendulum.timezone("Asia/Manila")),
    "doc_md": DOC_MD,
    "tags": ["dbt", "mart"],
    "max_active_tasks": MAX_ACTIVE_TASKS,
    "concurrency": 1,
    "dagrun_timeout": timedelta(minutes=60),
}

TABLE_GROUP_DESC = {
    1: "CL_Lease_Contracts",
    2: "Loans",
    3: "CASA_TD",
    6: "Dimensions_Collaterals",
    4: "FX_Swaps_Options",
    5: "Futures",
    7: "Cards_Trust",
    8: "Borrowings_Investments",
    9: "Money_Market_Repos",
    10: "Trade_Finance",
    11: "Insurance_SCR",
    12: "GUAVA_Tables_TS_Monitoring_and_GL_Mapping",
}
