from datetime import datetime
from airflow.decorators import dag
from fx_rate.scripts.utils import get_fx_rate


@dag(start_date=datetime(2024, 10, 1), schedule=None, catchup=False, concurrency=1)
def fx_rate():
    get_fx_rate()


fx_rate()
