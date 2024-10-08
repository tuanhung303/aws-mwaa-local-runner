import logging
import random
import requests
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import duckdb as db
import pandas as pd
from airflow.decorators import task, task_group
from airflow.hooks.base import BaseHook
from fx_rate.scripts.sql_stm import (
    CHECK_TABLE_STMT,
    CREATE_TABLE_STMT,
    INSERT_TABLE_STMT,
    DUCKDB_CONF,
    SELECT_NEW_RECORDS_STMT,
    PROXY_ADDRESSES,
    USER_AGENT_LIST,
)


def get_random_user_agent(user_agent_list):
    return random.choice(user_agent_list)


def generate_date_range(datetime_str, look_back_window: int = 7):
    date_rate = datetime.strptime(datetime_str, "%d.%m.%Y")
    date_range = [date_rate.strftime("%d.%m.%Y")]
    for i in range(look_back_window):
        date_rate -= timedelta(days=1)
        date_range.append(date_rate.strftime("%d.%m.%Y"))
    date_range.reverse()
    return date_range


def fetch_exchange_rate(date, user_agent, proxy_addresses):
    url = f"https://www.cbr.ru/eng/currency_base/daily/?UniDbQuery.Posted=True&UniDbQuery.To={date}"
    headers = {"User-Agent": user_agent}
    response = requests.get(url, headers=headers, proxies=proxy_addresses)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")
    table = soup.find_all("table")[0]
    df = pd.read_html(str(table))[0]
    df.columns = ["num_code", "char_code", "unit", "currency", "rate"]
    df["business_date"] = datetime.strptime(date, "%d.%m.%Y").strftime("%Y-%m-%d")
    return df


@task_group()
def get_fx_rate():
    user_agent = get_random_user_agent(USER_AGENT_LIST)
    date_range = generate_date_range(datetime.now().strftime("%d.%m.%Y"), look_back_window=7)

    first_date_dag = None
    remaining_dates_dag = []
    for date in date_range:
        # for first date in the range, we need to call the task directly
        if date == date_range[0]:
            fetch_and_store_task = create_fetch_and_store_task(date)
            first_date_dag = fetch_and_store_task(user_agent)
        else:
            fetch_and_store_task = create_fetch_and_store_task(date)
            first_date_dag >> fetch_and_store_task(user_agent)


def create_fetch_and_store_task(date):
    @task(task_id=f"fetch_and_store_exchange_rate_{date}")
    def fetch_and_store_exchange_rate(user_agent):
        logging.info("Fetching exchange rate for date: %s", date)
        df = fetch_exchange_rate(date, user_agent, PROXY_ADDRESSES)

        conn = create_duckdb_conn()
        query_conf = {"table_name": "exchange_rate"}
        conn.register(f"duckdb_{query_conf['table_name']}", df)

        if conn.sql(CHECK_TABLE_STMT.format(**query_conf)).df().shape[0] == 0:
            logging.info("Creating table")
            conn.sql(CREATE_TABLE_STMT.format(**query_conf))
        else:
            logging.info("Table already exists, inserting new data")
            new_records = conn.sql(SELECT_NEW_RECORDS_STMT.format(**query_conf)).df().shape[0]
            logging.info("New records to insert: %s", new_records)
            conn.sql(INSERT_TABLE_STMT.format(**query_conf))

    return fetch_and_store_exchange_rate


def create_duckdb_conn():
    mysql_conn = BaseHook.get_connection("mysql_default")
    conn = db.connect()
    conf = {
        "host": mysql_conn.host,
        "port": mysql_conn.port,
        "user": mysql_conn.login,
        "password": mysql_conn.password,
        "database": "demo_db",
    }
    conn.query(DUCKDB_CONF.format(**conf))
    return conn
