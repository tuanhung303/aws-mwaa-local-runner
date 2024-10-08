DUCKDB_CONF = """
INSTALL mysql;LOAD mysql;SET TimeZone = 'UTC';
ATTACH 'host={host} user={user} password={password} port={port} database={database}' AS mysql_db (TYPE mysql_scanner);
USE mysql_db;
"""

CHECK_TABLE_STMT = """
use mysql_db;
SELECT * FROM information_schema.tables WHERE table_name = '{table_name}';
"""

CREATE_TABLE_STMT = """
use mysql_db;
CREATE TABLE IF NOT EXISTS {table_name} as 
SELECT
*,
md5(concat(char_code, business_date)) as primary_key,
md5(concat(char_code, rate, business_date)) as surrogate_key
FROM duckdb_{table_name}
"""

SELECT_NEW_RECORDS_STMT = """
use mysql_db;
with cte as (
    select
    *,
    md5(concat(char_code, business_date)) as primary_key,
    md5(concat(char_code, rate, business_date)) as surrogate_key
    from duckdb_{table_name} a
)
select * from cte a
where not exists (select 1 from {table_name} where {table_name}.primary_key = a.primary_key)
"""

INSERT_TABLE_STMT = """
use mysql_db;
INSERT INTO {table_name}
with cte as (
    select
    *,
    md5(concat(char_code, business_date)) as primary_key,
    md5(concat(char_code, rate, business_date)) as surrogate_key
    from duckdb_{table_name} a
)
select * from cte a
where not exists (select 1 from {table_name} where {table_name}.primary_key = a.primary_key)
"""


PROXY_ADDRESSES = {"http": "http://116.125.141.115:80", "https": "35.220.254.137:8080"}

USER_AGENT_LIST = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15",
]
