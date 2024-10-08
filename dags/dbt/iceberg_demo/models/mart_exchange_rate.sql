{{
    config(
        materialized="table",
        table_type="iceberg",
        format="parquet",
        partitioned_by=["business_date", "bucket(currency, 5)"],
        table_properties={"optimize_rewrite_delete_file_threshold": "2"},
    )
}}

with
    rate as (
        select * from {{ ref("ss_exchange_rate") }} er where er.dbt_valid_to is null
    ),
    rate_changes as (
        select
            num_code,
            char_code,
            unit,
            currency,
            rate,
            business_date,
            surrogate_key,
            primary_key,
            lag(rate) over (
                partition by currency order by business_date
            ) as previous_rate_d1,
            lag(rate, 3) over (
                partition by currency order by business_date
            ) as previous_rate_d3,
            lag(rate, 7) over (
                partition by currency order by business_date
            ) as previous_rate_d7
        from rate a
    ),
    fluctuations as (
        select
            num_code,
            char_code,
            unit,
            currency,
            rate,
            business_date,
            surrogate_key,
            primary_key,
            rate - previous_rate_d1 as rate_fluctuation,
            rate - previous_rate_d3 as rate_fluctuation_3d,
            rate - previous_rate_d7 as rate_fluctuation_7d,
            rate / previous_rate_d1 as rate_fluctuation_ratio,
            rate / previous_rate_d3 as rate_fluctuation_ratio_3d,
            rate / previous_rate_d7 as rate_fluctuation_ratio_7d
        from rate_changes
        where previous_rate_d1 is not null
    )
select
    num_code,
    char_code,
    unit,
    rate,
    surrogate_key,
    primary_key,
    rate_fluctuation,
    rate_fluctuation_3d,
    rate_fluctuation_7d,
    rate_fluctuation_ratio,
    rate_fluctuation_ratio_3d,
    rate_fluctuation_ratio_7d,
    business_date,
    currency
from fluctuations
;
