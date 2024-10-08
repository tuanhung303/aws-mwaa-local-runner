{% snapshot ss_exchange_rate %}

    {{
        config(
            target_schema="snapshots",
            strategy="check",
            check_cols=["rate"],
            unique_key="primary_key",
            table_type="iceberg",
            partitioned_by=["business_date"],
            table_properties={
                "optimize_rewrite_delete_file_threshold": "2",
                "vacuum_max_snapshot_age_seconds": "60",
                "optimize_rewrite_delete_file_threshold": "2",
                "optimize_rewrite_data_file_threshold": "2",
                "vacuum_min_snapshots_to_keep": "2",
            },
            post_hook=[
                "VACUUM {{ nq( this ) }};",
                "OPTIMIZE {{ nq( this ) }}  REWRITE DATA USING BIN_PACK;",
            ],
        )
    }}

    select
        num_code,
        char_code,
        unit,
        currency,
        rate,
        business_date,
        primary_key,
        surrogate_key
    from {{ source("mysql", "exchange_rate") }}

{% endsnapshot %}
