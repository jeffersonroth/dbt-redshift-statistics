-- models/svv_table_info_history.sql
{{ config(
    materialized = 'table'
) }}

SELECT
    database_name,
    schema_name,
    table_name,
    table_id,
    table_statistics_freshness,
    table_vacuum_sort_benefit_percent,
    table_created_at,
    table_size_mb,
    table_size_percent,
    table_rows_estimate,
    table_unsorted_percent,
    table_skew_rows,
    table_max_varchar,
    table_dist_style,
    sortkey_num,
    sortkey1,
    sortkey1_enc,
    sortkey1_skew_ratio,
    last_analyzed_at,
    CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AS valid_from -- Add a timestamp column to indicate when the record was inserted
FROM
    {{ ref('svv_table_info_tmp') }}
