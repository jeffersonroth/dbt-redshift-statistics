-- models/monitoring/table_info.sql
{{ config(
    materialized = 'incremental',
    unique_key = 'database_name || schema_name || table_name',
    schema = 'monitoring',
    tags = ['redshift', 'monitoring', 'table_info'],
) }}

SELECT
    *
FROM
    {{ ref('table_info_tmp') }}

{% if is_incremental() %}
WHERE
    last_analyzed_at > (
        SELECT
            MAX(last_analyzed_at)
        FROM
            {{ this }}
    )
{% endif %}