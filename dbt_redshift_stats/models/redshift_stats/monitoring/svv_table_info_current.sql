-- models/svv_table_info_current.sql
{{ config(
    materialized = 'incremental',
    unique_key = 'database_name || schema_name || table_name'
) }}

SELECT
    * -- Select all columns as they are needed for the update
FROM
    {{ ref('svv_table_info_tmp') }}

{% if is_incremental() %}
-- Update logic to merge new data with existing records
WHERE
    last_analyzed_at > (
        SELECT
            MAX(last_analyzed_at)
        FROM
            {{ this }}
    )
{% endif %}
