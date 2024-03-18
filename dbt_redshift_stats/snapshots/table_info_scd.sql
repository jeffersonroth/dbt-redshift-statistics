-- snapshots/monitoring/table_info_scd.sql
{% snapshot table_info_scd %}
    {% set source_table = ref('table_info') %}
    {{ config(
        target_schema = 'monitoring',
        unique_key = 'database_name || schema_name || table_name',
        sort = ['dbt_valid_to', 'database_name', 'schema_name', 'table_name'],
        strategy = 'check',
        check_cols = ['table_id', 'table_created_at', 'table_size_mb', 'table_size_percent', 'table_rows_estimate', 'table_unsorted_percent', 'table_skew_rows', 'table_max_varchar', 'table_dist_style', 'sortkey_num', 'sortkey1', 'sortkey1_enc', 'sortkey1_skew_ratio'],
        tags = ['redshift', 'monitoring', 'table_info', 'snapshot', 'scd'],
    ) }}

    SELECT
        database_name,
        schema_name,
        table_name,
        table_id,
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
        last_analyzed_at
    FROM
        {{ source(
            'monitoring',
            'table_info'
        ) }}
{% endsnapshot %}