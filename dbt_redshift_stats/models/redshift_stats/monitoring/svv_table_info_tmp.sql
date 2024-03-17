-- models/svv_table_info_tmp.sql
{{ config(
    materialized = 'ephemeral'
) }}

{% set current_env = target.env if target.env else env_var(
    'DBT_ENV',
    'prod'
) %}
{% set schema_list = var('monitored_schemas') ['schemas'] %}
{% set mock_id = var(
    'mock_id',
    ''
) %}
-- Retrieve the variable value with a default

SELECT
    "database" :: VARCHAR(255) AS database_name,
    "schema" :: VARCHAR(255) AS schema_name,
    "table" :: VARCHAR(255) AS table_name,
    "table_id" :: bigint AS table_id,
    "stats_off" :: DECIMAL(
        5,
        2
    ) AS table_statistics_freshness,
    "vacuum_sort_benefit" :: DECIMAL(
        12,
        2
    ) AS table_vacuum_sort_benefit_percent,
    "create_time" :: TIMESTAMP without TIME ZONE AS table_created_at,
    "size" :: bigint AS table_size_mb,
    "pct_used" :: DECIMAL(
        10,
        4
    ) AS table_size_percent,
    "estimated_visible_rows" :: bigint AS table_rows_estimate,
    "unsorted" :: DECIMAL(
        5,
        2
    ) AS table_unsorted_percent,
    "skew_rows" :: DECIMAL(
        19,
        2
    ) AS table_skew_rows,
    "max_varchar" :: INT AS table_max_varchar,
    "diststyle" :: VARCHAR(50) AS table_dist_style,
    "sortkey_num" :: INT AS sortkey_num,
    "sortkey1" :: VARCHAR(255) AS sortkey1,
    "sortkey1_enc" :: VARCHAR(32) AS sortkey1_enc,
    "skew_sortkey1" :: DECIMAL(
        19,
        2
    ) AS sortkey1_skew_ratio,
    {% if current_env == 'dev' %}
        "mock_created_at" :: TIMESTAMP without TIME ZONE AS last_analyzed_at
    {% else %}
        CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AS last_analyzed_at
    {% endif %}
FROM
    {% if current_env == 'dev' %}
        {{ source(
            'mock_system',
            'mock_svv_table_info'
        ) }}
        -- Reference to a mock table or a different source in dev environment
    {% else %}
        {{ source(
            'redshift_system',
            'svv_table_info'
        ) }}
        -- Actual Redshift system table in prod environment
    {% endif %}
WHERE
    "schema" IN ({% for schema in schema_list %}
        '{{ schema }}' {% if not loop.last %},
        {% endif %}
    {% endfor %}) {% if current_env == 'dev' %}
        AND "mock_id" = '{{ mock_id }}'::BIGINT -- Additional filter for dev environment
    {% endif %}
ORDER BY
    database_name,
    schema_name,
    table_name
