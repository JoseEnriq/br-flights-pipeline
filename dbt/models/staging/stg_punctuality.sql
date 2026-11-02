{{
    config(
        materialized='table',
        table_type='hive',
        format='parquet',
        write_compression='zstd',
        external_location='s3://' ~ env_var('LAKE_BUCKET') ~ '/processed/stg_punctuality/'
    )
}}

{#-
    The latest ingested version of each month, passed as literal filters
    because Athena does not prune partitions through a subquery. A full
    rebuild: all months together are a few MB.
-#}

{%- set raw = source('raw', 'raw_anac_punctuality') -%}
{%- set versions = [] -%}

{%- if execute -%}
    {%- set latest_sql -%}
        select "month", max(ingestion_date) as ingestion_date
        from "{{ raw.schema }}"."{{ raw.identifier }}$partitions"
        group by "month"
    {%- endset -%}
    {%- for row in run_query(latest_sql) -%}
        {%- do versions.append(row) -%}
    {%- endfor -%}
{%- endif %}

-- the airline arrives as "AAL - AMERICAN AIRLINES, INC." and percentages use
-- a decimal comma
select
    split_part(airline, ' - ', 1)                                    as airline_icao,
    substr(airline, strpos(airline, ' - ') + 3)                      as airline_name,
    flight_number,
    origin_icao,
    destination_icao,
    cast(scheduled_legs as integer)                                  as scheduled_flights,
    cast(replace(nullif(pct_cancelled, ''), ',', '.') as decimal(5, 2))  as pct_cancelled,
    cast(replace(nullif(pct_delayed_30, ''), ',', '.') as decimal(5, 2)) as pct_delayed_30,
    cast(replace(nullif(pct_delayed_60, ''), ',', '.') as decimal(5, 2)) as pct_delayed_60,
    ingestion_date,
    "month"
from {{ raw }}
where {% for version in versions -%}
    {% if not loop.first %}or {% endif %}("month" = '{{ version[0] }}' and ingestion_date = '{{ version[1] }}')
{% else -%}
    false
{%- endfor %}
