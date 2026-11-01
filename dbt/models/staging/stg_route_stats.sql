{{
    config(
        materialized='table',
        table_type='hive',
        format='parquet',
        write_compression='zstd',
        external_location='s3://' ~ env_var('LAKE_BUCKET') ~ '/processed/stg_route_stats/'
    )
}}

{#-
    Only the latest ingested version, passed as a literal filter because
    Athena does not prune partitions through a subquery. Only the columns that
    name airlines and airports for now; the traffic figures (passengers,
    seats) stay in the raw layer until a model uses them.
-#}

{%- set raw = source('raw', 'raw_anac_stats') -%}
{%- set latest = none -%}

{%- if execute -%}
    {%- set latest_sql -%}
        select max(ingestion_date)
        from "{{ raw.schema }}"."{{ raw.identifier }}$partitions"
    {%- endset -%}
    {%- set latest = run_query(latest_sql).rows[0][0] -%}
{%- endif %}

-- empty strings mark missing values here, unlike the "null" text of the VRA
select
    nullif(airline_icao, '')          as airline_icao,
    nullif(airline_name, '')          as airline_name,
    nullif(airline_nationality, '')   as airline_nationality,
    cast(report_year as integer)      as report_year,
    cast(report_month as integer)     as report_month,
    nullif(origin_icao, '')           as origin_icao,
    nullif(origin_city, '')           as origin_city,
    nullif(origin_state, '')          as origin_state,
    nullif(origin_region, '')         as origin_region,
    nullif(origin_country, '')        as origin_country,
    nullif(origin_continent, '')      as origin_continent,
    nullif(destination_icao, '')      as destination_icao,
    nullif(destination_city, '')      as destination_city,
    nullif(destination_state, '')     as destination_state,
    nullif(destination_region, '')    as destination_region,
    nullif(destination_country, '')   as destination_country,
    nullif(destination_continent, '') as destination_continent,
    nullif(flight_nature, '')         as flight_nature,
    nullif(flight_group, '')          as flight_group,
    ingestion_date
from {{ raw }}
where ingestion_date = '{{ latest }}'
