{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        table_type='hive',
        format='parquet',
        write_compression='zstd',
        partitioned_by=['month'],
        external_location='s3://' ~ env_var('LAKE_BUCKET') ~ '/processed/stg_flights/'
    )
}}

{#-
    Each month is built from its latest ingested version, and only when that
    version is not in the table yet. The versions go into the query as literal
    filters because Athena does not prune partitions through a subquery.
    Quarantined months are skipped, and stg_flights_max_months caps how many
    months one run builds, oldest first; the next run picks up the rest.
-#}

{%- set raw = source('raw', 'raw_vra') -%}
{%- set versions = [] -%}
{%- set pending = [] -%}
{%- set max_months = var('stg_flights_max_months') | int -%}

{%- if execute -%}
    {%- set latest_sql -%}
        select "month", max(ingestion_date) as ingestion_date
        from "{{ raw.schema }}"."{{ raw.identifier }}$partitions"
        group by "month"
        order by "month"
    {%- endset -%}

    {%- set built = {} -%}
    {%- if is_incremental() -%}
        {%- set built_sql -%}
            select "month", max(ingestion_date) as ingestion_date
            from {{ this }}
            group by "month"
        {%- endset -%}
        {%- for row in run_query(built_sql) -%}
            {%- do built.update({row[0]: row[1]}) -%}
        {%- endfor -%}
    {%- endif -%}

    {%- for row in run_query(latest_sql) -%}
        {%- if built.get(row[0]) != row[1] and row[0] not in var('quarantined_months') -%}
            {%- do pending.append(row) -%}
        {%- endif -%}
    {%- endfor -%}

    {%- set versions = pending if max_months == 0 else pending[:max_months] -%}
    {%- if is_incremental() -%}
        {%- do log('stg_flights: building ' ~ versions | length ~ ' months, '
                   ~ (pending | length - versions | length) ~ ' left for later runs', info=True) -%}
    {%- endif -%}
{%- endif %}

with source as (

    select *
    from {{ raw }}
    where {% for version in versions -%}
        {% if not loop.first %}or {% endif %}("month" = '{{ version[0] }}' and ingestion_date = '{{ version[1] }}')
    {% else -%}
        false
    {%- endfor %}

),

cleaned as (

    -- ANAC writes missing values as the text "null" and the empty
    -- justification as "N/A"; scheduled times may carry a ".100000000"
    -- fraction, so only the first 19 characters are kept
    select
        nullif(airline_icao, 'null')                                       as airline_icao,
        nullif(flight_number, 'null')                                      as flight_number,
        nullif(authorization_code, 'null')                                 as authorization_code,
        nullif(line_type, 'null')                                          as line_type,
        nullif(origin_icao, 'null')                                        as origin_icao,
        nullif(destination_icao, 'null')                                   as destination_icao,
        cast(substr(nullif(scheduled_departure, 'null'), 1, 19) as timestamp) as scheduled_departure,
        cast(substr(nullif(actual_departure, 'null'), 1, 19) as timestamp)    as actual_departure,
        cast(substr(nullif(scheduled_arrival, 'null'), 1, 19) as timestamp)   as scheduled_arrival,
        cast(substr(nullif(actual_arrival, 'null'), 1, 19) as timestamp)      as actual_arrival,
        nullif(flight_status, 'null')                                      as flight_status,
        nullif(nullif(justification_code, 'null'), 'N/A')                  as justification_code,
        ingestion_date,
        "month"
    from source

)

-- the source repeats a few rows in full (6 in September 2026); records with
-- no departure time at all, 18 cancellations in 2019, cannot be placed in time
select distinct *
from cleaned
where scheduled_departure is not null or actual_departure is not null
