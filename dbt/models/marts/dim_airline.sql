{{
    config(
        materialized='table',
        table_type='hive',
        format='parquet',
        write_compression='zstd',
        external_location='s3://' ~ env_var('LAKE_BUCKET') ~ '/curated/dim_airline/'
    )
}}

with named as (

    -- names change over the years; each airline keeps the latest one
    select
        airline_icao,
        airline_name,
        airline_nationality,
        row_number() over (
            partition by airline_icao
            order by report_year desc, report_month desc, airline_name
        ) as row_rank
    from {{ ref('stg_route_stats') }}
    where airline_icao is not null

),

flown as (

    select distinct airline_icao
    from {{ ref('stg_flights') }}

)

-- every airline that flies gets a row, named or not, so joining the fact
-- table to this one never drops a flight
select
    coalesce(named.airline_icao, flown.airline_icao) as airline_icao,
    named.airline_name,
    named.airline_nationality,
    named.airline_nationality = 'BRASILEIRA'         as is_brazilian
from (select * from named where row_rank = 1) as named
full outer join flown
    on named.airline_icao = flown.airline_icao
