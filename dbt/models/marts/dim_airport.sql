{{
    config(
        materialized='table',
        table_type='hive',
        format='parquet',
        write_compression='zstd',
        external_location='s3://' ~ env_var('LAKE_BUCKET') ~ '/curated/dim_airport/'
    )
}}

with sides as (

    select
        origin_icao      as airport_icao,
        origin_city      as city,
        origin_state     as state,
        origin_region    as region,
        origin_country   as country,
        origin_continent as continent,
        report_year,
        report_month
    from {{ ref('stg_route_stats') }}

    union all

    select
        destination_icao,
        destination_city,
        destination_state,
        destination_region,
        destination_country,
        destination_continent,
        report_year,
        report_month
    from {{ ref('stg_route_stats') }}

),

named as (

    select
        *,
        row_number() over (
            partition by airport_icao
            order by report_year desc, report_month desc, city
        ) as row_rank
    from sides
    where airport_icao is not null

),

flown as (

    select origin_icao as airport_icao from {{ ref('stg_flights') }}
    union
    select destination_icao from {{ ref('stg_flights') }}

)

-- ANAC's "airport name" is the city it serves (SBSP and SBGR read SÃO PAULO
-- and GUARULHOS), hence city; every airport that appears in a flight gets a
-- row, named or not
select
    coalesce(named.airport_icao, flown.airport_icao) as airport_icao,
    named.city,
    named.state,
    named.region,
    named.country,
    named.continent,
    named.country = 'BRASIL'                         as is_brazilian
from (select * from named where row_rank = 1) as named
full outer join flown
    on named.airport_icao = flown.airport_icao
