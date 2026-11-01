-- Codes that fly but are missing from ANAC's statistics stay in the
-- dimensions without a name; this lists them (4 airlines and 9 airports in
-- the first 12 months loaded, together under 50 flights).
{{ config(severity='warn') }}

select 'airline' as kind, airline_icao as code
from {{ ref('dim_airline') }}
where airline_name is null

union all

select 'airport', airport_icao
from {{ ref('dim_airport') }}
where city is null
