{{ config(materialized='table') }}

-- Streetlight estimated kWh for a single month, formatted for the MDM
-- Loss Analysis streetlight importer.
--
-- One row per non-rental streetlight. Every column is varchar(50) and
-- nullable, matching the rental importer's convention.
--
-- Account Number is a stable per-light identifier — wge_number + object_id_1
-- — since streetlights have no real account/meter number. The other
-- rental-only fields (Account Sub, Sequence, Rate Type, Disconnect Date) have
-- no streetlight source and are NULL. Substation / Feeder are parsed from the
-- GIS circuit code in stg_street_lights.
--
-- Month is selected by report_month() — default is the current calendar
-- month; override with `dbt run --vars 'report_month: <1-12>'`.

with street as (

    select *
    from {{ ref('int_street_light_monthly_kwh') }}
    where month_num = {{ report_month() }}

)

select
    cast(concat(wge_number, '-', object_id_1) as varchar(50)) as [Account Number],
    cast(null as varchar(50))                       as [Account Sub],
    cast(null as varchar(50))                       as [Sequence],
    convert(varchar(50), date_installed, 23)        as [Connect Date],
    cast(null as varchar(50))                       as [Disconnect Date],
    cast(substation as varchar(50))                 as [Substation],
    cast(feeder as varchar(50))                     as [Feeder],
    cast(null as varchar(50))                       as [Rate Type],
    cast(est_kwh as varchar(50))                    as [kWh],
    cast(wattage as varchar(50))                    as [Wattage]
from street
