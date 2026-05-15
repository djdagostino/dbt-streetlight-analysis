{{ config(materialized='table') }}

-- Rental-light estimated kWh for a single month, formatted for the MDM
-- Loss Analysis rental importer (already built by the vendor).
--
-- One row per active rental connection. Every column is varchar(50) and
-- nullable, matching the importer's fixed file spec.
--
-- Month is selected by report_month() — default is the previous calendar
-- month; override with `dbt run --vars 'report_month: <1-12>'`.
--
-- DisConnectionDate is passed through as stored, including the 1900-01-01
-- "still connected" sentinel, to match the files the importer was built on.

with rental as (

    select *
    from {{ ref('int_rental_light_monthly_kwh') }}
    where month_num = {{ report_month() }}

)

select
    cast(location_id as varchar(50))                as [LocationID],
    convert(varchar(50), connection_date, 23)       as [ConnectionDate],
    convert(varchar(50), disconnection_date, 23)    as [DisConnectionDate],
    cast(equipment_id as varchar(50))               as [EquipmentID],
    cast(rate1 as varchar(50))                      as [Rate1],
    cast(equip_class as varchar(50))                as [EquipClass],
    cast(wattage as varchar(50))                    as [Wattage],
    cast(est_kwh as varchar(50))                    as [KWH]
from rental
