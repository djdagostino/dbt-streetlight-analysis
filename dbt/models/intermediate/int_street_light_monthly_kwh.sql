{{ config(materialized='view') }}

-- Estimated monthly kWh for non-rental streetlights (sourced from the GIS
-- inventory seed). Tall: one row per (light, month).
--
-- Scope filter: subtype_cd in ('Streetlight', 'Decorative Streetlight').
-- Rental lights and off-street lights live in the seed too but are out of
-- scope here — rental kWh is handled by int_rental_light_monthly_kwh.
--
-- kWh formula, with fallback:
--   1. If kwh_hr is a valid per-hour kWh value (0 < kwh_hr <= 1):
--        est_kwh = kwh_hr * monthly_darkness_hours
--   2. Else if wattage is populated:
--        est_kwh = wattage * monthly_darkness_hours / 1000
--   3. Else NULL.
-- The (0, 1] guard rejects the source rows where wattage was keyed into the
-- kwh_hr column (values like 39, 129).

with street_lights as (

    select *
    from {{ ref('stg_street_lights') }}
    where subtype_cd in ('Streetlight', 'Decorative Streetlight')

),

monthly_dark as (

    select month_num, monthly_darkness_hours
    from {{ ref('int_hours_of_darkness_monthly') }}

)

select
    sl.object_id_1,
    sl.wge_number,
    sl.subtype_cd,
    sl.light_type,
    sl.date_installed,
    sl.substation,
    sl.feeder,
    sl.wattage,
    md.month_num,
    cast(round(
        case
            when sl.kwh_hr > 0 and sl.kwh_hr <= 1
                then sl.kwh_hr * md.monthly_darkness_hours
            when sl.wattage > 0
                then sl.wattage * md.monthly_darkness_hours / 1000.0
        end
    , 0) as int) as est_kwh
from street_lights as sl
cross join monthly_dark as md
