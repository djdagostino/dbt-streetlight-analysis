{{ config(materialized='view') }}

-- Estimated monthly kWh for rental lights. Tall: one row per
-- (active rental connection, month).
--
-- kWh formula:
--   est_kwh = wattage * monthly_darkness_hours * fixed_mult / 1000
-- where wattage comes from the rate_wattage seed via the connection's
-- current rate code (rate1), and fixed_mult counts how many lights share
-- this single connection.

with rental_connections as (

    select *
    from {{ ref('int_connection_information') }}

),

rate_wattage as (

    select bill_rate, wattage
    from {{ ref('stg_rate_wattage') }}

),

monthly_dark as (

    select month_num, monthly_darkness_hours
    from {{ ref('int_hours_of_darkness_monthly') }}

),

rentals_with_wattage as (

    select
        rc.location_id,
        rc.equipment_id,
        rc.connection_date,
        rc.disconnection_date,
        rc.fixed_mult,
        rc.rate1,
        rc.equip_class,
        rw.wattage
    from rental_connections as rc
    left join rate_wattage as rw
        on rw.bill_rate = rc.rate1

)

select
    rw.location_id,
    rw.equipment_id,
    rw.connection_date,
    rw.disconnection_date,
    rw.fixed_mult,
    rw.rate1,
    rw.equip_class,
    rw.wattage,
    md.month_num,
    cast(round(
        rw.wattage * md.monthly_darkness_hours * rw.fixed_mult / 1000.0
    , 0) as int) as est_kwh
from rentals_with_wattage as rw
cross join monthly_dark as md
