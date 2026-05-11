{{ config(materialized='table') }}

-- Active rental-light / streetlight estimated monthly kWh, wide format.
-- One row per (LocationID, EquipmentID); twelve monthly kWh columns.
-- Replaces the legacy "Connection Information" tab in the Excel workbook
-- (Rental Light Customers as of 02-11-2026 WORK.xlsx).
--
-- Column names preserve the legacy Excel headers (PascalCase, spaces,
-- hyphens) for drop-in compatibility with downstream MDM Loss Analysis.

with conn as (

    select * from {{ ref('int_connection_information') }}

),

rate_wattage as (

    select bill_rate, wattage
    from {{ ref('stg_rate_wattage') }}

),

monthly_dark as (

    select month_num, monthly_darkness_hours
    from {{ ref('int_hours_of_darkness_monthly') }}

),

conn_w as (

    -- attach wattage via the Rate1 lookup
    select
        c.location_id,
        c.connection_date,
        c.disconnection_date,
        c.equipment_id,
        c.fixed_mult,
        c.rate1,
        c.equip_class,
        rw.wattage
    from conn c
    left join rate_wattage rw
        on rw.bill_rate = c.rate1

),

monthly as (

    -- 12 rows per light, one per calendar month, with rounded kWh
    select
        cw.*,
        md.month_num,
        cast(round(cw.wattage * md.monthly_darkness_hours * cw.fixed_mult / 1000.0, 0) as int) as est_kwh
    from conn_w cw
    cross join monthly_dark md

)

select
    location_id                                       as [LocationID],
    connection_date                                   as [ConnectionDate],
    disconnection_date                                as [DisconnectionDate],
    equipment_id                                      as [EquipmentID],
    cast(fixed_mult as decimal(10,5))                 as [Fixed Mult (Rate)],
    rate1                                             as [Rate1],
    equip_class                                       as [EquipClass],
    wattage                                           as [Wattage],
    max(case when month_num =  1 then est_kwh end)    as [Jan-EstkWh],
    max(case when month_num =  2 then est_kwh end)    as [Feb-EstkWh],
    max(case when month_num =  3 then est_kwh end)    as [Mar-EstkWh],
    max(case when month_num =  4 then est_kwh end)    as [Apr-EstkWh],
    max(case when month_num =  5 then est_kwh end)    as [May-EstkWh],
    max(case when month_num =  6 then est_kwh end)    as [Jun-EstkWh],
    max(case when month_num =  7 then est_kwh end)    as [Jul-EstkWh],
    max(case when month_num =  8 then est_kwh end)    as [Aug-EstkWh],
    max(case when month_num =  9 then est_kwh end)    as [Sep-EstkWh],
    max(case when month_num = 10 then est_kwh end)    as [Oct-EstkWh],
    max(case when month_num = 11 then est_kwh end)    as [Nov-EstkWh],
    max(case when month_num = 12 then est_kwh end)    as [Dec-EstkWh]
from monthly
group by
    location_id,
    connection_date,
    disconnection_date,
    equipment_id,
    fixed_mult,
    rate1,
    equip_class,
    wattage
