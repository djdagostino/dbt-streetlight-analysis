{{ config(materialized='view') }}

-- Total darkness hours per month — the value that feeds the kWh formula
-- (Wattage * Hours_of_Darkness * FixedMult / 1000).
-- Matches the monthly totals shown in row 1 of the legacy Excel "Connection
-- Information" tab within rounding noise (~0.004 h).

select
    month_num,
    cast(sum(darkness_hours) as decimal(8,4)) as monthly_darkness_hours
from {{ ref('stg_hours_of_darkness_daily') }}
group by month_num
