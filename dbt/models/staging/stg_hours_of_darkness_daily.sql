{{ config(materialized='view') }}

-- Daily duration of darkness at Westfield, MA, per the U.S. Naval Observatory.
-- One row per (month_num, day_num). Year-agnostic: latitude doesn't change, so
-- this pattern holds for any year (excluding leap-day refinements).

select
    cast(month_num      as tinyint)        as month_num,
    cast(day_num        as tinyint)        as day_num,
    cast(darkness_hours as decimal(6,4))   as darkness_hours
from {{ ref('hours_of_darkness_daily') }}
