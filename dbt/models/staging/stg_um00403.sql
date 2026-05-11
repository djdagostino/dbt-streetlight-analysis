{{ config(materialized='view') }}

-- Rate-history rows for rental-light / streetlight equipment. Multiple rows
-- per equipment_id; the row with the highest connect_seq is the current rate.
-- Latest-rate selection happens in int_connection_information.

select
    location_id,
    equipment_id,
    connect_seq,
    rate1
from {{ source('wge', 'um00403') }}
