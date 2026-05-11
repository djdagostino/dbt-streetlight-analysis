{{ config(materialized='view') }}

-- Connection facts for rental-light / streetlight equipment.
-- Raw layer (populated by ingest/sync_wge.py) is already filtered to 'zz' prefix
-- and pre-cast, so this stage is a thin pass-through. Active-only filtering
-- happens downstream in int_connection_information.

select
    location_id,
    equipment_id,
    connection_date,
    disconnection_date,
    connection_status,
    fixed_mult
from {{ source('wge', 'csm_connection_master') }}
