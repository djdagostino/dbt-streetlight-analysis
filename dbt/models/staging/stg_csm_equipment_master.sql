{{ config(materialized='view') }}

-- Equipment Class lookup for rental-light / streetlight equipment.

select
    equipment_id,
    equip_class
from {{ source('wge', 'csm_equipment_master') }}
