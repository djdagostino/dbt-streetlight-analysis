{{ config(materialized='view') }}

-- Equipment Class lookup, scoped to rental-light / streetlight equipment.

with src as (
    select *
    from {{ source('wge', 'CSM_Equipment_Master') }}
    where left(Equipment, 2) = 'zz'
)

select
    cast(Equipment         as varchar(32))  as equipment_id,
    cast([Equipment Class] as varchar(64))  as equip_class
from src
