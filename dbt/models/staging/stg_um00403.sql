{{ config(materialized='view') }}

-- Rate-history rows for rental-light / streetlight equipment.
-- Multiple rows per equipment_id; the highest connect_seq is the current rate.
-- Latest-rate selection happens in int_connection_information.

with src as (
    select *
    from {{ source('wge', 'UM00403') }}
    where left(umEquipmentID, 2) = 'zz'
)

select
    cast(umLocationID  as varchar(32))  as location_id,
    cast(umEquipmentID as varchar(32))  as equipment_id,
    cast(umConnectSeq  as int)          as connect_seq,
    cast(UMTAR1        as varchar(16))  as rate1
from src
