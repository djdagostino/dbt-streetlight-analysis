{{ config(materialized='view') }}

-- Connection facts for rental-light / streetlight equipment only ('zz' prefix).
-- Keeps all statuses (Active + history); the Active filter is applied downstream
-- in int_connection_information so historical analysis remains possible.

with src as (
    select *
    from {{ source('wge', 'CSM_Connection_Master') }}
    where left(Equipment, 2) = 'zz'
)

select
    cast(Location              as varchar(32))  as location_id,
    cast(Equipment             as varchar(32))  as equipment_id,
    cast([Connection Date]     as date)         as connection_date,
    cast([Disconnection Date]  as date)         as disconnection_date,
    cast([Connection Status]   as varchar(32))  as connection_status,
    cast(Consumption           as int)          as fixed_mult
from src
