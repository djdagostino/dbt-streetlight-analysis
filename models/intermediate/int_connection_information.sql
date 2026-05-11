{{ config(materialized='view') }}

-- Active rental-light / streetlight connections with their currently-effective
-- rate code. dbt equivalent of the legacy "active streetlights" UIS query.
--
-- Logic mirrors that query exactly:
--   1. Restrict CSM_Connection_Master to status = 'Active'.
--   2. For each equipment, pick the UM00403 row with the highest umConnectSeq
--      (latest rate) — but only consider rate rows whose (location, equipment)
--      matches an Active connection (the inner join in the original CTE).
--   3. Left-join equipment-class lookup; left-join the current rate.

with active_connections as (

    select *
    from {{ ref('stg_csm_connection_master') }}
    where connection_status = 'Active'

),

rate_history_scoped as (

    select
        r.location_id,
        r.equipment_id,
        r.connect_seq,
        r.rate1,
        row_number() over (
            partition by r.equipment_id
            order by r.connect_seq desc
        ) as rate_recency_rn
    from {{ ref('stg_um00403') }} as r
    inner join active_connections as ac
        on  r.location_id  = ac.location_id
        and r.equipment_id = ac.equipment_id

),

current_rate as (

    select equipment_id, rate1
    from rate_history_scoped
    where rate_recency_rn = 1

)

select
    ac.location_id,
    ac.equipment_id,
    ac.connection_date,
    ac.disconnection_date,
    ac.fixed_mult,
    cr.rate1,
    em.equip_class
from active_connections as ac
left join {{ ref('stg_csm_equipment_master') }} as em
    on em.equipment_id = ac.equipment_id
left join current_rate as cr
    on cr.equipment_id = ac.equipment_id
