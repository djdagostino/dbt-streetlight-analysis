{{ config(materialized='view') }}

with src as (
    select * from {{ ref('rate_wattage') }}
)

select
    cast(BillRate       as varchar(16))  as bill_rate,
    cast(ESRate         as varchar(16))  as es_rate,
    cast(LightType      as varchar(64))  as light_type,
    cast(Wattage        as int)          as wattage,
    cast(NominalWattage as int)          as nominal_wattage
from src
