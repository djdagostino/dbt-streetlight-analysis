{{ config(materialized='view') }}

-- GIS asset inventory of WGE streetlights (rental + non-rental).
-- Pass-through of the seed, plus splitting circuit into substation (first
-- 2 chars) and feeder (the rest). Subtype-scope filtering happens downstream
-- in int_street_light_monthly_kwh.

select
    object_id_1,
    object_id,
    street,
    kwh_hr,
    subtype_cd,
    manufacturer,
    model,
    light_type,
    image_path,
    date_installed,
    wge_number,
    latitude,
    longitude,
    pole,
    lumens,
    wattage,
    comments,
    bracket_type,
    banners,
    circuit,
    case when len(circuit) >= 2
        then left(circuit, 2)
    end                                       as substation,
    case when len(circuit) > 2
        then substring(circuit, 3, len(circuit) - 2)
    end                                       as feeder
from {{ ref('street_lights') }}
