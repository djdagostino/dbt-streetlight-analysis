{{ config(materialized='view') }}

-- GIS asset inventory of WGE streetlights (rental + non-rental).
-- Pass-through of the seed plus two cleanups:
--   1. Normalize the 'Off-Stree Light' typo from the source workbook.
--   2. Split circuit into substation (first 2 chars) and feeder (the rest).
-- Subtype-scope filtering (rental rows excluded) happens downstream in
-- int_street_light_monthly_kwh.

select
    object_id_1,
    object_id,
    street,
    kwh_hr,
    case
        when subtype_cd = 'Off-Stree Light' then 'Off-Street Light'
        else subtype_cd
    end                                       as subtype_cd,
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
