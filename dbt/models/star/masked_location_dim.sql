{{ config(materialized='view') }}

SELECT
    id,
    county,
    urban,
    municipality,
    village,
    '#####' AS address,
    '#####' AS gps_x,
    '#####' AS gps_y
FROM {{ ref('location_dim') }}
