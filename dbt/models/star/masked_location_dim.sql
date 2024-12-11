SELECT
    id,
    county,
    urban,
    municipality,
    village,
    NULL AS gps_x,  -- Mask sensitive fields
    NULL AS gps_y   -- Mask sensitive fields
FROM {{ ref('location_dim') }}
