WITH accidents AS {{ source('dbt_source', 'accidents') }}

SELECT
        county,
        is_settlement AS urban, -- TODO: map 'JAH' 'EI' to bool
        commune AS municipality,
        village,
        x AS gps_x,
        y AS gps_y 
FROM accidents
