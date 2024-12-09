WITH accidents AS {{ source('dbt_source', 'accidents') }}

SELECT
        case_id as id,
        county,
        is_settlement AS urban,
        commune AS municipality,
        village,
        x AS gps_x,
        y AS gps_y
FROM accidents
