WITH accidents AS {{ source('dbt_source', 'accidents') }}

SELECT
        speed_limit,
        road_condition as road_state_of_repair
FROM accidents
