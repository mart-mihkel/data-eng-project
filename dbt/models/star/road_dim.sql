WITH accidents AS {{ source('dbt_source', 'accidents') }}
WITH density AS {{ source('dbt_source', 'accidents') }}

SELECT
        a.road_hill_type as road_geometry,
        a.road_condition as road_state_of_repair,
        a.speed_limit,
        a.road_number as highway_number,
        a.road_kilometer as highway_km,
        a.road_number as highway_number,
        d.AADT_vehicles_per_day as highway_cars_per_day
FROM accidents AS a
LEFT JOIN density AS d ON d.road_number = a.road_number
