WITH weather_dim AS {{ ref('weather_dim') }}
WITH location_dim AS {{ ref('location_dim') }}
WITH parties_dim AS {{ ref('parties_dim') }}
WITH road_dim AS {{ ref('road_dim') }}
WITH time_dim AS {{ ref('time_dim') }}

SELECT
        a.case_id AS id,
        t.id AS time_id,
        case_id AS location_id,
        case_id AS weather_id,
        case_id AS parties_id,
        case_id AS road_id,
        a.num_people, 
        a.num_vehicles,
        a.num_dead,
        a.num_injured
FROM accidents_tmp AS a
JOIN time_dim AS t
        ON t.year = a.year
        AND y.month = a.month
        AND y.day = a.day
