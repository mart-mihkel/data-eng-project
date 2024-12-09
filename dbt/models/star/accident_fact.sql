SELECT
        case_id AS id,
        case_id AS time_id,
        case_id AS location_id,
        case_id AS weather_id,
        case_id AS parties_id,
        case_id AS road_id,
        time AS happened_at,
        num_people AS number_of_people,
        num_vehicles AS number_of_vehicles,
        num_dead AS uumber_of_fatalities,
        num_injured AS number_of_injured
FROM accidents_tmp
