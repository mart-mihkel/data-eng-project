SELECT
        case_id as id,
        case_id as time_id,
        case_id as location_id,
        case_id as weather_id,
        case_id as parties_id,
        case_id as road_id,
        time as happened_at,
        num_people as number_of_people,
        num_vehicles as number_of_vehicles,
        num_dead as number_of_fatalities,
        num_injured as number_of_injured
FROM accidents_tmp
