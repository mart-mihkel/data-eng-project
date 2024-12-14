select
        a.case_id as id,
        case_id as time_id,
        case_id as location_id,
        case_id as weather_id,
        case_id as parties_id,
        case_id as road_id,
        a.num_people,
        a.num_vehicles,
        a.num_dead,
        a.num_injured
from accidents_tmp a
