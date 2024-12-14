select
        case_id as id,
        county,
        is_settlement as urban,
        commune as municipality,
        village,
        address,
        x as gps_x,
        y as gps_y
from accidents_tmp
