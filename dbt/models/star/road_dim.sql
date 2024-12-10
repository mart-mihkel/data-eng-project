select
        a.case_id as id,
        a.road_hill_type as road_geometry,
        a.road_condition as road_state_of_repair,
        a.speed_limit,
        a.road_number as highway_number,
        a.road_kilometer as highway_km,
        a.road_number as highway_number,
        d.AADT_vehicles_per_day as highway_cars_per_day
from accidents_tmp a
left join density_tmp d
        on d.road_number = a.road_number 
        and a.road_kilometer*1000 between d.start and d.end
