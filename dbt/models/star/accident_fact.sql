with weather_dim as {{ ref('weather_dim') }}
with location_dim as {{ ref('location_dim') }}
with parties_dim as {{ ref('parties_dim') }}
with road_dim as {{ ref('road_dim') }}
with time_dim as {{ ref('time_dim') }}

select
        a.case_id as id,
        t.id as time_id,
        case_id as location_id,
        case_id as weather_id,
        case_id as parties_id,
        case_id as road_id,
        a.num_people,
        a.num_vehicles,
        a.num_dead,
        a.num_injured
from accidents_tmp as a
join time_dim as t
        using t.year = a.year
        and y.month = a.month
        and y.day = a.day
