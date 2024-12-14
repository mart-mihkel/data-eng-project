select
        a.case_id as id,
        w.hourly_precipitation_total as precipitation,
        w.hourly_maximum_wind_speed as wind_speed,
        w.air_temperature as temperature,
        w.relative_humidity
from accidents_tmp a
left join weather_tmp w
        on a.nearest_station = w.station
        and a.year = w.year
        and a.month = w.month
        and a.day = w.day
        and a.hour = w.hour
