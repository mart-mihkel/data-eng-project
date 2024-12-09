SELECT
        w.hourly_precipitation_total as precipitation,
        w.hourly_maximum_wind_speed as wind_speed,
        w.air_temperature as temperature,
        w.relative_humidity
FROM accidents_tmp AS a
LEFT JOIN weather_tmp AS w 
        ON a.nearest_station = w.station 
        AND a.year = w.year
        AND a.month = w.month
        AND a.day = w.day
