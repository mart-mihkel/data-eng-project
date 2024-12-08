WITH raw_weather_data AS (
    SELECT * FROM read_csv_auto('{{ var("weather_csv") }}')
)
SELECT
    weather_id,
    precipitation,
    temperature,
    snow_depth,
    relative_humidity,
    weather_code
FROM raw_weather_data;
