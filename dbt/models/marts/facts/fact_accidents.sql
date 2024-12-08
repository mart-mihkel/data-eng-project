WITH raw_data AS (
    SELECT * FROM read_csv_auto('{{ var("fact_csv") }}')
)
SELECT
    id,
    time_id,
    location_id,
    weather_id,
    happened_at,
    number_of_people,
    number_of_vehicles,
    number_of_fatalities,
    number_of_injured
FROM raw_data;
