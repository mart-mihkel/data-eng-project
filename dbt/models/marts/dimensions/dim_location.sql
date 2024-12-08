WITH raw_location_data AS (
    SELECT * FROM read_csv_auto('{{ var("location_csv") }}')
)
SELECT
    location_id,
    gps_x,
    gps_y,
    urban,
    country,
    municipality
FROM raw_location_data;
