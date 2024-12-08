WITH raw_road_data AS (
    SELECT * FROM read_csv_auto('{{ var("road_csv") }}')
)
SELECT
    road_id,
    road_geometry,
    road_state_of_repair,
    max_speed,
    highway_number,
    highway_km,
    highway_cars_per_day
FROM raw_road_data;
