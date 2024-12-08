WITH raw_time_data AS (
    SELECT * FROM read_csv_auto('{{ var("time_csv") }}')
)
SELECT
    time_id,
    season,
    day_of_week,
    time_of_day
FROM raw_time_data;
