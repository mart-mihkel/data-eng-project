WITH accidents AS {{ source('dbt_source', 'accidents') }}

SELECT
        season,
        day_of_the_week,
        time_of_day
FROM accidents
