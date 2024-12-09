WITH accidents AS {{ source('dbt_source', 'accidents') }}

SELECT
  id,
  season,
  day_of_the_week,
  time_of_day
from accidents
