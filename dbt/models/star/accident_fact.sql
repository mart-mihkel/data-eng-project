WITH accidents AS {{ source('dbt_source', 'accidents') }}

SELECT
  case_id as id,
  time as happened_at,
  num_people as number_of_people,
  num_vehicles as number_of_vehicles,
  num_dead as number_of_fatalities,
  num_injured as number_of_injured
FROM accidents
