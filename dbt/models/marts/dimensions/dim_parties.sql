WITH raw_parties_data AS (
    SELECT * FROM read_csv_auto('{{ var("parties_csv") }}')
)
SELECT
    parties_id,
    any_motor_vehicle_involved,
    cars_involved,
    pedestrians_involved,
    low_speed_vehicles_involved,
    elderly_driver_involved,
    public_transportation_vehicle_involved,
    truck_involved,
    motorcycle_involved
FROM raw_parties_data;
