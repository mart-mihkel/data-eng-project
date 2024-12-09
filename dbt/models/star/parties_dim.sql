SELECt
        case_id as id,
        motor_vehicle_driver_involved as any_motor_vehicle_involved,
        car_driver_involved as cars_involved,
        pedestrian_involved,
        light_vehicle_driver_involved as low_speed_vehicles_involved,
        elder_driver_involved as elderly_driver_involved,
        public_transport_driver_involved as public_transportation_involved,
        truck_driver_involved as truck_involved,
        motorcyclist_involved as motorcycle_involved
FROM accidents_tmp
