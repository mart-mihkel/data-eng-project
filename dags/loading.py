import scipy
import duckdb
import numpy as np
import pandas as pd

from pymongo import MongoClient

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

FACT_TABLE = "accidents"
FACT_CSV = "accidents_clean.csv"
TIME_CSV = "time_dim_clean.csv"
WEATHER_CSV = "weather_clean.csv"
PARTIES_CSV = "parties_clean.csv"
LOCATION_CSV = "location_clean.csv"
ROAD_CSV = "road_clean.csv"

MONGO_CONNECTION_STRING = "mongodb://admin:admin@mongo:27017"
PROJECT_DB = "dataeng_project"
WEATHER_COLLECTION = "weather"
DENSITY_COLLECTION = "density"
ACCIDENT_COLLECTION = "accidents"

DUCK_DB = "duckdb/duck.db"
STATION_COORDS = [
    (58.945278, 23.555278),
    (58.036709, 24.458048),
    (58.749722, 26.415),
    (59.328889, 27.398333),
    (58.098611, 23.970278),
    (59.521389, 26.541389),
    (58.973056, 24.733889),
    (58.951111, 23.815556),
    (59.389444, 28.109167),
    (59.389444, 24.04),
    (58.384567, 24.485197),
    (58.404567, 24.505197),
    (58.920833, 22.066389),
    (58.218056, 22.506389),
    (57.783333, 23.258889),
    (57.913611, 22.058056),
    (59.398056, 24.602778),
    (58.264167, 26.461389),
    (58.865278, 26.952222),
    (58.872778, 26.272778),
    (58.808611, 25.409167),
    (57.79, 26.037778),
    (58.377778, 25.600278),
    (58.382778, 21.814167),
    (58.572778, 23.513611),
    (57.846389, 27.019444),
    (59.141389,26.230833)
]
STATIONS = np.array([
    "Haapsalu",
    "Haademeeste",
    "Jogeva",
    "Johvi",
    "Kihnu",
    "Kunda",
    "Kuusiku",
    "Laane-Nigula",
    "Narva",
    "Pakri",
    "Parnu",
    "Parnu-Sauga",
    "Ristna",
    "Roomassaare",
    "Ruhnu",
    "Sorve",
    "Tallinn-Harku",
    "Tartu-Toravere",
    "Tiirikoja",
    "Tooma",
    "Turi",
    "Valga",
    "Viljandi",
    "Vilsandi",
    "Virtsu",
    "Voru",
    "Vaike-Maarja"
])


def accident_to_parquet():
    client = MongoClient(MONGO_CONNECTION_STRING)
    coll = client[PROJECT_DB][ACCIDENT_COLLECTION]

    df = pd.DataFrame(coll.find()).drop(columns=["_id"]).dropna(subset=["X coordinate", "Y coordinate"])
    _, station_idx = scipy.spatial.KDTree(STATION_COORDS).query(df[["X coordinate", "Y coordinate"]])
    df["nearest_station"] = STATIONS[station_idx]

    df.to_parquet("/tmp/accidents.parquet")


def weather_to_parquet():
    client = MongoClient(MONGO_CONNECTION_STRING)
    coll = client[PROJECT_DB][WEATHER_COLLECTION]
    pd.DataFrame(coll.find()).to_parquet("/tmp/weather.parquet")


def density_to_parquet():
    client = MongoClient(MONGO_CONNECTION_STRING)
    coll = client[PROJECT_DB][DENSITY_COLLECTION]
    pd.DataFrame(coll.find()).to_parquet("/tmp/weather.parquet")


def create_schema():
    con = duckdb.connect(DUCK_DB)

    con.sql("""
        CREATE TABLE IF NOT EXISTS time_dim as (
            time_id                 INTEGER PRIMARY KEY,
            season                  VARCHAR,
            day_of_the_week         INTEGER,
            time_of_day             VARCHAR
        )""")

    con.sql("""
        CREATE TABLE IF NOT EXISTS location_dim as (
            location_id             INTEGER PRIMARY KEY, 
            gps_x                   DOUBLE,
            gps_y                   DOUBLE,
            urban                   BOOLEAN,
            country                 VARCHAR, 
            municipality            VARCHAR
        )""")

    con.sql("""
        CREATE TABLE IF NOT EXISTS weather_dim as (
            weather_id              INTEGER PRIMARY KEY,
            precipitation           INTEGER,
            temperature             INTEGER,
            snow_depth              INTEGER,
            relative_humidity       INTEGER,
            weather_code            INTEGER
        )""")

    con.sql("""
        CREATE TABLE IF NOT EXISTS road_dim as (
            road_id                 INTEGER PRIMARY KEY,
            road_geometry           VARCHAR,
            road_state_of_repair    VARCHAR,
            max_speed               INTEGER,
            highway_number          INTEGER,
            highway_km              DOUBLE,
            highway_cars_per_day    INTEGER
        )""")

    con.sql("""
        CREATE TABLE IF NOT EXISTS parties_dim as (
            parties_id CREATE                       INTEGER PRIMARY KEY,
            any_motor_vehicle_involved              BOOLEAN,
            cars_involved                           BOOLEAN,
            pedestrians_involved                    BOOLEAN,
            low_speed_vehicles_involved             BOOLEAN,
            elderly_driver_involved                 BOOLEAN,
            public_transportation_vehicle_involved  BOOLEAN,
            truck_involved                          BOOLEAN,
            motorcycle_involved                     BOOLEAN
        )""")

    con.sql(f"""
        CREATE TABLE IF NOT EXISTS {FACT_TABLE} as (
            id                          INTEGER PRIMARY KEY, 
            FOREIGN KEY (time_id)       REFERENCES time_dim (time_id),
            FOREIGN KEY (location_id)   REFERENCES location_dim (location_id), 
            FOREIGN KEY (weather_id)    REFERENCES weather_dim (weather_id), 
            FOREIGN KEY (parties_id)    REFERENCES parties_dim (parties_id),
            FOREIGN KEY (road_id)       REFERENCES road_dim (road_id),
            happened_at                 TIMESTAMP,
            number_of_people            INTEGER,
            number_of_vehicles          INTEGER,
            number_of_fatalities        INTEGER,
            number_of_injured           INTEGER
        )""")


def load_dimensions():
    con = duckdb.connect(DUCK_DB)
    con.sql(f"INSERT INTO road_dim SELECT * FROM {ROAD_CSV}")
    con.sql(f"INSERT INTO time_dim SELECT * FROM {TIME_CSV}")
    con.sql(f"INSERT INTO location_dim SELECT * FROM {LOCATION_CSV}")
    con.sql(f"INSERT INTO weather_dim SELECT * FROM {WEATHER_CSV}")
    con.sql(f"INSERT INTO parties_dim SELECT * FROM {PARTIES_CSV}")


def load_facts():
    con = duckdb.connect(DUCK_DB)
    con.sql(f"INSERT INTO {FACT_TABLE} SELECT * FROM {FACT_CSV}")


with DAG("transformation_etl", catchup=False) as dag:
    extract_accidents = PythonOperator(
        task_id="extract_accident_data_from_lake",
        python_callable=accident_to_parquet,
    )

    extact_weather = PythonOperator(
        task_id="extract_weather_data_from_lake",
        python_callable=weather_to_parquet,
    )

    extract_density = PythonOperator(
        task_id="extract_density_data_from_lake",
        python_callable=density_to_parquet,
    )

    t2 = PythonOperator(
        task_id="create_star_schema",
        python_callable=create_schema,
    )

    # t3 = PythonOperator(
    #     task_id="load_dimensions",
    #     python_callable=load_dimensions,
    # )
    #
    # t4 = PythonOperator(
    #     task_id="load_facts",
    #     python_callable=load_facts,
    # )
    #
    _ = [extract_accidents, extact_weather, extract_density] >> t2 # >> t3 >> t4
