import duckdb
import requests
import pandas as pd
from pymongo import MongoClient

from pyproj import Transformer
from scipy import spatial

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
DUCK_DB = ""

PROJECT_DB = "dataeng_project"
WEATHER_COLLECTION = "weather"
ACCIDENT_COLLECTION = "accidents"


def get_all_data():
  
  coordinates_stations = [(58.945278,23.555278),(58.036709,24.458048),(58.749722,26.415),(59.328889,27.398333),(58.098611,23.970278),(59.521389,26.541389),(58.973056,24.733889),(58.951111,23.815556),(59.389444,28.109167),(59.389444,24.04),(58.384567,24.485197),(58.404567,24.505197),(58.920833,22.066389),(58.218056,22.506389),(57.783333,23.258889),(57.913611,22.058056),(59.398056,24.602778),(58.264167,26.461389),(58.865278,26.952222),(58.872778,26.272778),(58.808611,25.409167),(57.79,26.037778),(58.377778,25.600278),(58.382778,21.814167),(58.572778,23.513611),(57.846389,27.019444),(59.141389,26.230833)]
  stations = [ "Haapsalu", "Haademeeste", "Jogeva","Johvi","Kihnu","Kunda","Kuusiku","Laane-Nigula","Narva","Pakri","Parnu","Parnu-Sauga","Ristna","Roomassaare","Ruhnu","Sorve","Tallinn-Harku","Tartu-Toravere","Tiirikoja","Tooma","Turi","Valga","Viljandi","Vilsandi","Virtsu","Voru","Vaike-Maarja"]
  tree = spatial.KDTree(coordinates_stations)

  myclient = MongoClient(MONGO_CONNECTION_STRING)
  my_db = myclient[PROJECT_DB]
  weather_coll = my_db[WEATHER_COLLECTION]

  print(list(weather_coll.find({"Year":2024, "Month": 1})))

  accident_coll = my_db[ACCIDENT_COLLECTION]
  cursor = accident_coll.find({"Year":2024})

  accident_data = pd.DataFrame(list(cursor))

  print(accident_data["Time of accident"].str.split(" ")[0])
  return 
  print("Retrieved accident data from mongodb")

  for index, row in accident_data.iterrows():
     if(index % 10000 == 0):
        print(index)
     if(pd.isna(row["X coordinate"]) or pd.isna(row["Y coordinate"])):
        continue
     print(row)
     _ , station_index = tree.query([(row["X coordinate"], row["Y coordinate"])])
     station = stations[station_index[0]]
     timestamp = pd.to_datetime(row["Time of accident"])
     print(timestamp.day)
     print(timestamp.month)
     print(station)
     print(row["Year"])
     accident_weather = weather_coll.find({"Year":row["Year"],"Month":timestamp.month,"Day":timestamp.day, "Location":station})
     accident_weather = list(accident_weather)
     if(len(accident_weather)<1):
        continue
     print(accident_weather)
     break


def load_dimensions_to_warehouse():


  con = duckdb.connect(DUCK_DB)

  con.sql("""
        CREATE TABLE IF NOT EXISTS time_dim as (
        time_id INTEGER PRIMARY KEY,
        season VARCHAR,
        day_of_the_week INTEGER,
        time_of_day VARCHAR
        )
        """)
  con.sql("INSERT INTO time_dim SELECT * FROM {TIME_CSV}")

  con.sql("""
        CREATE TABLE IF NOT EXISTS location_dim as (
        location_id INTEGER PRIMARY KEY,
        gps_x DOUBLE,
        gps_y DOUBLE,
        urban BOOLEAN,
        country VARCHAR,
        municipality VARCHAR
        )
        """)
  con.sql("INSERT INTO location_dim SELECT * FROM {LOCATION_CSV}")

  con.sql("""
        CREATE TABLE IF NOT EXISTS weather_dim as (
        weather_id INTEGER PRIMARY KEY,
        precipitation INTEGER,
        temperature INTEGER,
        snow_depth INTEGER,
        relative_humidity INTEGER,
        weather_code INTEGER
        )
        """)
  con.sql("INSERT INTO weather_dim SELECT * FROM {WEATHER_CSV}")

  con.sql("""
        CREATE TABLE IF NOT EXISTS road_dim as (
        road_id INTEGER PRIMARY KEY,
        road_geometry VARCHAR,
        road_state_of_repair VARCHAR,
        max_speed INTEGER,
        highway_number INTEGER,
        highway_km DOUBLE,
        highway_cars_per_day INTEGER
        )
        """)

  con.sql("INSERT INTO road_dim SELECT * FROM {ROAD_CSV}")

  con.sql("""
        CREATE TABLE IF NOT EXISTS parties_dim as (
        parties_id INTEGER PRIMARY KEY,
        any_motor_vehicle_involved BOOLEAN,
        cars_involved BOOLEAN,
        pedestrians_involved BOOLEAN,
        low_speed_vehicles_involved BOOLEAN,
        elderly_driver_involved BOOLEAN,
        public_transportation_vehicle_involved BOOLEAN,
        truck_involved BOOLEAN,
        motorcycle_involved BOOLEAN
        )
        """)
  con.execute("INSERT INTO parties_dim SELECT * FROM {PARTIES_CSV}")

def load_facts_to_warehouse():
  con = duckdb.connect(DUCK_DB)

  con.sql("""
        CREATE TABLE IF NOT EXISTS {FACT_TABLE} as (
        id INTEGER PRIMARY KEY,
        FOREIGN KEY (time_id) REFERENCES time_dim (time_id),
        FOREIGN KEY (location_id) REFERENCES location_dim (location_id),
        FOREIGN KEY (weather_id) REFERENCES weather_dim (weather_id),
        FOREIGN KEY (parties_id) REFERENCES parties_dim (parties_id),
        FOREIGN KEY (road_id) REFERENCES road_dim (road_id),
        happened_at TIMESTAMP,
        number_of_people INTEGER,
        number_of_vehicles INTEGER,
        number_of_fatalities INTEGER,
        number_of_injured INTEGER
        )
        """)
  con.sql("INSERT INTO {FACT_TABLE} SELECT * FROM {FACT_CSV}")

with DAG("transformation_etl", catchup=False) as dag:

    t1 = PythonOperator(
        task_id="load_data_from_mongodb",
        python_callable=get_all_data,
    )

    t2 = PythonOperator(
        task_id="transform_data_and_save_as_csv",
        python_callable=load_dimensions_to_warehouse,
    )

    _ = t1 >> t2 
