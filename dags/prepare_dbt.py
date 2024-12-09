import scipy
import duckdb
import numpy as np
import pandas as pd

from pymongo import MongoClient

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

MONGO_CONNECTION_STRING = "mongodb://admin:admin@mongo:27017"
PROJECT_DB = "dataeng_project"
DUCK_DB = "duckdb/duck.db"

WEATHER_COLLECTION = "weather"
DENSITY_COLLECTION = "density"
ACCIDENT_COLLECTION = "accidents"

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


def serialize_accident():
    duck_client = duckdb.connect(DUCK_DB)
    mongo_client = MongoClient(MONGO_CONNECTION_STRING)
    coll = mongo_client[PROJECT_DB][ACCIDENT_COLLECTION]

    df = pd.DataFrame(coll.find()).dropna(subset=["x", "y"])
    _, station_idx = scipy.spatial.KDTree(STATION_COORDS).query(df[["x", "y"]])
    df["nearest_station"] = STATIONS[station_idx]

    def to_season(date):
        time_tuple = (date.month, date.day)
        if time_tuple > (12,20) or time_tuple < (3,20):
            return "Winter"
        elif time_tuple > (9,23):
            return "Fall"
        elif time_tuple > (6,21):
            return "Summer"
        else:
            return "Spring"

    df["time"] = pd.to_datetime(df["time"])
    df["season"] = df["time"].map(to_season)
    df["weekday"] = df["time"].dt.weekday
    df["urban"] = df["is_settlement"].to_numpy() == "JAH"

    duck_client.sql("DROP TABLE IF EXISTS accidents_tmp")
    duck_client.sql("CREATE TABLE accidents_tmp AS SELECT * FROM df")


def serialize_weather():
    duck_client = duckdb.connect(DUCK_DB)
    mongo_client = MongoClient(MONGO_CONNECTION_STRING)
    coll = mongo_client[PROJECT_DB][WEATHER_COLLECTION]

    batch_size = 100_000
    total_count = coll.count_documents({})

    batch = coll.find().limit(batch_size).to_list()
    batch_df = pd.DataFrame(batch)

    duck_client.sql("DROP TABLE IF EXISTS weather_tmp")
    duck_client.sql("CREATE TABLE weather_tmp AS SELECT * FROM batch_df")

    for skip in range(batch_size, total_count, batch_size):
        print(f"Processing weather records {skip} to {skip + batch_size}")

        batch = coll.find().skip(skip).limit(batch_size).to_list()
        batch_df = pd.DataFrame(batch)

        duck_client.sql("INSERT INTO weather_tmp SELECT * FROM batch_df")



def serialize_density():
    duck_client = duckdb.connect(DUCK_DB)
    mongo_client = MongoClient(MONGO_CONNECTION_STRING)
    coll = mongo_client[PROJECT_DB][DENSITY_COLLECTION]

    df = pd.DataFrame(coll.find())

    duck_client.sql("DROP TABLE IF EXISTS density_tmp")
    duck_client.sql("CREATE TABLE density_tmp AS SELECT * FROM df")


def cleanup():
    duck_client = duckdb.connect(DUCK_DB)

    # TODO: uncomment when done testing
    # duck_client.sql("DROP TABLE accidents_tmp")
    # duck_client.sql("DROP TABLE weather_tmp")
    # duck_client.sql("DROP TABLE density_tmp")


with DAG("transformation_dbt", catchup=False) as dag:
    prepare_accidents = PythonOperator(
        task_id="extract_accident_data_from_lake",
        python_callable=serialize_accident,
    )

    prepare_weather = PythonOperator(
        task_id="extract_weather_data_from_lake",
        python_callable=serialize_weather,
    )

    prepare_density = PythonOperator(
        task_id="extract_density_data_from_lake",
        python_callable=serialize_density,
    )

    dbt = BashOperator(
        task_id="dbt_tranform",
        bash_command=f"echo '!!! TODO: IMPLEMENT DBT !!!'", # TODO: implement dbt
    )

    cleanup_task = PythonOperator(
        task_id="cleanup",
        python_callable=cleanup,
    )

    _ = prepare_accidents >> prepare_density >> prepare_weather >> dbt >> cleanup_task
