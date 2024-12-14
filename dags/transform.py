import os
import duckdb
import datetime
import pandas as pd

from pymongo import MongoClient

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

MONGO_CONNECTION_STRING = "mongodb://admin:admin@mongo:27017"
PROJECT_DB = "dataeng_project"
DUCK_DB = "duckdb/duck.db"

WEATHER_COLLECTION = "weather"
DENSITY_COLLECTION = "density"
ACCIDENT_COLLECTION = "accidents"

WEATHER_COLUMNS = ['year', 'month', 'day', 'hour', 'air_pressure_at_station_height', 'hourly_precipitation_total', 'relative_humidity', 'air_temperature', 'hourly_min_air_temperature', 'hourly_max_air_temperature', '10_min_average_wind_speed', 'hourly_maximum_wind_speed', 'station']


def serialize_accident():
    duck_client = duckdb.connect(DUCK_DB)
    mongo_client = MongoClient(MONGO_CONNECTION_STRING)
    coll = mongo_client[PROJECT_DB][ACCIDENT_COLLECTION]

    df = pd.DataFrame(coll.find())

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
    batch_df = batch_df[WEATHER_COLUMNS]

    duck_client.sql("DROP TABLE IF EXISTS weather_tmp")
    duck_client.sql("CREATE TABLE weather_tmp AS SELECT * FROM batch_df")

    for skip in range(batch_size, total_count, batch_size):
        print(f"Processing weather records {skip} to {skip + batch_size}")

        batch = coll.find().skip(skip).limit(batch_size).to_list()
        batch_df = pd.DataFrame(batch)
        batch_df = batch_df[WEATHER_COLUMNS]

        duck_client.sql("INSERT INTO weather_tmp SELECT * FROM batch_df")


def serialize_density():
    duck_client = duckdb.connect(DUCK_DB)
    mongo_client = MongoClient(MONGO_CONNECTION_STRING)
    coll = mongo_client[PROJECT_DB][DENSITY_COLLECTION]

    df = pd.DataFrame(coll.find())

    duck_client.sql("DROP TABLE IF EXISTS density_tmp")
    duck_client.sql("CREATE TABLE density_tmp AS SELECT * FROM df")


def clone_and_mask():
    """
    Create a copy of the primary duckdb file with no sensitive 
    location data. This option can be served to people who
    don't need to know the location data.
    """
    masked = 'duckdb/masked.db'
    os.system(f'cp {DUCK_DB} {masked}')

    duck_client = duckdb.connect(masked)
    duck_client.sql("DROP TABLE location_dim")
    duck_client.sql("CREATE TABLE location_dim AS SELECT * FROM masked_location_dim")
    duck_client.sql("DROP TABLE masked_location_dim")


def cleanup():
    duck_client = duckdb.connect(DUCK_DB)

    duck_client.sql("DROP TABLE accidents_tmp")
    duck_client.sql("DROP TABLE weather_tmp")
    duck_client.sql("DROP TABLE density_tmp")


with DAG(
    "transformation_dbt",
    start_date=datetime.datetime(2024, 12, 1),
    schedule="@monthly",
    catchup=False,
) as dag:
    # wait_accidents = ExternalTaskSensor(
    #     task_id='wait_accident_igested',
    #     external_dag_id='traffic_accidents_etl',
    #     check_existence=True,
    #     mode='reschedule',
    # )

    prepare_accidents = PythonOperator(
        task_id="extract_accident_data_from_lake",
        python_callable=serialize_accident,
    )

    # wait_weather = ExternalTaskSensor(
    #     task_id='wait_weather_igested',
    #     external_dag_id='historical_weather_etl',
    #     check_existence=True,
    #     mode='reschedule',
    # )

    prepare_weather = PythonOperator(
        task_id="extract_weather_data_from_lake",
        python_callable=serialize_weather,
    )

    # wait_density = ExternalTaskSensor(
    #     task_id='wait_density_igested',
    #     external_dag_id='traffic_density_etl',
    #     check_existence=True,
    #     mode='reschedule',
    # )

    prepare_density = PythonOperator(
        task_id="extract_density_data_from_lake",
        python_callable=serialize_density,
    )

    dbt = BashOperator(
        task_id="dbt_tranform",
        bash_command=f"cd /opt/airflow/dbt && dbt compile && dbt run",
    )

    cleanup_task = PythonOperator(
        task_id="cleanup",
        python_callable=cleanup,
    )

    create_masked_duckdb = PythonOperator(
        task_id="masked_duckdb",
        python_callable=clone_and_mask,
    )

    # _ = wait_accidents >> prepare_accidents
    # _ = wait_density >> prepare_density
    # _ = wait_weather >> prepare_weather
    _ = prepare_accidents >> prepare_density >> prepare_weather >> dbt >> cleanup_task >> create_masked_duckdb
