import scipy
import pandas as pd
import numpy as np
from pymongo import MongoClient
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

# MongoDB Connection and Constants
MONGO_CONNECTION_STRING = "mongodb://admin:admin@mongo:27017"
PROJECT_DB = "dataeng_project"
WEATHER_COLLECTION = "weather"
ACCIDENT_COLLECTION = "accidents"

# Paths to Save CSVs for dbt
FACT_CSV = "data/accidents_clean.csv"
TIME_CSV = "data/time_dim_clean.csv"
WEATHER_CSV = "data/weather_clean.csv"
LOCATION_CSV = "data/location_clean.csv"
ROAD_CSV = "data/road_clean.csv"
PARTIES_CSV = "data/parties_clean.csv"

# Weather Station Data
STATION_COORDS = [
    (58.945278, 23.555278), (58.036709, 24.458048), (58.749722, 26.415),
    (59.328889, 27.398333), (58.098611, 23.970278)
]
STATIONS = np.array(["Haapsalu", "Haademeeste", "Jogeva", "Johvi", "Kihnu"])


def extract_and_save_data():
    """
    Extracts accident and weather data from MongoDB, enriches it, and saves it as CSVs.
    """
    # MongoDB connection
    client = MongoClient(MONGO_CONNECTION_STRING)
    db = client[PROJECT_DB]
    accident_coll = db[ACCIDENT_COLLECTION]
    weather_coll = db[WEATHER_COLLECTION]

    # Extract accident data
    df_accident = pd.DataFrame(list(accident_coll.find()))
    df_accident = df_accident.dropna(subset=["X coordinate", "Y coordinate"])
    dates = pd.to_datetime(df_accident["Time of accident"])

    # Match accidents to nearest weather stations
    _, station_idx = scipy.spatial.KDTree(STATION_COORDS).query(df_accident[["X coordinate", "Y coordinate"]])
    df_accident["Station"] = STATIONS[station_idx]

    # Enrich accidents with weather data
    enriched_data = []
    for _, row in df_accident.iterrows():
        weather = weather_coll.find_one({
            "Year": row["Time of accident"].year,
            "Month": row["Time of accident"].month,
            "Day": row["Time of accident"].day,
            "Station": row["Station"]
        })
        if weather:
            enriched_row = row.to_dict()
            enriched_row.update(weather)
            enriched_data.append(enriched_row)

    # Save fact table (accidents) for dbt
    pd.DataFrame(enriched_data).to_csv(FACT_CSV, index=False)
    print(f"Fact data saved to {FACT_CSV}")

    # Save other dimensions as needed
    # Example: Time dimension (static or programmatically generated)
    time_dim = pd.DataFrame({
        "time_id": [1, 2],
        "season": ["Winter", "Summer"],
        "day_of_week": [1, 2],
        "time_of_day": ["Morning", "Afternoon"]
    })
    time_dim.to_csv(TIME_CSV, index=False)
    print(f"Time dimension saved to {TIME_CSV}")

    # Additional dimension files can also be saved dynamically
    # Example for locations:
    # df_locations = pd.DataFrame(...)  # Extracted from accidents or other sources
    # df_locations.to_csv(LOCATION_CSV, index=False)


# Airflow DAG Definition
default_args = {
    "owner": "airflow",
    "retries": 1,
    "start_date": datetime(2024, 12, 1),
}

with DAG("etl_pipeline_with_dbt", default_args=default_args, schedule_interval="@daily", catchup=False) as dag:
    # Task 1: Extract and Save Data from MongoDB
    extract_task = PythonOperator(
        task_id="extract_and_save_data",
        python_callable=extract_and_save_data,
    )

    # Task 2: Run dbt transformations
    dbt_task = BashOperator(
        task_id="run_dbt",
        bash_command="dbt run --profiles-dir /path/to/dbt",
    )

    extract_task >> dbt_task
