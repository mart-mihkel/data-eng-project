import os
import pandas as pd

from pymongo import MongoClient

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

DB = "dataeng_project"
COLLECTION = "weather"
MONGO_CONNECTION_STRING = "mongodb://admin:admin@mongo:27017"

COL_MAP = {
    "Aasta": "year",
    "Kuu": "month",
    "Päev": "day",
    "Kell (UTC)": "time",
    "Õhutemperatuur °C": "air_temperature",
    "Tunni miinimum õhutemperatuur °C": "hourly_min_air_temperature",
    "Tunni maksimum õhutemperatuur °C": "hourly_max_air_temperature",
    "10 minuti keskmine tuule kiirus m/s": "10_min_average_wind_speed",
    "Tunni maksimum tuule kiirus m/s": "hourly_maximum_wind_speed",
    "Tunni sademete summa mm": "hourly_precipitation_total",
    "Õhurõhk jaama kõrgusel hPa": "air_pressure_at_station_height",
    "Suhteline õhuniiskus %": "relative_humidity"
}


def wrangle():
    xlsxs = filter(
        lambda x: x.endswith("xlsx"), 
        os.listdir("/tmp/historical_weather")
    )

    for f in xlsxs:
        print("Wrangling: ", f)
        df = pd.read_excel(f"/tmp/historical_weather/{f}", header=2)
        df = df.rename(columns=COL_MAP)

        # Removes columns not in column map
        if(len(df.columns) != len(COL_MAP.values())):
            df = df.drop(columns = df.columns.difference(COL_MAP.values()))

        stem = f.split(".")[0]
        df.to_csv(f"/tmp/historical_weather/{stem}.csv", index=False)


def load():
    client = MongoClient(MONGO_CONNECTION_STRING, timeoutMS=40000)
    col = client[DB][COLLECTION]

    csvs = filter(
        lambda x: x.endswith("csv"),
        os.listdir("/tmp/historical_weather")
    )

    for csv in csvs:
        print("Loading: ", csv)
        items = pd.read_csv(f"/tmp/historical_weather/{csv}").to_dict(orient="records")
        col.insert_many(items)


with DAG("historical_weather_etl", catchup=False) as dag:
    extract = BashOperator(
        task_id="extract_historical_weather",
        bash_command="scripts/download_weather.bash",
    )

    wrangle = PythonOperator(
        task_id="preporcess_historical_weather",
        python_callable=wrangle,
    )

    load_lake = PythonOperator(
        task_id="load_historical_weather",
        python_callable=load,
    )

    cleanup = BashOperator(
        task_id="historical_weather_cleanup",
        bash_command="rm -rf /tmp/historical_weather",
    )

    _ = extract >> wrangle >> load_lake >> cleanup

