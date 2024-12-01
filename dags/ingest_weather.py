import os
import pandas as pd

from pymongo import MongoClient

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

MONGO_CONNECTION_STRING = "mongodb://admin:admin@mongo:27017"

WEATHER_DB = "weather"
WEATHER_COLLECTION = "historical_weather"

COL_MAP = {
    "Aasta": "Year",
    "Kuu": "Month",
    "Päev": "Day",
    "Kell (UTC)": "Time (UTC)",
    "Õhutemperatuur °C": "Air Temperature (°C)",
    "Tunni miinimum õhutemperatuur °C": "Hourly Minimum Air Temperature (°C)",
    "Tunni maksimum õhutemperatuur °C": "Hourly Maximum Air Temperature (°C)",
    "10 minuti keskmine tuule suund °": "10 Min Average Wind Direction (°)",
    "10 minuti keskmine tuule kiirus m/s": "10 Min Average Wind Speed (m/s)",
    "Tunni maksimum tuule kiirus m/s": "Hourly Maximum Wind Speed (m/s)",
    "Õhurõhk merepinna kõrgusel hPa": "Air Pressure at Sea Level (hPa)",
    "Tunni sademete summa mm": "Hourly Precipitation Total (mm)",
    "Õhurõhk jaama kõrgusel hPa": "Air Pressure at Station Height (hPa)",
    "Suhteline õhuniiskus %": "Relative Humidity (%)",
    "Tunni keskmine summaarne kiirgus W/m²": "Hourly Average Total Radiation W/m²"
}


def wrangle():
    xlsxs = filter(
        lambda x: x.endswith("xlsx"), 
        os.listdir("/tmp/historical_weather")
    )

    for f in xlsxs:
        df = pd.read_excel(f"/tmp/historical_weather/{f}", header=2)
        df = df.rename(columns=COL_MAP)

        stem = f.split(".")[0]
        df.to_csv(f"/tmp/historical_weather/{stem}.csv", index=False)


def load():
    client = MongoClient(MONGO_CONNECTION_STRING)
    col = client[WEATHER_DB][WEATHER_COLLECTION]

    csvs = filter(
        lambda x: x.endswith("csv"), 
        os.listdir("/tmp/historical_weather")
    )

    for f in csvs:
        items = pd.read_csv(f).to_dict(orient="records")
        col.insert_many(items)


with DAG("historical_weather_etl", catchup=False) as dag:
    t1 = BashOperator(
        task_id="extract_historical_weather",
        bash_command="scripts/download_weather.bash",
    )

    t2 = PythonOperator(
        task_id="preporcess_historical_weather",
        python_callable=wrangle,
    )

    t3 = PythonOperator(
        task_id="load_historical_weather",
        python_callable=load,
    )

    _ = t1 >> t2 >> t3

