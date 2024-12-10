import os
import zipfile
import pandas as pd

from pymongo import MongoClient

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

DB = "dataeng_project"
COLLECTION = "density"
MONGO_CONNECTION_STRING = "mongodb://admin:admin@mongo:27017"

COL_MAP = {
    "Road No": "road_number",
    "Road Name": "road_name",
    "Start (m)": "start",
    "End (m)": "end",
    "Length (m)": "length",
    "SAPA %": "SAPA",
    "VAAB %": "VAAB",
    "AR %": "AR",
    "AADT vehicles/day": "AADT_vehicles_per_day",
    "SAPA vehicles/day": "SAPA_vehicles_per_day",
    "VAAB vehicles/day": "VAAB_vehicles_per_day",
    "AR vehicles/day": "AR_vehicles_per_day",
    "Läbisõit SAPA": "SAPA_mileage",
    "Loenduse liik": "survey_type",
    "Survey Year": "survey_year",
    "Source Sheet": "source_sheet",
    "County": "county",
    "Region": "region",
}

DESIRED_COLUMNS = ["road_number", "start", "end", "AADT_vehicles_per_day", "survey_year"]

def extract():
    target = "/tmp/traffic_density"
    os.makedirs(target, exist_ok=True)
    zipfile.ZipFile("/mnt/data/traffic_flow_output.zip", "r").extractall(target)


def load():
    client = MongoClient(MONGO_CONNECTION_STRING)
    col = client[DB][COLLECTION]

    xlsxs = filter(
        lambda x: x.endswith("xlsx"), 
        os.listdir("/tmp/traffic_density")
    )

    for f in xlsxs:
        df = pd.read_excel(f"/tmp/traffic_density/{f}")
        df = df.rename(columns=COL_MAP)
        df = df[DESIRED_COLUMNS]
        items = df.to_dict(orient="records")
        col.insert_many(items)


with DAG("traffic_density_etl", catchup=False) as dag:
    extract_task = PythonOperator(
        task_id="extract_traffic_density",
        python_callable=extract,
    )

    load_lake = PythonOperator(
        task_id="load_traffic_density",
        python_callable=load,
    )

    cleanup = BashOperator(
        task_id="traffic_density_cleanup",
        bash_command="rm -rf /tmp/traffic_density",
    )

    _ = extract_task >> load_lake >> cleanup

