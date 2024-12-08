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
    "Mnt nr": "road_number",
    "Maantee nimetus": "road_name",
    "Algus m": "start",
    "Lõpp m": "end",
    "Pikkus m": "length",
    "AKÖL autot/ööp": "AADT_vehicles_per_day",
    "SAPA %": "SAPA",
    "VAAB %": "VAAB",
    "AR %": "AR",
    "SAPA autot/ööp": "SAPA_vehicles_per_day",
    "VAAB autot/ööp": "VAAB_vehicles_per_day",
    "AR autot/ööp": "AR_vehicles_per_day",
    "Loenduse aasta": "survey_year",
    "Maakond": "county",
    "Regioon": "region"
}


def extract():
    target = "/tmp/traffic_density"
    os.makedirs(target, exist_ok=True)
    zipfile.ZipFile("/mnt/data/traffic_flow_output.zip", "r").extractall(target)


def load():
    client = MongoClient(MONGO_CONNECTION_STRING)
    col = client[DB][COLLECTION]

    csvs = filter(
        lambda x: x.endswith("xlsx"), 
        os.listdir("/tmp/traffic_density")
    )

    for f in csvs:
        items = pd.read_excel(f"/tmp/traffic_density/{f}").to_dict(orient="records")
        col.insert_many(items)


with DAG("traffic_density_etl", catchup=False) as dag:
    extract = PythonOperator(
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

    _ = extract >> load_lake >> cleanup

