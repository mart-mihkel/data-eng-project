import os
import zipfile
import pandas as pd

from pymongo import MongoClient

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

MONGO_CONNECTION_STRING = "mongodb://admin:admin@mongo:27017/?connectTimeoutMS=40000"

DENSITY_DB = "dataeng_project"
DENSITY_COLLECTION = "density"

COL_MAP = {
    "Mnt nr": "Road No",
    "Maantee nimetus": "Road Name",
    "Algus m": "Start (m)",
    "Lõpp m": "End (m)",
    "Pikkus m": "Length (m)",
    "AKÖL autot/ööp": "AADT vehicles/day",
    "SAPA %": "SAPA %",
    "VAAB %": "VAAB %",
    "AR %": "AR %",
    "SAPA autot/ööp": "SAPA vehicles/day",
    "VAAB autot/ööp": "VAAB vehicles/day",
    "AR autot/ööp": "AR vehicles/day",
    "Loenduse aasta": "Survey Year",
    "Maakond": "County",
    "Regioon": "Region"
}


def extract():
    target = "/tmp/traffic_density"
    os.makedirs(target, exist_ok=True)
    zipfile.ZipFile("/mnt/data/traffic_flow_output.zip", "r").extractall(target)


def load():
    client = MongoClient(MONGO_CONNECTION_STRING)
    col = client[DENSITY_DB][DENSITY_COLLECTION]

    csvs = filter(
        lambda x: x.endswith("xlsx"), 
        os.listdir("/tmp/traffic_density")
    )

    for f in csvs:
        items = pd.read_excel(f"/tmp/traffic_density/{f}").to_dict(orient="records")
        col.insert_many(items)


with DAG("traffic_density_etl", catchup=False) as dag:
    t1 = PythonOperator(
        task_id="extract_traffic_density",
        python_callable=extract,
    )

    t2 = PythonOperator(
        task_id="load_traffic_density",
        python_callable=load,
    )

    _ = t1 >> t2 

