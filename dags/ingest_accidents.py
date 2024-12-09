import requests
import pandas as pd

from pyproj import Transformer
from pymongo import MongoClient

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

API = "https://avaandmed.eesti.ee/api"
DATASET_ID = "d43cbb24-f58f-4928-b7ed-1fcec2ef355b"
FILE_ID = "3c255d23-8fa7-479f-b4bb-9c8c636dbba9"

DB = "dataeng_project"
COLLECTION = "accidents"
MONGO_CONNECTION_STRING = "mongodb://admin:admin@mongo:27017"

COL_MAP = {
    "Juhtumi nr": "case_id",
    "Toimumisaeg": "time",
    "Isikuid": "num_people",
    "Hukkunuid": "num_dead",
    "Sõidukeid": "num_vehicles",
    "Vigastatuid": "num_injured",
    "Aadress": "address",
    "Tänav": "street",
    "Maja nr": "house_number",
    "Ristuv tänav": "crossing_street",
    "Tee nr": "road_number",
    "Tee km": "road_kilometer",
    "Maakond": "county",
    "Omavalitsus": "commune",
    "Asutusüksus": "village",
    "Asula": "is_settlement",
    "Liiklusõnnetuse liik": "type_accident_general",
    "Liiklusõnnetuse liik (detailne)": "type_accident_detailed",
    "Joobes mootorsõidukijuhi osalusel": "drunk_driver_involved",
    "Kergliikurijuhi osalusel": "light_vehicle_driver_involved",
    "Jalakäija osalusel": "pedestrian_involved",
    "Kaassõitja osalusel": "passenger_involved",
    "Maastikusõiduki juhi osalusel": "terrain_vehicle_driver_involved",
    "Eaka (65+) mootorsõidukijuhi osalusel": "elder_driver_involved",
    "Bussijuhi osalusel": "bus_driver_involved",
    "Veoautojuhi osalusel": "truck_driver_involved",
    "Ühissõidukijuhi osalusel": "public_transport_driver_involved",
    "Sõiduautojuhi osalusel": "car_driver_involved",
    "Mootorratturi osalusel": "motorcyclist_involved",
    "Mopeedijuhi osalusel": "moped_driver_involved",
    "Jalgratturi osalusel": "cyclist_involved",
    "Alaealise osalusel": "underage_involved",
    "Esmase juhiloa omaniku osalusel": "provisional_driving_license_involved",
    "Turvavarustust mitte kasutanud isiku osalusel": "safety_equipment_not_used",
    "Mootorsõidukijuhi osalusel": "motor_vehicle_driver_involved",
    "Tüüpskeemi nr": "type_scheme_code",
    "Tüüpskeem": "type_scheme_name",
    "Tee tüüp": "road_type_general",
    "Tee tüüp (detailne)": "road_type_detailed",
    "Tee liik": "road_kind",
    "Tee element": "road_element_general",
    "Tee element (detailne)": "road_element_detailed",
    "Tee objekt": "road_object",
    "Kurvilisus": "road_curvature",
    "Tee tasasus": "road_hill_type",
    "Tee seisund": "road_condition",
    "Teekate": "road_paving",
    "Teekatte seisund": "road_paving_condition",
    "Sõiduradade arv": "number_of_lanes",
    "Lubatud sõidukiirus": "speed_limit",
    "Ilmastik": "weather",
    "Valgustus": "lighting_general",
    "Valgustus (detailne)": "lighting_detailed",
    "X koordinaat": "x",
    "Y koordinaat": "y"
}


def extract():
    url = f"{API}/datasets/{DATASET_ID}/files/{FILE_ID}"
    res = requests.get(url)

    # NOTE: api is broken for this file specifially
    # just download it manually and place it in mnt/data
    if res.status_code != 200:
        print(f"Fetching traffic accident datat failed with code {res.status_code}")
        # return

    # df = pd.DataFrame(res["data"]) # pseudocode for real solution
    df = pd.read_csv("/mnt/data/lo_2011_2024.csv", sep=";")
    df.to_csv(f"/tmp/{FILE_ID}", index=False)


def wrangle():
    df = pd.read_csv(f"/tmp/{FILE_ID}")
    df = df.rename(columns=COL_MAP)

    original_crs_epsg = 3301
    target_crs_epsg = 4326
    transformer = Transformer.from_crs(original_crs_epsg, target_crs_epsg)

    x, y = transformer.transform(df["x"], df["y"])
    df["x"], df["y"] = x, y

    df["time"] = pd.to_datetime(df["time"])
    df["year"] = df["time"].dt.year

    df.to_csv(f"/tmp/{FILE_ID}", index=False)


def load():
    client = MongoClient(MONGO_CONNECTION_STRING)
    col = client[DB][COLLECTION]

    data = pd.read_csv(f"/tmp/{FILE_ID}")
    data.index = data.index.map(str)
    data = data.to_dict(orient="records")
    col.insert_many(data)


with DAG("traffic_accidents_etl", catchup=False) as dag:
    extract = PythonOperator(
        task_id="extract_traffic_accidents",
        python_callable=extract,
    )

    wrangle = PythonOperator(
        task_id="preporcess_traffic_accidents",
        python_callable=wrangle,
    )

    load_lake = PythonOperator(
        task_id="load_traffic_accidents",
        python_callable=load,
    )

    cleanup = BashOperator(
        task_id="traffic_accidents_cleanup",
        bash_command=f"rm -f /tmp/{FILE_ID}",
    )

    _ = extract >> wrangle >> load_lake >> cleanup

