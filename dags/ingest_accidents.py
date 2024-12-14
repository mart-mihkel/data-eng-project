import requests
import datetime
import scipy as sp
import numpy as np
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
    df = df.dropna(subset=["x", "y"])

    original_crs_epsg = 3301
    target_crs_epsg = 4326
    transformer = Transformer.from_crs(original_crs_epsg, target_crs_epsg)

    x, y = transformer.transform(df["x"], df["y"])
    df["x"], df["y"] = x, y

    _, station_idx = sp.spatial.KDTree(STATION_COORDS).query(df[["x", "y"]])
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
    df["year"] = df["time"].dt.year
    df["month"] = df["time"].dt.month
    df["day"] = df["time"].dt.day
    df["hour"] = df["time"].dt.hour
    df["weekday"] = df["time"].dt.weekday
    df["season"] = df["time"].map(to_season)
    df["urban"] = df["is_settlement"].map(lambda x: "Urban" if x=="JAH" else "Rural")

    df.to_csv(f"/tmp/{FILE_ID}", index=False)


def load():
    client = MongoClient(MONGO_CONNECTION_STRING)
    col = client[DB][COLLECTION]

    data = pd.read_csv(f"/tmp/{FILE_ID}")
    data.index = data.index.map(str)
    data = data.to_dict(orient="records")
    col.insert_many(data)


with DAG(
    "traffic_accidents_etl", 
    start_date=datetime.datetime(2024, 12, 1),
    schedule="@monthly",
    catchup=False,
) as dag:
    extract_task = PythonOperator(
        task_id="extract_traffic_accidents",
        python_callable=extract,
    )

    wrangle_task = PythonOperator(
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

    _ = extract_task >> wrangle_task >> load_lake >> cleanup

