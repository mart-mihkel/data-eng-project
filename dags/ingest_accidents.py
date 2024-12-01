import duckdb
import requests

import pandas as pd

from pyproj import Transformer

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

API = "https://avaandmed.eesti.ee/api"
DATASET_ID = "d43cbb24-f58f-4928-b7ed-1fcec2ef355b"
FILE_ID = "3c255d23-8fa7-479f-b4bb-9c8c636dbba9"

DUCK_DB = "/opt/airflow/duckdb/duck.db"
ACCIDENT_TABLE = "traffic_accidents"

COL_MAP = {
    "Juhtumi nr": "Case ID",
    "Toimumisaeg": "Time of accident",
    "Isikuid": "Amount of persons",
    "Hukkunuid": "Amount of dead persons",
    "Sõidukeid": "Amount of vehicles",
    "Vigastatuid": "Amount of injured persons",
    "Aadress": "Address",
    "Tänav": "Street",
    "Maja nr": "House number",
    "Ristuv tänav": "Crossing street",
    "Tee nr": "Road number",
    "Tee km": "Road kilometer",
    "Maakond": "County",
    "Omavalitsus": "Commune / Local Government",
    "Asustusüksus": "Village",
    "Asula": "Is the location a settlement",
    "Liiklusõnnetuse liik": "Type of traffic accident (generalized)",
    "Liiklusõnnetuse liik (detailne)": "Type of traffic accident (detailed)",
    "Joobes mootorsõidukijuhi osalusel": "Drunk driver participated",
    "Kergliikurijuhi osalusel": "Light vehicle driver participated",
    "Jalakäija osalusel": "Pedestrian participated",
    "Kaassõitja osalusel": "Passenger participated",
    "Maastikusõiduki juhi osalusel": "Terrain vehicle driver participated",
    "Eaka (65+) mootorsõidukijuhi osalusel": "Elder driver participated",
    "Bussijuhi osalusel": "Bus driver participated",
    "Veoautojuhi osalusel": "Truck driver participated",
    "Ühissõidukijuhi osalusel": "Public transport driver participated",
    "Sõiduautojuhi osalusel": "Car driver participated",
    "Mootorratturi osalusel": "Motorcyclist participated",
    "Mopeedijuhi osalusel": "Moped driver participated",
    "Jalgratturi osalusel": "Cyclist participated",
    "Alaealise osalusel": "Underage participated",
    "Esmase juhiloa omaniku osalusel": "Provisional driving license participated",
    "Turvavarustust mitte kasutanud isiku osalusel": "Safety equipment not used",
    "Mootorsõidukijuhi osalusel": "Motor vehicle driver participated",
    "Tüüpskeemi nr": "Type scheme code",
    "Tüüpskeem": "Type scheme name",
    "Tee tüüp": "Road type (generalized)",
    "Tee tüüp (detailne)": "Road type (detailed)",
    "Tee liik": "Road kind",
    "Tee element": "Road element (generalized)",
    "Tee element (detailne)": "Road element (detailed)",
    "Tee objekt": "Road object",
    "Kurvilisus": "Road curvature",
    "Tee tasasus": "Road hill type",
    "Tee seisund": "Road condition",
    "Teekate": "Road paving",
    "Teekatte seisund": "Road paving condition",
    "Sõiduradade arv": "Number of lanes",
    "Lubatud sõidukiirus": "Allowed driving speed",
    "Ilmastik": "Weather",
    "Valgustus": "Lighting (generalized)",
    "Valgustus (detailne)": "Lighting (detailed)",
    "X koordinaat": "X coordinate",
    "Y koordinaat": "Y coordinate"
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

    x, y = transformer.transform(df['X coordinate'], df['Y coordinate'])
    df['X coordinate'], df['Y coordinate'] = x, y

    df.to_csv(f"/tmp/{FILE_ID}", index=False)


def load():
    con = duckdb.connect(DUCK_DB)

    con.sql(f"""
        CREATE TABLE IF NOT EXISTS {ACCIDENT_TABLE} AS FROM read_csv("/tmp/{FILE_ID}")
    """)

    con.sql(f"""
        INSERT INTO {ACCIDENT_TABLE} 
        SELECT * 
        FROM read_csv("/tmp/{FILE_ID}") 
        WHERE "Time of accident" > (SELECT "Time of accident"
                                    FROM {ACCIDENT_TABLE} 
                                    ORDER BY "Time of accident" DESC 
                                    LIMIT 1)
    """)


with DAG("traffic_accidents_etl", catchup=False) as dag:
    t1 = PythonOperator(
        task_id="extract_traffic_accidents",
        python_callable=extract,
    )

    t2 = PythonOperator(
        task_id="preporcess_traffic_accidents",
        python_callable=wrangle,
    )

    t3 = PythonOperator(
        task_id="load_traffic_accidents",
        python_callable=load,
    )

    _ = t1 >> t2 >> t3

