import duckdb
import requests

import pandas as pd

from pyproj import Transformer

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

DATASET_ID = "d43cbb24-f58f-4928-b7ed-1fcec2ef355b"
FIELD_ID = "lo_2011_2024.csv"

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


def download_data():
    url = f"https://avaandmed.eesti.ee/api/datasets/{DATASET_ID}/files/{FIELD_ID}"
    # TODO: need api token
    res = requests.get(url)
    with open(f"/tmp/{FIELD_ID}", "wb") as f:
        f.write(res.content)


def preprocess_data():
    df = pd.read_csv(f"/tmp/{FIELD_ID}")
    df = df.rename(columns=COL_MAP)

    original_crs_epsg = 3301
    target_crs_epsg = 4326 
    transformer = Transformer.from_crs(original_crs_epsg, target_crs_epsg)

    x, y = transformer.transform(df['X coordinate'], df['Y coordinate'])
    df['X coordinate'], df['Y coordinate'] = x, y

    df.to_csv(f"/tmp/{FIELD_ID}")


def load_duckdb():
    # TODO: insert into where date > latest in table
    #       queries are basically pseudocode right now
    db_name = "db"
    table_name = "accidents"

    con = duckdb.connect(db_name)
    con.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM read_csv('/tmp/{FIELD_ID}')")
    con.execute(f"INSERT INTO {table_name} SELECT * FROM read_csv('/tmp/{FIELD_ID})")


with DAG("ingest_accidents", catchup=False) as dag:
    t1 = PythonOperator(
        task_id="download_traffic_accident_data",
        python_callable=download_data,
    )

    t2 = PythonOperator(
        task_id="preporcess_traffic_accident_data",
        python_callable=preprocess_data,
    )

    t3 = PythonOperator(
        task_id="load_traffic_accident_data",
        python_callable=load_duckdb,
    )

    _ = t1 >> t2 >> t3

