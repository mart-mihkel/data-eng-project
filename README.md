# **Traffic Accidents Analysis in Estonia**

## **Project Overview**

This project analyzes traffic accidents in Estonia by examining the impact of weather conditions, traffic volumes, and road characteristics. The goal is to provide actionable insights for improving road safety and reducing accidents. This is achieved through combining data from varied sources, such as the Estonian Open Data Portal, Estonian Transport Administration and the Estonian Weather Service. By using data engineering techniques and tools, this data is cleaned, combined, transformed, loaded and served.

### **Key Objectives**

1. Find reliable and detailed data about the needed domains.
2. Enable the detailed analysis of of how weather conditions like precipitation or wind speed influence accident severity.
3. Enable investigating the effects of traffic density of accident severity.
4. Create customizable data-driven visualizations that can be used for communicating key findings.

---

## **Technologies Used**

The project uses the following tools and technologies to build a complete data pipeline:

| **Technology**      | **Purpose**                                                                    |
| ------------------- | ------------------------------------------------------------------------------ |
| **Apache Airflow**  | Automate and schedule ETL workflows to extract, clean and transfer data.       |
| **DuckDB**          | Store and query structured datasets and views efficiently for analytical purposes. |
| **dbt**             | Perform data transformations, build a star schema, and enable data masking.  |
| **MongoDB**         | Store raw accident, traffic density and weather data in an unstructured format.                 |
| **Streamlit**       | Create an interactive dashboard for visualizing traffic accident insights.     |
| **Data Privacy**    | Ensure sensitive data (e.g., GPS coordinates, address) is masked while querying.        |
| **Data Security**   | Ensure that data is securely handled, apprpriate access control measures have been implemented. | 
| **OpenMetadata**    | Used for data lineage and governance.                             |
| **Redis**           | A high-performance in-memory database used for caching common SQL queries and quick lookups.      |

---

## **Data Sources**

We used the following datasets to perform the analysis:

1. **Traffic Accidents Dataset**:
   - Contains details about accidents, such as date, location, severity, and participants.
   - Source: Estonian Open Data Portal.
   - Link to the dataset - https://avaandmed.eesti.ee/datasets/inimkannatanutega-liiklusonnetuste-andmed
   - Link to the dataset: [Traffic Accidents Dataset](https://avaandmed.eesti.ee/datasets/inimkannatanutega-liiklusonnetuste-andmed)
2. **Weather Data**:
   - Includes hourly weather conditions (precipitation, temperature, wind speed).
   - Source: Open-Meteo API.
   - Link to the dataset: [Weather Data](https://www.ilmateenistus.ee/kliima/ajaloolised-ilmaandmed/)
3. **Traffic Volume Data**:
   - Provides vehicle counts on Estonian highways and roads.
   - Source: Transpordiamet.
   - Link to the dataset: [Traffic Volume Data](https://www.transpordiamet.ee/liiklussageduse-statistika)

---

## **Star Schema**

To organize the data for efficient analysis, we created a **star schema** with the following components see [schema.yml](./dbt/models/star/schema.yml):

1. **Fact Table**:
   - `accident_fact`: Captures accident-related metrics (e.g., number of injuries, fatalities).
2. **Dimension Tables**:
   - `time_dim`: Temporal data (year, month, day, weekday).
   - `location_dim`: Spatial data (county, urban/rural, municipality).
   - `weather_dim`: Weather conditions (precipitation, temperature, wind speed).
   - `road_dim`: Road characteristics (road type, speed limit, geometry).
   - `parties_dim`: Information about participants in accidents (pedestrians, cyclists, vehicles).

---

## **Project Workflow**

The project consists of the following steps:

### 1. **Data Extraction**

- Accident and weather data are extracted from their respective sources and loaded into a data lake.

### 2. **Data Transformation**

- **dbt** performs the following transformations:
  - Cleans and enriches raw data.
  - Builds dimension and fact tables in DuckDB.
  - Implements data masking for sensitive columns like GPS coordinates and addresses.

### 3. **Analysis**

- Queries are written in **DuckDB** to analyze:
  - Weather conditions contributing to severe accidents.
  - Accident patterns during peak traffic hours.
  - Risks to vulnerable groups during adverse weather.

### 4. **Visualization**

- Results are visualized using **Streamlit** dashboards:
  - Accident frequencies by region and weather conditions.
  - Vulnerable group impact under specific weather conditions.

---

## Start Docker Containers

```bash
docker-compose up -d
docker exec traffic-accidents-elt-airflow-worker-1 /mnt/scripts/create_airflow_users.bash # access control
```

### Airflow

1. **Open Airflow Dashboard**: [http://localhost:8080](http://localhost:8080)
2. **Log in using:**
   - **Username**: `admin`
   - **Password**: `admin`
3. **Trigger the ingestions DAGs**
   - `ingest_traffic_density`
   - `ingest_accidents`
   - `ingest_weather`
4. **Trigger tranformation DAG**
   - `transform`

### Streamlit

[http://localhost:8501](http://localhost:8501)

### Openmetadata

Get jwt token from [http://localhost:8585](http://localhost:8585) --> Settings --> Bots --> IngestionBot
Put it in `BOT_JWT` at [ingest_mongodb_metadata.py](./dags/ingest_mongodb_metadata.py)
Run `ingest_mongodb_metadata`

