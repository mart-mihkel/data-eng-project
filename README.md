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
   - COnsisted of traffic accident with at least one injured person from 2011 to 2024.
   - Source: Estonian Open Data Portal.
   - Link to the dataset: [Traffic Accidents Dataset](https://avaandmed.eesti.ee/datasets/inimkannatanutega-liiklusonnetuste-andmed)
2. **Weather Data**:
   - Includes hourly weather conditions (precipitation, temperature, wind speed, etc).
   - Actually consisted of 27 datasets, for 27 different weather stations in Estonia, with data from 2004 to 2024.
   - Source: Estonian Weather Service.
   - Link to the dataset: [Weather Data](https://www.ilmateenistus.ee/kliima/ajaloolised-ilmaandmed/)
3. **Traffic Volume Data**:
   - Provides daily vehicle counts on Estonian highways and roads.
   - Data from 2017 to 2023, earlier years were only available in non-machinereadable .pdf files.
   - Source: Estonian Transport Administration.
   - Link to the dataset: [Traffic Volume Data](https://www.transpordiamet.ee/liiklussageduse-statistika)

---

## **Data modelling with star schema**

To organize the data for efficient analysis, we created a **star schema** with the following components see [schema.yml](./dbt/models/star/schema.yml):

1. **Fact Table**:
   - `accident_fact`: Captures accident-related metrics (e.g., number of injuries, fatalities) and contains keys to all the dimension tables.
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
- In python code, useful features are extracted from the raw data, such as extracting day and hour from a timestamp.
- Accidents are geographically matched to their closest weather station.
- The cleaned and augmented data is loaded to a staging area in DuckDB.
- **dbt** performs the following transformations on DuckDB data:
  - Selects the relevant columns from staging area tables.
  - Builds dimension and fact tables and saves them as tables in DuckDB.
  - Implements data masking for sensitive columns like GPS coordinates and addresses.

### 3. **Visualization**

- Results are visualized using **Streamlit** dashboards:
  - Accident severity by weather conditions.
  - Accident severity by traffic density.
  - Filters for many features, such as year, season, county, etc.

---

## Start Docker Containers

```bash

docker-compose up -d
docker exec data-eng-project-airflow-worker-1 /mnt/scripts/create_airflow_users.bash
```

## Openmetadata

Get jwt token from [http://localhost:8585](http://localhost:8585) 
Settings -> Bots -> IngestionBot
Put it in `BOT_JWT` at [ingest_mongodb_metadata.py](./dags/ingest_mongodb_metadata.py)

### Start Airflow

1. **Open Airflow Dashboard**: [http://localhost:8080](http://localhost:8080)
2. **Log in using:**
   - **Username**: `admin`
   - **Password**: `admin`
3. **Trigger the ingestions DAGs**
4. **Trigger the transformation DAGs**
5. **Open [http://localhost:8501] to use Streamlit.**
