# **Traffic Accidents Analysis in Estonia**

## **Project Overview**

This project analyzes traffic accidents in Estonia by examining the impact of weather conditions, traffic volumes, and road characteristics. The goal is to provide actionable insights for improving road safety and reducing accidents.

### **Key Objectives**

1. Understand how weather conditions like rain, snow, and fog influence accident severity.
2. Identify traffic patterns during peak hours that contribute to accidents.
3. Assess the impact of adverse weather on vulnerable groups (pedestrians, cyclists).
4. Provide data-driven recommendations to policymakers for enhancing road safety.

---

## **Technologies Used**

The project uses the following tools and technologies to build a complete data pipeline:

| **Technology**      | **Purpose**                                                                    |
| ------------------- | ------------------------------------------------------------------------------ |
| **Apache Airflow**  | Automate and schedule ETL workflows to extract and process data from MongoDB.  |
| **DuckDB**          | Store and query structured datasets efficiently for analytical purposes.       |
| **dbt**             | Perform data transformations, build a star schema, and handle data governance. |
| **MongoDB**         | Store raw accident and weather data in an unstructured format.                 |
| **Streamlit**       | Create an interactive dashboard for visualizing traffic accident insights.     |
| **Data Privacy**    | Ensure sensitive data (e.g., GPS coordinates) is masked while querying.        |
| **Data Governance** | Used OpenMetadata for data lineage and governance.                             |
| **Redis**           | A high-performance in-memory database used for caching and quick lookups.      |

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

To organize the data for efficient analysis, we created a **star schema** with the following components:

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

- Accident and weather data are extracted from loaded into **MongoDB** using.

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
docker exec data-eng-project-airflow-worker-1 /mnt/scripts/create_airflow_users.bash
```

### Start Airflow

1. **Open Airflow Dashboard**: [http://localhost:8080](http://localhost:8080)
2. **Log in using:**
   - **Username**: `admin`
   - **Password**: `admin`
3. **Trigger the ingestions DAGs**
