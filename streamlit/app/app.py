import duckdb
import numpy as np
import pandas as pd
import pydeck as pdk
import streamlit as st

from plotnine import ggplot, aes, facet_wrap, geom_histogram, geom_bar, geom_point, geom_smooth, geom_text, geom_line, geom_col, position_dodge2, labs, scale_x_discrete

from querier import CachedQuery

QUERIER = CachedQuery()
YEAR_QUERY = "SELECT DISTINCT year FROM time_dim ORDER BY year"
COUNTY_QUERY = "SELECT DISTINCT county FROM location_dim WHERE county NOT NULL ORDER BY county"


def prepare_scene():
    st.title('Traffic accidents dashboard')


def prepare_filters() -> dict[str, list | tuple]:
    years = QUERIER.query_cached(sql=YEAR_QUERY, cache_key='year')
    seasons = ['Winter', 'Fall', 'Summer', 'Spring']
    urban_values = ['JAH','EI']
    counties = QUERIER.query_cached(sql=COUNTY_QUERY, cache_key='county')
    min_speed, max_speed = 9, 121
    st.sidebar.header('Filters')

    # Possibly deprecated after next docker compose
    def urban_map(value):
       if(value=='JAH'):
          return "Urban"
       else:
          return "Rural"

    selected_years = st.sidebar.multiselect('Years', years, default=years)
    selected_urban = st.sidebar.multiselect('Urban or rural', urban_values, default=urban_values, format_func=urban_map)
    selected_speed = st.sidebar.slider(
        'Speed limit',
        min_value=min_speed,
        max_value=max_speed,
        value=(min_speed, max_speed)
    )
    selected_seasons = st.sidebar.multiselect('Season', seasons, default=seasons)
    selected_counties = st.sidebar.multiselect('County', counties, default=counties)



    return {
        'year': selected_years,
        'urban': selected_urban,
        'season': selected_seasons,
        'county':selected_counties,
        'speed': selected_speed
    }


def assert_selected(selections: dict[str, list | tuple]):
    if not selections['year']:
        st.write('No years selected')
        return False

    if not selections['season']:
        st.write('No seasons selected')
        return False

    if not selections['county']:
        st.write('No counties selected')
        return False
    if not selections['urban']:
        st.write('Neither urban nor rural areas selected')
        return False

    return True


def query_example(selections: dict[str, list | tuple]) -> pd.DataFrame:
    q = f"""
       SELECT 
          CASE
            WHEN r.highway_cars_per_day < 2000 THEN '0-2000'
            WHEN r.highway_cars_per_day >= 2000 AND r.highway_cars_per_day < 5000 THEN '2000-5000'
            WHEN r.highway_cars_per_day >= 5000 AND r.highway_cars_per_day < 10000 THEN '5000-10000'
            WHEN r.highway_cars_per_day >= 10000 THEN '10000+'
          END as density,
          ROUND(AVG(a.num_dead),4),
          CASE
            WHEN l.urban == 'JAH' THEN 'Urban'
            WHEN l.urban == 'EI' THEN 'Rural'
          END as urban
       FROM accident_fact a
       JOIN road_dim r ON a.road_id = r.id
       JOIN time_dim t ON a.time_id = t.id
       JOIN location_dim l ON a.location_id = l.id
       WHERE t.year IN {selections['year']} AND t.season IN {selections['season']}
           AND r.highway_cars_per_day NOT NULL AND l.county IN {selections['county']}
           AND l.urban IN {selections['urban']} AND r.speed_limit BETWEEN {selections['speed'][0]} AND {selections['speed'][1]}
       GROUP BY
         density,
         urban
       ORDER BY
         density
    """

    return QUERIER.query_cached(sql=q, cache_key='example' + str(selections))

def query_example_spatial(selections) -> pd.DataFrame:

    q = f"""
      SELECT
        CASE
          WHEN w.precipitation = 0.0 and w.wind_speed < 7 THEN 'Dry and calm'
          WHEN w.precipitation = 0.0 and w.wind_speed >= 7 THEN 'Dry and windy'
          WHEN w.precipitation > 0 AND w.wind_speed < 7 THEN 'Rainy and calm'
          WHEN w.precipitation > 0 AND w.wind_speed >= 7 THEN 'Rainy and windy'
        END as weather,
        AVG(a.num_injured) as injured,
        ROUND(AVG(a.num_dead),4) as deaths,
        ROUND(SUM(a.num_dead + a.num_injured) * 1.0 / COUNT(a.id), 2) AS avg_severity
      FROM accident_fact a
      JOIN weather_dim w ON a.weather_id = w.id
      JOIN location_dim l ON a.location_id = l.id
      JOIN time_dim t ON a.time_id = t.id
      JOIN road_dim r ON a.road_id = r.id
      WHERE w.precipitation NOT NULL and w.wind_speed NOT NULL
        AND t.year IN {selections['year']} AND t.season IN {selections['season']}
        AND l.county IN {selections['county']} AND l.urban IN {selections['urban']}
        AND r.speed_limit BETWEEN {selections['speed'][0]} AND {selections['speed'][1]}
      GROUP BY
       weather
    """
    return QUERIER.query_cached(sql=q, cache_key='example_2' + str(selections))


def plot_example(df: pd.DataFrame):
    if(len(df)==0 or len(df.columns)==0):
       st.write("Insufficient data to draw plot!")
       return
    df.rename(columns={0:"density", 1:"injuries", 2:"urban"}, inplace=True)
    print(df)
    g = ggplot(df) +\
        aes(x="density", y="injuries",fill="urban") +\
        geom_col(position="dodge") +\
        geom_text(aes(label = "injuries", group="urban"), position=position_dodge2(width=0.9,padding=0.4), va='bottom') +\
        scale_x_discrete(limits = ['0-2000', '2000-5000', '5000-10000', '10000+']) +\
        labs(x="Traffic density (cars per day)", y="Average deaths per accident", fill="Area")

    st.pyplot(ggplot.draw(g))


def plot_example_spatial(df: pd.DataFrame):
    if(len(df)==0 or len(df.columns)==0):
       st.write("Insufficient data to draw plot!")
       return

    df.rename(columns={0:"weather", 1:"injured",2:"deaths", 3:"severity"}, inplace=True)
    #print(df)
    g = ggplot(df) +\
        aes(x="weather", y="deaths") +\
        geom_bar(stat="identity", fill="#4BACC6") +\
        geom_text(aes(label = "deaths"), nudge_y=0.003)

    st.pyplot(ggplot.draw(g))


prepare_scene()
selections = prepare_filters()
is_selected = assert_selected(selections)

if is_selected:
    df_example = query_example(selections)
    plot_example(df_example)

    df_example_spatial = query_example_spatial(selections)
    plot_example_spatial(df_example_spatial)

