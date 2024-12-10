import duckdb
import pandas as pd
import pydeck as pdk
import streamlit as st

from plotnine import * # TODO: only import what's needed

DUCKDB = "duckdb/duck.db"
YEAR_QUERY = "SELECT DISTINCT year FROM time_dim ORDER BY year"
COUNTY_QUERY = "SELECT DISTINCT county FROM location_dim ORDER BY county"


def prepare_scene():
    st.title('Traffic accidents dashboard')


def prepare_filters() -> dict[str, list | tuple]:
    con = duckdb.connect(DUCKDB, read_only=True)

    years = con.sql(YEAR_QUERY).fetchall()
    seasons = ['Winter', 'Fall', 'Summer', 'Spring']
    is_weekday = [True, False]
    counties = con.sql(COUNTY_QUERY).fetchall()
    is_urban = [True, False]
    min_speed, max_speed = 10, 110

    st.sidebar.header('Filters')
    selected_years = st.sidebar.multiselect('Years', years, default=years)
    selected_seasons = st.sidebar.multiselect('Season', seasons, default=seasons)
    selected_counties = st.sidebar.multiselect('County', counties, default=counties)
    selected_speed = st.sidebar.slider(
        'Speed limit',
        min_value=min_speed,
        max_value=max_speed,
        value=(min_speed, max_speed)
    )

    return {
        'year': selected_years,
        'season': selected_seasons, 
        'county':selected_counties,
        'speed': selected_speed
    }


def assert_selected(selections: dict[str, list | tuple]):
    if not selections['year']:
        st.write('No years selected')
        return False

    if not selections['season']:
        st.write('No seasons selected'):
        return False

    if not selections['county']:
        st.write('No counties selected')
        return False

    return True


def query_example(selections: dict[str, list | tuple]) -> pd.DataFrame:
    con = duckdb.connect(DUCKDB, read_only=True)

    q = f"""
        SELECT t.season, l.county
        FROM accident_fact AS a
        JOIN time_dim AS t ON a.time_id = t.id
        WHERE t.year IN {selections['year']} AND t.season IN {selections['season']}
        JOIN location_dim AS l ON a.location_id = l.id
        WHERE l.county IN {selections['county']}
        JOIN road_dim AS r ON a.road_id = r.id
        WHERE r.speed_limit BETWEEN {selections['speed']}
    """

    res = con.sql(q).fetchall()
    return pd.DataFrame(res)


prepare_scene()
selections = prepare_filters()
is_selected = assert_selected(selections)

if is_selected:
    df = query_example(selections)

    g = ggplot(df) +\
        aes('season') +\
        facet_wrap('county') +\
        geom_histogram()

    # TODO: use pydeck for map charts
    pdk.Layer('scatter')
