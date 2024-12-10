import duckdb
import pandas as pd
import pydeck as pdk
import streamlit as st

from plotnine import ggplot, aes, facet_wrap, geom_histogram

DUCKDB = "duckdb/duck.db"
YEAR_QUERY = "SELECT DISTINCT year FROM time_dim ORDER BY year"
COUNTY_QUERY = "SELECT DISTINCT county FROM location_dim ORDER BY county"


def prepare_scene():
    st.title('Traffic accidents dashboard')


def prepare_filters() -> dict[str, list | tuple]:
    con = duckdb.connect(DUCKDB, read_only=True)

    years = con.sql(YEAR_QUERY).fetchall()
    seasons = ['Winter', 'Fall', 'Summer', 'Spring']
    counties = con.sql(COUNTY_QUERY).fetchall()
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
        FROM accident_fact a
        JOIN time_dim t ON a.time_id = t.id
        WHERE t.year IN {selections['year']} AND t.season IN {selections['season']}
        JOIN location_dim l ON a.location_id = l.id
        WHERE l.county IN {selections['county']}
        JOIN road_dim r ON a.road_id = r.id
        WHERE r.speed_limit BETWEEN {selections['speed']}
    """

    res = con.sql(q).fetchall()
    return pd.DataFrame(res)


def query_example_spatial(selections) -> pd.DataFrame:
    con = duckdb.connect(DUCKDB, read_only=True)
    q = f"""
        SELECT l.gps_x, l.gps_y, a.number_injured, a.number_fatalities
        FROM accident_fact a
        JOIN time_dim t ON a.time_id = t.id
        WHERE t.year IN {selections['year']} AND t.season IN {selections['season']}
        JOIN location_dim l ON a.location_id = l.id
        JOIN road_dim r ON a.road_id = r.id
        WHERE r.speed_limit BETWEEN {selections['speed']}
    """

    res = con.sql(q).fetchall()
    return pd.DataFrame(res)


def plot_example(df: pd.DataFrame):
    # TODO: verify there is data

    g = ggplot(df) +\
        aes('season') +\
        facet_wrap('county') +\
        geom_histogram()

    st.pyplot(ggplot.draw(g))


def plot_example_spatial(df: pd.DataFrame):
    # TODO: verify there is data

    layer = pdk.Layer(
        'ScatterPlotLayer',
        data=df,
        get_position=['gps_x', 'gps_y'],
    )

    view_state = pdk.ViewState(latitude=59.44, longitude=24.75, zoom=6)
    deck = pdk.Deck(layers=layer, initial_view_state=view_state)

    st.pydeck_chart(deck)


prepare_scene()
selections = prepare_filters()
is_selected = assert_selected(selections)

if is_selected:
    df_example = query_example(selections)
    plot_example(df_example)

    df_example_spatial = query_example_spatial(selections)
    plot_example_spatial(df_example_spatial)

