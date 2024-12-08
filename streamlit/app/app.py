import duckdb
import pydeck as pdk
import streamlit as st

from plotnine import * # TODO: only import what's needed

# TODO: mount duckdb volume and fix path
DUCKDB = "duck.db"

con = duckdb.connect(DUCKDB, read_only=True)

st.title("Traffic accidents dashboard")


# TODO: create filters, for example by year, location ...
st.sidebar.header("Filters")

# TODO: ggplot some stuff
g = ggplot() + aes() + ...

# TODO: use pydeck for map charts
pdk.Layer("scatter")
