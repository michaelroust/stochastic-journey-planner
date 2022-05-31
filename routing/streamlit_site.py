from pkg_resources import DEVELOP_DIST
import streamlit as st
import pandas as pd

pd.set_option("display.max_rows",40)
# pd.set_option("display.max_columns", 10)
import matplotlib.pyplot as plt

import plotly.express as px
import plotly.graph_objects as go

import numpy as np
from datetime import datetime

from PIL import Image

# import os
# st._LOGGER.info('Path' + os.getcwd())

#---------------------------------------------------------------

from utils import *
from algo import *
from visualization import *

#---------------------------------------------------------------

STOPS_RADIUS = 15000

PATH_STOPS_15K_PBZ2 = 'data/stops_15k_short.pbz2'
PATH_CONNECTIONS_PBZ2 = 'data/full_timetable.pbz2'
PATH_WALK_EDGES_15K_PBZ2 = 'data/walks_15k.pbz2'

df_stops = filter_stops_by_distance_from_zurich_hb(decompress_pickle(PATH_STOPS_15K_PBZ2), STOPS_RADIUS)
df_walks = filter_connections_by_stops(decompress_pickle(PATH_WALK_EDGES_15K_PBZ2), df_stops)
df_conns = filter_connections_by_stops(decompress_pickle(PATH_CONNECTIONS_PBZ2), df_stops)
dfs = (df_stops, df_walks, df_conns)


mapbox_access_token = 'pk.eyJ1IjoibWljaGFlbHJvdXN0IiwiYSI6ImNsM2tpbXlxdTA2dnUzY3AzdnZndWF2MGIifQ.eAlbvCcax9TMLeOyel2PdA'

#---------------------------------------------------------------

st.set_page_config(page_title='Stochastic journey Planner', page_icon='🚂', layout="wide")

df = df_stops.reset_index()

# col1, _, col2 = st.columns([4, 1, 2])
# with col1:
#     st.image(Image.open('assets/sbb_logo.png'))
# with col2:
#     st.image(Image.open('assets/graph.jpeg'))


st.write("## Stochastic journey planner")

col1, col2, col3= st.columns(3)
sorted_names = sorted(df.stop_name.unique())


SELECTION_ZURICH_HB_INDEX = 1140
with col1:
    dep_station = st.selectbox(
        'Search for a departing station',
        sorted_names,
        index=SELECTION_ZURICH_HB_INDEX
    )


with col3:
    arr_station = st.selectbox(
     'Search for an arrival station',
     sorted_names)

st.markdown('From *%s* to *%s*' % (dep_station,arr_station))

col1_hour, col2_min= st.columns(2)
with col1_hour:
    hour_user = st.number_input("Select hour", min_value=6, max_value=22, value=10)
with col2_min:
    min_user = st.number_input("Select minute", min_value=0, max_value=59, value=0)


DAY_OF_WEEK = 1
prob_connection = st.number_input("Select probability of getting connection", min_value=0.1, max_value=1.0, value=0.8)
day_of_week_name = st.selectbox(
     'Select a day in the week',
     ["Monday","Tuesday","Wednesday","Thursday","Friday"]
    )

dic_of_days = {"Monday":1,"Tuesday":2,"Wednesday":3,"Thursday":4,"Friday":5}
DAY_OF_WEEK = dic_of_days[day_of_week_name]



#select time
#select day (Monday-Sunday)


END_TIME = Time(h=hour_user,m=min_user).in_seconds()


END_ID = df[df.stop_name == arr_station].stop_id.iloc[0]
#END_ID = 8503016
#START_ID = 8576195
START_ID = df[df.stop_name == dep_station].stop_id.iloc[0]

col1_search, col2_search = st.columns([1,1])
with col1_search:
    run_search = st.button('Find optimal routes')
with col2_search:
    fast = st.checkbox('Fast search', value=True)


if run_search:
    with st.spinner('Finding best routes..'):
        for total_cost, cum_proba, route_data in generate_routes_gen(dfs, START_ID, END_ID, END_TIME, DAY_OF_WEEK, fast=fast, verbose=True,min_confidence=prob_connection):

            tot_time = Time.from_seconds(total_cost)

            if tot_time.h == 0:
                st.write(f'Total travel time: {round(tot_time.m)} min')
            else:
                st.write(f'Total travel time: {round(tot_time.h)} h {round(tot_time.m)} min')

            st.write(f'Probabilty to make all transfers: {cum_proba:.0%}')

            df_2 = print_directions(route_data,df[df.stop_id == END_ID].stop_name.iloc[0])
            st.write(df_2)

            # assign Graph Objects figure
            fig = visualize_itinerary(route_data,df_stops,START_ID,END_ID)
            # display streamlit map
            st.plotly_chart(fig)