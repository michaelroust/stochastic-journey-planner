from pkg_resources import DEVELOP_DIST
import streamlit as st
import pandas as pd
import pickle
pd.set_option("display.max_rows",40)
# pd.set_option("display.max_columns", 10)
import matplotlib.pyplot as plt
import networkx as nx

import plotly.express as px
import plotly.graph_objects as go

from scipy.stats import norm
import numpy as np
from datetime import datetime

#---------------------------------------------------------------

from utils import *
from algo import *
from visualization import *

mapbox_access_token = 'pk.eyJ1IjoibWljaGFlbHJvdXN0IiwiYSI6ImNsM2tpbXlxdTA2dnUzY3AzdnZndWF2MGIifQ.eAlbvCcax9TMLeOyel2PdA'

#---------------------------------------------------------------

# df = df_stops
df  = pd.read_csv("data/stops_15k_short.csv",index_col=[0])

from PIL import Image
image = Image.open('assets/la (1).jpeg')
st.image(image)

st.write("### Stochastic Journey planner")

col1, col2, col3= st.columns(3)
sorted_names = sorted(df.stop_name.unique())

with col1:
    dep_station = st.selectbox(
     'Search for a departing station',
     sorted_names
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



if st.button('Search for best route'):
    with st.spinner('Finding best routes..'):
        for route_data in generate_routes_gen(START_ID, END_ID, END_TIME, DAY_OF_WEEK, verbose=True,min_confidence=prob_connection):
            df_2 = print_directions(route_data,df[df.stop_id == END_ID].stop_name.iloc[0])
            st.write(df_2)

            # assign Graph Objects figure
            fig = visualize_itinerary(route_data,df_stops,START_ID,END_ID)
            # display streamlit map
            st.plotly_chart(fig)