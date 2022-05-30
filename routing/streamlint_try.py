from pkg_resources import DEVELOP_DIST
import streamlit as st
import pandas as pd
import pickle
pd.set_option("display.max_rows",40)
# pd.set_option("display.max_columns", 10)
import matplotlib.pyplot as plt
import networkx as nx

from utils import *
from algo import *
from visualization import *

import plotly.express as px
import plotly.graph_objects as go

from scipy.stats import norm
import numpy as np


from datetime import datetime

mapbox_access_token = 'pk.eyJ1IjoibWljaGFlbHJvdXN0IiwiYSI6ImNsM2tpbXlxdTA2dnUzY3AzdnZndWF2MGIifQ.eAlbvCcax9TMLeOyel2PdA'


df  = pd.read_csv("../data/stops_15k.csv",index_col=[0])

from PIL import Image
image = Image.open('../assets/la (1).jpeg')
st.image(image)

col1, col2, col3= st.columns(3)
sorted_names = sorted(df.stop_name.unique())

with col1: 
    dep_station = st.selectbox(
     'Search for a departing station',
     sorted_names
    )

    st.write('From %s' % dep_station)

with col3:
    arr_station = st.selectbox(
     'Search for an arrival station',
     sorted_names)
    st.write('To %s' % arr_station)

hour_user = st.multiselect(
     'Select hour',
     list(range(7,23)))
min_user = st.multiselect(
     'Select minute',
     list(range(0,60)))


prob_connection = st.number_input("Select probability of getting connection", min_value=0.1, max_value=1.0, value=0.8)



#select time
#select day (Monday-Sunday)


END_TIME = Time(h=hour_user,m=min_user).in_seconds()


END_ID = df[df.stop_name == arr_station].stop_id.iloc[0]
#END_ID = 8503016
#START_ID = 8576195
START_ID = df[df.stop_name == dep_station].stop_id.iloc[0] 
DAY_OF_WEEK = 1

st.write("### Robust Journey planner")
if st.button('Run'):
    with st.spinner('Finding best routes..'):
        routes_data = generate_routes(START_ID, END_ID, END_TIME, DAY_OF_WEEK, verbose=True,min_confidence=prob_connection)
        df_2 = print_directions(routes_data[0],df[df.stop_id == END_ID].stop_name.iloc[0])
        st.write(df_2)

        # assign Graph Objects figure
        fig = visualize_itinerary(routes_data[0],df_stops,START_ID,END_ID)
        # display streamlit map
        st.plotly_chart(fig)

print(routes_data)