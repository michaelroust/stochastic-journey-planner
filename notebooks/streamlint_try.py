from pkg_resources import DEVELOP_DIST
import streamlit as st
import pandas as pd

import pickle
pd.set_option("display.max_rows",40)
import matplotlib.pyplot as plt
from utils import *
from algo import *

from scipy.stats import norm

#https://towardsdatascience.com/build-a-multi-layer-map-using-streamlit-2b4d44eb28f3

df  = pd.read_csv("../data/stops_15k.csv",index_col=[0])

from PIL import Image
image = Image.open('../assets/canva.png')
st.image(image)

col1, col2, col3 = st.columns(3)
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



##need to insert start and end station + map it to id
##need to add prob option


END_TIME = Time(h=10).in_seconds()


#END_ID = df[df.stop_name == arr_station].stop_id.iloc[0] //not working yet
#START_ID = df[df.stop_name == dep_station].stop_id.iloc[0] //not working yet
END_ID = ZURICH_HB_ID
START_ID = 8591192
DAY_OF_WEEK = 1

st.write("### Robust Journey planner")
if st.button('Run'):
    with st.spinner('Finding best routes..'):
        routes_data = generate_routes(START_ID, END_ID, END_TIME, DAY_OF_WEEK, verbose=False)
        print(routes_data)
        st.write(routes_data[0])

