import pandas as pd
pd.set_option("display.max_rows",40)
# pd.set_option("display.max_columns", 10)
import matplotlib.pyplot as plt

from utils import *

import plotly.express as px
import plotly.graph_objects as go

from scipy.stats import norm
import numpy as np


from datetime import datetime


#helper function
def get_lat_long(id, df_stops):
    return (df_stops.loc[id].latitude, df_stops.loc[id].longitude)

def visualize_itinerary(df_connections, df_stops,START_ID,END_ID):

    #start color 2, end color 1

    df_stops['color'] = 0
    df_stops.loc[END_ID,'color'] = 1
    df_stops.loc[START_ID,'color'] = 2


    df_stops['color'] = df_stops['color'].astype(str)
    fig = px.scatter_mapbox(df_stops, hover_name="stop_name", lat="latitude", lon="longitude", color='color', zoom=10, opacity = 1)
    lats = []
    lons = []
    for _, row in df_connections.iterrows():
        loc_dep = get_lat_long(row['dep_stop_id'],df_stops)
        loc_arr = get_lat_long(row['arr_stop_id'],df_stops)

        lats.append(loc_dep[0])
        lats.append(loc_arr[0])
        lons.append(loc_dep[1])
        lons.append(loc_arr[1])

    fig.add_trace(go.Scattermapbox(
        mode = "lines",
        lon = lons,
        lat = lats,
        hoverinfo='none'
    ))
    fig.update_traces(line=dict(color="Purple",width=3))
    fig.update_layout(
    mapbox = {
    'style': "open-street-map"},
    showlegend = False
    )

    return fig



#print directions

def print_directions(result_from_algo, end_stop_name):
    result = result_from_algo.copy()
    rows = []
    for row in range(len(result)):
        # check if last trip id was the same
        if row != 0:
            if result.iloc[row]['trip_id'] == result.iloc[row - 1]['trip_id']:
                continue
        walking_route = result.iloc[row]['trip_id'] == 'walk'
        if not walking_route:
            next_row = row + 1
            same_trip_id = True
            while same_trip_id:
                try:
                    same_trip_id = result.iloc[next_row]['trip_id'] == result.iloc[row]['trip_id']
                except IndexError:
                    same_trip_id = False
                if same_trip_id:
                    result.iloc[row]['arr_stop_name'] = result.iloc[next_row]['arr_stop_name']
                    result.iloc[row]['arr_time_s'] = result.iloc[next_row]['arr_time_s']
                    next_row += 1
                else:
                    break
        dep_sec = Time.from_seconds(result.iloc[row]['dep_time_s']) if not walking_route else ''
        arr_sec = Time.from_seconds(result.iloc[row]['arr_time_s']) if not walking_route else ''
        if not walking_route:
            dep_sec = datetime(2019, 1, 1, int(dep_sec.h), int(dep_sec.m), int(dep_sec.s)).strftime('%H:%M')
            arr_sec = datetime(2019, 1, 1, int(arr_sec.h), int(arr_sec.m), int(arr_sec.s)).strftime('%H:%M')
        else:
            walk_minutes = round(Time.from_seconds(result.iloc[row]['weight']).m)

            result.iloc[row]['dep_stop_name'] = ""
            result.iloc[row]['arr_stop_name'] = ""
            result.iloc[row]['transport_name'] = f"{walk_minutes} minute walk"
            try:
                result.iloc[row]['trip_headsign'] = result.iloc[row + 1]['dep_stop_name']
            except IndexError:
                result.iloc[row]['trip_headsign'] = end_stop_name
            result.iloc[row]['route_short_name'] = ""
        result.iloc[row]['transport_name'] = "Train" if result.iloc[row]['transport_name'] == 'zug' else result.iloc[row]['transport_name'].capitalize()
        result.iloc[row]['transport_name'] = f"{result.iloc[row]['transport_name']} {result.iloc[row]['route_short_name']} towards {result.iloc[row]['trip_headsign']}"
        rows += [[
            result.iloc[row]['transport_name'],
            result.iloc[row]['dep_stop_name'],
            dep_sec,
            result.iloc[row]['arr_stop_name'],
            arr_sec
        ]]
    pretty_output = pd.DataFrame(rows, columns=['Route', 'From', 'Departing at', 'To', 'Arriving at', ]).reset_index().rename({'index': 'Connection #'}, axis=1)
    pretty_output['Connection #'] += 1
    return pretty_output


# preprocess
def preprocess(df, df_stops):
    final_df_dep = df.merge(df_stops, left_on ='dep_stop_id', right_on=df_stops.index)
    final_df_dep.rename(columns = {'stop_name':'dep_name'}, inplace=True)
    final_df_arr = final_df_dep.merge(df_stops,left_on='arr_stop_id', right_on='stop_id')
    final_df_arr.rename(columns = {'stop_name':'arr_name'}, inplace=True)
    d = final_df_arr.drop(columns=['mean','std','dep_stop_id', 'latitude_x','longitude_x', 'color_x' , 'latitude_y','longitude_y', 'color_y'])
    d['trip_time'] = np.where(d['trip_id'] == 'walk', d['weight'], np.abs(d['dep_time_s']-d['arr_time_s']))
    d['dep_time_s']= d['dep_time_s'].apply(lambda x : Time.from_seconds(x))
    d['arr_time_s']= d['arr_time_s'].apply(lambda x : Time.from_seconds(x))
    d['dep_time_s'] = np.where(d['trip_id'] == 'walk', '-', d['dep_time_s'])
    d['arr_time_s'] = np.where(d['trip_id'] == 'walk', '-', d['arr_time_s'])
    d['proba'] = d['proba'].astype(float)
    return d

#display confidence level
#display number of routes
#display direction and number of buds