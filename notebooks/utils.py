
import pandas as pd
from fuzzywuzzy import fuzz

# from geopy.distance import distance as geo_distance

from geopy import distance

ZURICH_HB_ID = '8503000'
ZURICH_HB_LAT = 47.378176
ZURICH_HB_LON = 8.540212

ZURICH_WERD_ID = '8591427'
ZURICH_WERD_LAT = 47.372324
ZURICH_WERD_LON = 8.526845

ZURICH_HB_ID = '8503090'
ZURICH_HB_LAT = 47.372239
ZURICH_HB_LON = 8.531722

#===============================================================

#---------------------------------------------------------------
# Time math

def time_to_seconds(hour, minute=0, second=0):
    return hour * 3600 + minute * 60 + second

def seconds_to_time(seconds):
    hour = seconds // 3600
    temp = (seconds - hour*3600)
    minute = temp // 60
    second = (temp - minute*60)
    return hour, minute, second


def reformat_time(time_str):
    """Convert time of day to seconds since start of day"""
    temp = time_str.split(':')
    hour = int(temp[0])
    minute = int(temp[1])

    return time_to_seconds(hour, minute)


#---------------------------------------------------------------
# Search functions

def fuzzy_search_best_stop(df_stops, search_str):
    return fuzzy_search_stops(df_stops, search_str).iloc[0]['stop_id']

def fuzzy_search_stops(df_stops, search_str):
    df_stops_search = df_stops.copy()
    df_stops_search['ratio'] = df_stops['stop_name'].map(lambda stop_name: fuzz.ratio(search_str, stop_name))
    df_stops_search.sort_values(by='ratio', ascending=False, inplace=True)
    return df_stops_search


#---------------------------------------------------------------
# Loading Dataframes


def load_df_stops():
    df_stops = pd.read_csv('../data/stops_15k.csv')
    df_stops.drop('Unnamed: 0',axis=1,inplace=True)
    df_stops.set_index('stop_id',inplace=True)
    return df_stops

def load_df_connections():
    df_connections = pd.read_csv('../data/connections_8_10.csv')
    df_connections.drop('Unnamed: 0',axis=1,inplace=True)

    df_connections['dep_time_s'] = df_connections['dep_time'].map(reformat_time)
    df_connections['arr_time_s'] = df_connections['arr_time'].map(reformat_time)

    return df_connections


#---------------------------------------------------------------
# Data manipulation


def filter_by_hour_interval(df_connections, start_time_h, end_time_h):
    return filter_by_seconds_interval(df_connections, time_to_seconds(start_time_h), time_to_seconds(end_time_h))


def filter_by_seconds_interval(df_connections, start_time_s, end_time_s):
    return df_connections[(df_connections['dep_time_s'] > start_time_s) & (df_connections['arr_time_s'] < end_time_s)]


def filter_stops_by_distance_from_zurich_hb(df_stops, dist_km):
    df_dist = df_stops.apply(lambda row:
        geo_distance_km((ZURICH_HB_LAT, ZURICH_HB_LON), (row['latitude'], row['longitude'])),
        axis=1)

    return df_stops[df_dist < dist_km]


def filter_connections_by_stops(df_connections, df_stops):
    return df_stops.reset_index()[['stop_id']].merge(df_connections, left_on='stop_id', right_on='dep_stop_id').drop('stop_id',axis=1)


#---------------------------------------------------------------
# Misc

def geo_distance_km(a:tuple, b:tuple):
    return distance.distance(a, b).km

