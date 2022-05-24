
import pandas as pd
from fuzzywuzzy import fuzz

#===============================================================

def fuzzy_search_best_stop(df_stops, search_str):
    return fuzzy_search_stops(df_stops, search_str).iloc[0]['stop_id']

def fuzzy_search_stops(df_stops, search_str):
    df_stops_search = df_stops.copy()
    df_stops_search['ratio'] = df_stops['stop_name'].map(lambda stop_name: fuzz.ratio(search_str, stop_name))
    df_stops_search.sort_values(by='ratio', ascending=False, inplace=True)
    return df_stops_search

#---------------------------------------------------------------

def time_to_seconds(hour, minute=0, second=0):
    return hour * 3600 + minute * 60 + second

def reformat_time(time_str):
    """Convert time of day to seconds since start of day"""
    temp = time_str.split(':')
    hour = int(temp[0])
    minute = int(temp[1])

    return time_to_seconds(hour, minute)

def load_stops():
    df_stops = pd.read_csv('../data/stops_15k.csv')
    df_stops.drop('Unnamed: 0',axis=1,inplace=True)
    df_stops.set_index('stop_id',inplace=True)
    return df_stops

def load_stop_times():
    df_connections = pd.read_csv('../data/connections_8_10.csv')
    df_connections.drop('Unnamed: 0',axis=1,inplace=True)

    df_connections['dep_time_s'] = df_connections['dep_time'].map(reformat_time)
    df_connections['arr_time_s'] = df_connections['arr_time'].map(reformat_time)

    return df_connections

def search_stops():
    pass