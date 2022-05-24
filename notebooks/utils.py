
import pandas as pd

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