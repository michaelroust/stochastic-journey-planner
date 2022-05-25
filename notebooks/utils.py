
from os import stat
import pandas as pd
from fuzzywuzzy import fuzz

# from geopy.distance import distance as geo_distance

from geopy import distance

ZURICH_HB_ID = 8503000
ZURICH_HB_LAT = 47.378176
ZURICH_HB_LON = 8.540212

ZURICH_WERD_ID = 8591427
ZURICH_WERD_LAT = 47.372324
ZURICH_WERD_LON = 8.526845

ZURICH_HB_ID = 8503090
ZURICH_HB_LAT = 47.372239
ZURICH_HB_LON = 8.531722

#===============================================================

# Max (As the Crows Flies") walking distances for transfers between two stops (meters)
MAX_WALKING_DIST = 500

# Meter's per second walking speed
WALKING_SPEED_MPS = 50 / 60

#---------------------------------------------------------------
# Time

class Time:
    """Utility class for a simple representation of time of day."""

    def __init__(self, h=0, m=0, s=0) -> None:
        self.h=h
        self.m=m
        self.s=s

    def in_seconds(self):
        return self.h * 3600 + self.m * 60 + self.s

    def __repr__(self) -> str:
        return f"Time({self.h}:{self.m}:{self.s})"

    def __add__(self, other):
        return Time.from_seconds(self.in_seconds() + other.in_seconds())

    def __sub__(self, other):
        return Time.from_seconds(self.in_seconds() - other.in_seconds())

    @staticmethod
    def from_seconds(seconds):
        h = seconds // 3600
        temp = (seconds - h*3600)
        m = temp // 60
        s = (temp - m*60)

        return Time(h, m, s)


def reformat_time(time_str):
    """Convert time of day to seconds since start of day"""
    temp = time_str.split(':')
    hour = int(temp[0])
    minute = int(temp[1])

    return Time(hour, minute).in_seconds()


#---------------------------------------------------------------
# Search functions - for conveniently searching the stops DataFrame

def fuzzy_search_best_stop(df_stops, search_str):
    return fuzzy_search_stops(df_stops, search_str).iloc[0]['stop_id']

def fuzzy_search_stops(df_stops, search_str):
    df_stops_search = df_stops.copy()
    df_stops_search['ratio'] = df_stops['stop_name'].map(lambda stop_name: fuzz.ratio(search_str, stop_name))
    df_stops_search.sort_values(by='ratio', ascending=False, inplace=True)
    return df_stops_search


#---------------------------------------------------------------
# Data loading

# TODO Methods may need modifications as data evolves.

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
    # df_connections.drop(['dep_time', 'arr_time'],axis=1,inplace=True) # TODO maybe uncomment this?

    df_connections['std'] = 66

    return df_connections


#---------------------------------------------------------------
# Data manipulation


def filter_by_time_interval(df_connections, start_time:Time, end_time:Time):
    """Filters out connections that aren't within a time interval"""
    return filter_by_seconds_interval(df_connections, start_time.in_seconds(), end_time.in_seconds())

def filter_by_seconds_interval(df_connections, start_time_s:int, end_time_s:int):
    """Filters out connections that aren't within a time interval defined in seconds."""
    return df_connections[(df_connections['dep_time_s'] > start_time_s) & (df_connections['arr_time_s'] < end_time_s)]



def filter_stops_by_distance_from_zurich_hb(df_stops, dist):
    """Filters out any stops that aren't within dist (meters) of Zurich HB"""
    df_dist = df_stops.apply(lambda row:
        geo_distance((ZURICH_HB_LAT, ZURICH_HB_LON), (row['latitude'], row['longitude'])),
        axis=1)
    return df_stops[df_dist < dist]

    # return filter_stops_by_distance(df_stops, (ZURICH_HB_LAT, ZURICH_HB_LON), dist_km).drop('dist',axis=1)



def filter_connections_by_stops(df_connections, df_stops):
    """Can be used to take a subset of connections. For example we can only take stops from a certain region
    which could have been filtered with filter_stops_by_distance_from_zurich_hb for example."""
    return df_stops.reset_index()[['stop_id']].merge(df_connections, left_on='stop_id', right_on='dep_stop_id').drop('stop_id',axis=1)


#---------------------------------------------------------------





#---------------------------------------------------------------

# TODO figure out WTF did I do here

def stops_in_walking_distance(df_stops, pos:tuple, dist=MAX_WALKING_DIST):
    """Filters out any stops not in walking distance. Returns a df_stops with added distance and walk_time columns."""
    df_close_stops, ser_dist = filter_stops_by_distance(df_stops, pos, dist)
    ser_walk_time = ser_dist.map(walking_time)
    return df_close_stops, ser_walk_time


def filter_stops_by_distance(df_stops, pos:tuple, dist):
    """Filters out any stops that aren't within dist (meters). Returns df_stops and df_distance."""
    ser_dist = df_stops.apply(lambda row:
        geo_distance(pos, (row['latitude'], row['longitude'])),
        axis=1)

    mask = ser_dist < dist
    return df_stops[mask], ser_dist[mask]


def build_walking_dist_data(df_stops, pos:tuple):
    """Outputs a list of tuples of stop_ids and walking times from a given stop_id"""
    _, ser_walk_time = stops_in_walking_distance(df_stops, pos)

    # walk_time_series = walk_time_series.sort_values()   # TODO This line can be commented for efficiency

    return list(zip(ser_walk_time.index.values, ser_walk_time.values))

# def build_walking_dist_data(df_stops):
#     df_stops


#---------------------------------------------------------------
# Misc

# All distances in meters in algo/project for consistency

def geo_distance(a:tuple, b:tuple):
    return distance.distance(a, b).m

def walking_time(dist):
    return round(dist / WALKING_SPEED_MPS)
