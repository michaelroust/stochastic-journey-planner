
from os import stat
import pandas as pd
from fuzzywuzzy import fuzz

import bz2
# import pickle
import _pickle as cPickle

from geopy import distance

#===============================================================
# Constants

#---------------------------------------------------------------
# Paths

PATH_STOPS_15K = '../data/stops_15k_short.csv'
PATH_STOPS_15K_PBZ2 = '../data/stops_15k_short.pbz2'

PATH_CONNECTIONS = '../data/full_timetable.csv'
PATH_CONNECTIONS_PBZ2 = '../data/full_timetable.pbz2'

PATH_WALK_EDGES_15K = '../data/walks_15k.csv'
PATH_WALK_EDGES_15K_PBZ2 = '../data/walks_15k.pbz2'

#---------------------------------------------------------------
# Numerical

ZURICH_HB_ID = 8503000
ZURICH_HB_LAT = 47.378176
ZURICH_HB_LON = 8.540212

ZURICH_WERD_ID = 8591427
ZURICH_WERD_LAT = 47.372324
ZURICH_WERD_LON = 8.526845

# ZURICH_HB_ID = 8503090
# ZURICH_HB_LAT = 47.372239
# ZURICH_HB_LON = 8.531722

ZURICH_HEURIED_ID = 8591190
ZURICH_HEURIED_LAT = 47.369410
ZURICH_HEURIED_LON = 8.506354


# Max (As the Crows Flies") walking distances for transfers between two stops (meters)
MAX_WALKING_DIST = 500

# Meter's per second walking speed
WALKING_SPEED_MPS = 50 / 60

BASE_TRANSFER_TIME = 120

#===============================================================
# Code

#---------------------------------------------------------------
# Time

class Time:
    """Utility class for a simple representation of time of day."""

    def __init__(self, h=0, m=0, s=0) -> None:
        self.h=h
        self.m=m
        self.s=s

    def in_seconds(self) -> int:
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
# Compression


# Pickle a file and then compress it into a file with extension
def compressed_pickle(path, data):
    with bz2.BZ2File(path, 'wb') as f:
        return cPickle.dump(data, f)

# Load any compressed pickle file
def decompress_pickle(path):
    with bz2.BZ2File(path, 'rb') as f:
        return cPickle.load(f)


#---------------------------------------------------------------
# Data loading

# TODO Methods may need modifications as data evolves.

def load_df_stops():
    df_stops = pd.read_csv(PATH_STOPS_15K)
    df_stops.drop('Unnamed: 0',axis=1,inplace=True)
    df_stops.drop_duplicates('stop_id', inplace=True)
    df_stops.set_index('stop_id',inplace=True)
    return df_stops

# def load_df_stops_walk_time():
#     df_stops_walk_time = pd.read_csv(PATH_STOPS_WALK_TIME_15K)
#     df_stops_walk_time.set_index('stop_id',inplace=True)
#     return df_stops_walk_time

def load_df_walks():
    df_walks = pd.read_csv(PATH_WALK_EDGES_15K)
    df_walks.drop('Unnamed: 0',axis=1,inplace=True)
    df_walks.sort_index(axis=1, inplace=True)
    return df_walks

def load_df_connections():
    # df_connections = pd.read_csv(PATH_CONNECTIONS_8_10)
    df_connections = pd.read_csv(PATH_CONNECTIONS)
    df_connections.drop('Unnamed: 0',axis=1,inplace=True)
    df_connections.drop('time_period',axis=1, inplace=True)
    df_connections.rename({'delay_mean':'mean', 'delay_std':'std'},axis=1, inplace=True)

    df_connections['dep_time_s'] = df_connections['dep_time'].map(reformat_time)
    df_connections['arr_time_s'] = df_connections['arr_time'].map(reformat_time)
    df_connections.drop(['dep_time', 'arr_time'],axis=1,inplace=True)
    df_connections.drop_duplicates(['dep_stop_id', 'arr_stop_id', 'dep_time_s', 'arr_time_s', 'dayofweek', 'transport_name'], inplace=True)

    return df_connections


# def load_df_stops_compressed(path):
#     return decompress_pickle(path)

# def load_df_walks_compressed():
#     return decompress_pickle(PATH_WALK_EDGES_15K_PBZ2)

# def load_df_connections_compressed():
#     return decompress_pickle(PATH_CONNECTIONS_PBZ2)


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

def stops_in_walking_distance(df_stops, stop_id, pos:tuple, max_dist=MAX_WALKING_DIST):
    """Filters out any stops not in walking distance. Returns a df_stops with added distance and walk_time columns."""
    ser_dist = filter_stops_by_distance(df_stops, stop_id, pos, max_dist)
    ser_walk_time = (ser_dist / WALKING_SPEED_MPS + BASE_TRANSFER_TIME).astype(int)
    # ser_walk_time['dep_stop_id'] =

    return ser_walk_time
    # return ser_dist.map(walking_time)


def filter_stops_by_distance(df_stops, stop_id, pos:tuple, max_dist):
    """Filters out any stops that aren't within dist (meters). Returns df_stops and df_distance."""
    ser_dist = df_stops.apply(lambda row:
        geo_distance(pos, (row['latitude'], row['longitude'])),
        axis=1)

    mask = (ser_dist < max_dist)
    return ser_dist[mask].drop(index=stop_id)


def walk_edge_list(df_stops, arr_stop_id, arr_pos:tuple):
    ser_walk_time = stops_in_walking_distance(df_stops, arr_stop_id, arr_pos)
    df_walk = ser_walk_time.to_frame()
    df_walk.reset_index(inplace=True)
    df_walk.rename({0:'weight', 'stop_id':'dep_stop_id'}, axis=1, inplace=True)
    df_walk['arr_stop_id'] = arr_stop_id
    return df_walk


def walk_all_edge_list(df_stops) -> pd.DataFrame:
    temp = []
    total_stops = df_stops.shape[0]
    i = 0
    for index, row in df_stops.iterrows():
        temp.append(walk_edge_list(df_stops, index, (row['latitude'], row['longitude'])))
        print(f'Processed stops: {i}/{total_stops}')
        i += 1

    return pd.concat(temp,ignore_index=True)


# Deprecated
def build_walk_time_adj_list(df_stops, stop_id, pos:tuple, prints=False):
    """Outputs a list of tuples of stop_ids and walking times from a given stop_id"""
    if prints: print(stop_id)
    ser_walk_time = stops_in_walking_distance(df_stops, stop_id, pos)
    ser_walk_time = ser_walk_time.sort_values()   # TODO This line can be commented for efficiency

    return list(zip(ser_walk_time.index.values, ser_walk_time.values))

# Deprecated
def build_walk_time_adj_data(df_stops, prints=False):

    ser_walk_time = df_stops.apply(
        lambda row:
            build_walk_time_adj_list(df_stops, row.name, (row['latitude'], row['longitude']), prints),
        axis=1)

    df_stops_with_walk_time = df_stops.copy()
    df_stops_with_walk_time['walk_time'] = ser_walk_time

    return df_stops_with_walk_time


#---------------------------------------------------------------
# Misc

# All distances in meters in algo/project for consistency

def geo_distance(a:tuple, b:tuple):
    return distance.distance(a, b).m

def walking_time(dist):
    return round(dist / WALKING_SPEED_MPS)
