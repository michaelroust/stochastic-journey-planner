
from utils import *


#===============================================================

MAX_WAIT_TIME = Time(m=45).in_seconds()

STOPS_RADIUS = 2000





#===============================================================
# Setup

df_stops = filter_stops_by_distance_from_zurich_hb(load_df_stops(), STOPS_RADIUS)
df_walks = filter_connections_by_stops(load_df_walks(), df_stops)
df_conns = filter_connections_by_stops(load_df_connections(), df_stops)

#===============================================================


def get_connections_walk(dest_stop_id):
    return df_walks[df_walks['arr_stop_id'] == dest_stop_id].copy()


def get_connections_trans(dest_stop_id, end_time_s, max_wait_time=MAX_WAIT_TIME, keep_n_cheapest=3):
    arrive_to_same_stop = df_conns['arr_stop_id'] == dest_stop_id
    arrive_after_start = df_conns['arr_time_s'] >= (end_time_s - max_wait_time)
    arrive_before_end = df_conns['arr_time_s'] <= end_time_s

    # Apply all the masks/conditions (can be optimized by giving tighter connections interval).
    df_edges = df_conns[arrive_to_same_stop & arrive_after_start & arrive_before_end].copy()

    # Calculate weight of each connection
    #   weight = end_time_s - dep_time_s
    df_edges['weight'] = end_time_s - df_edges['dep_time_s']

    # Sort connections by weight
    df_edges.sort_values('weight', inplace=True)

    # Keep only 3 cheapest connections from each different stops
    df_edges = df_edges.groupby('dep_stop_id').head(keep_n_cheapest)

    return df_edges


def get_connections(dest_stop_id, end_time_s, max_wait_time=MAX_WAIT_TIME, keep_n_cheapest=3):
    df_conns_trans = get_connections_trans(dest_stop_id, end_time_s, max_wait_time, keep_n_cheapest)

    df_conns_walk = get_connections_walk(dest_stop_id)
    df_conns_walk['trip_id'] = 'walk'
    df_conns_walk['mean'] = 0
    df_conns_walk['std'] = 0

    return pd.concat([df_conns_trans, df_conns_walk], ignore_index=True)




#===============================================================

def build_routes(start_stop_id, dest_stop_id, tgt_arrival_time_s:int, tgt_confidence):
    """Build N routes that go from A to B and arrive by tgt_arrival_time_s with tgt_confidence%"""
    pass

