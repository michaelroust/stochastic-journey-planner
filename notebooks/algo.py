

from utils import *

# def build_journey_nice():

MAX_WAIT_TIME_S = Time(m=45).in_seconds()

def build_routes(start_stop_id, dest_stop_id, tgt_arrival_time_s:int, tgt_confidence):
    """Build N routes that go from A to B and arrive by tgt_arrival_time_s with tgt_confidence%"""
    pass


# def get_connections_from_strict(df_connections, stop_id, start_time_s, max_wait_time_s=MAX_WAIT_TIME_S):
#     depart_from_same_stop = df_connections['dep_stop_id'] == stop_id
#     depart_after_start = df_connections['dep_time_s'] > start_time_s
#     depart_before_end = df_connections['dep_time_s'] < (start_time_s + max_wait_time_s)

#     return df_connections[depart_from_same_stop & depart_after_start & depart_before_end]


def get_connections_to_strict(df_connections, stop_id, end_time_s, max_wait_time_s=MAX_WAIT_TIME_S, keep_n_cheapest=3):
    arrive_to_same_stop = df_connections['arr_stop_id'] == stop_id
    arrive_after_start = df_connections['arr_time_s'] >= (end_time_s - max_wait_time_s)
    arrive_before_end = df_connections['arr_time_s'] <= end_time_s

    # Apply all the masks/conditions (can be optimized by giving tighter connections interval).
    df_conns = df_connections[arrive_to_same_stop & arrive_after_start & arrive_before_end].copy()

    # Calculate weight of each connection
    #   weight = end_time_s - dep_time_s
    df_conns['weight'] = end_time_s - df_conns['dep_time_s']

    # Sort connections by weight
    df_conns.sort_values('weight', inplace=True)

    # Keep only 3 cheapest connections from each different stops
    df_conns = df_conns.groupby('dep_stop_id').head(keep_n_cheapest)

    return df_conns

# def