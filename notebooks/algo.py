

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


def get_connections_to_strict(df_connections, stop_id, end_time_s, max_wait_time_s=MAX_WAIT_TIME_S):
    arrive_to_same_stop = df_connections['arr_stop_id'] == stop_id
    arrive_after_start = df_connections['arr_time_s'] > (end_time_s - max_wait_time_s)
    arrive_before_end = df_connections['arr_time_s'] < end_time_s

    # Group by dep_stop_id
    # We could sort by and drop connections that depart sooner than later

    return df_connections[arrive_to_same_stop & arrive_after_start & arrive_before_end]

# def