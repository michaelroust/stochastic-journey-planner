
from queue import PriorityQueue
from math import inf
from utils import *


#===============================================================

MAX_WAIT_TIME = Time(m=45).in_seconds()
KEEP_N_CHEAPEST = 1

STOPS_RADIUS = 15000





#===============================================================
# Setup

df_stops = filter_stops_by_distance_from_zurich_hb(load_df_stops(), STOPS_RADIUS)
df_walks = filter_connections_by_stops(load_df_walks(), df_stops)
df_conns = filter_connections_by_stops(load_df_connections(), df_stops)

#===============================================================


def get_connections_walk(dest_stop_id):
    return df_walks[df_walks['arr_stop_id'] == dest_stop_id].copy()


def get_connections_trans(dest_stop_id, end_time_s, prev_trip_id, max_wait_time=MAX_WAIT_TIME, keep_n_cheapest=KEEP_N_CHEAPEST):
    arrive_to_same_stop = df_conns['arr_stop_id'] == dest_stop_id
    arrive_after_start = df_conns['arr_time_s'] >= (end_time_s - max_wait_time)

    arrive_before_end_no_delay = df_conns['arr_time_s'] <= end_time_s
    arrive_before_end_delay = df_conns['arr_time_s'] <= (end_time_s - BASE_TRANSFER_TIME)

    # Conditions to check if we apply delay calculation
    if (prev_trip_id == 'walk' or prev_trip_id == None):
        consider_transfer_delay = False
        # is_same_trip_id = False
    else:
        consider_transfer_delay = df_conns['trip_id'] != prev_trip_id
        # is_same_trip_id = df_conns['trip_id'] == prev_trip_id
    arrive_before_end = (consider_transfer_delay & arrive_before_end_delay) | (~consider_transfer_delay & arrive_before_end_no_delay)

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


def get_connections(dest_stop_id, end_time_s, prev_trip_id, max_wait_time=MAX_WAIT_TIME, keep_n_cheapest=KEEP_N_CHEAPEST):
    df_conns_trans = get_connections_trans(dest_stop_id, end_time_s, prev_trip_id, max_wait_time, keep_n_cheapest)

    # Don't give 2 consecutive walk connections
    if prev_trip_id == 'walk':
        return df_conns_trans
    else:
        df_conns_walk = get_connections_walk(dest_stop_id)
        df_conns_walk['trip_id'] = 'walk'
        df_conns_walk['mean'] = 0
        df_conns_walk['std'] = 0

        return pd.concat([df_conns_trans, df_conns_walk], ignore_index=True)


def neighbors(dest_stop_id, end_time_s, prev_trip_id):
    df_conns = get_connections(dest_stop_id, end_time_s, prev_trip_id)

    # TODO adapt returned values as needed
    for _, conn in df_conns.iterrows():

        # Read the departure time (for cost/distance calculation)
        dep_time_s = conn['dep_time_s']

        # If departure time is null this is a walking edge then we calculate the
        # 'departure' time according to the next departure time.
        if pd.isnull(dep_time_s):
            dep_time_s = end_time_s - conn['weight']

        yield conn['trip_id'], conn['dep_stop_id'], dep_time_s, conn


#===============================================================

def build_route(prev_trip_id, prev, distances, conn_datas, start_id, end_id):

    if start_id not in prev.keys():
        return None

    node = start_id
    # prev_id = prev[start_id]
    trip_id = prev_trip_id[node]
    dist = distances[start_id]
    path = []
    path_conn_datas = []
    while node != end_id:
        path.append((trip_id, node, dist))
        path_conn_datas.append(conn_datas[node])

        node = prev[node]
        trip_id = prev_trip_id[node]
        dist = distances[node]
    # route.append(node,dist)
    path.append((trip_id, node, dist))
    path_conn_datas.append(conn_datas[node])
    return path, pd.concat(path_conn_datas, axis=1).T


# end_id = ZURICH_HB_ID
# end_time = Time(h=10).in_seconds()

def dijkstra_base(start_id, end_id, end_time, print_progress=False):

    distances = {}      # stores travel_times
    prev = {}           # stores predecessor
    prev_trip_id = {}   # stores prev_trip_id
    # prev_travel_times = {}
    conn_datas = {}     # TODO we can replace this with only needed data

    visited = set()     # stores already visited stop_ids
    queue = PriorityQueue()

    distances[end_id] = 0
    prev[end_id] = None
    prev_trip_id[end_id] = None
    conn_datas[end_id] = None
    # prev_travel_times[end_id] = 0

    queue.put((distances[end_id], (end_id, end_time)))

    # TODO? Add thing to stop once we reach target?
    while not queue.empty():
        _, (curr_id, curr_time) = queue.get()
        if curr_id == start_id:
            break

        visited.add(curr_id)
        for trip_id, neighbor_id, neighbor_dep_time_s, conn_data in neighbors(curr_id, curr_time, prev_trip_id[curr_id]):
            new_dist = end_time - neighbor_dep_time_s
            # new_dist = distances[curr_id] + neighbor_weight
            if new_dist < distances.get(neighbor_id, inf):

                distances[neighbor_id] = new_dist
                prev[neighbor_id] = curr_id
                prev_trip_id[neighbor_id] = trip_id
                conn_datas[neighbor_id] = conn_data

                if neighbor_id not in visited:
                    visited.add(neighbor_id)
                    queue.put((distances[neighbor_id], (neighbor_id, neighbor_dep_time_s)))

                    if print_progress:
                        print(f'Visited {len(visited)}/{df_stops.shape[0]}')

    return build_route(prev_trip_id, prev, distances, conn_datas, start_id, end_id)


#===============================================================

def build_routes(start_stop_id, dest_stop_id, tgt_arrival_time_s:int, tgt_confidence):
    """Build N routes that go from A to B and arrive by tgt_arrival_time_s with tgt_confidence%"""
    pass

