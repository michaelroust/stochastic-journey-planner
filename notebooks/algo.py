
from IPython.display import display
from queue import PriorityQueue
from math import inf
from utils import *
from scipy.stats import norm
import sys

#===============================================================

MAX_WAIT_TIME = Time(m=45).in_seconds()
KEEP_N_CHEAPEST = 1

STOPS_RADIUS = 3000





#===============================================================
# Setup

df_stops = filter_stops_by_distance_from_zurich_hb(load_df_stops(), STOPS_RADIUS)
df_walks = filter_connections_by_stops(load_df_walks(), df_stops)
df_conns = filter_connections_by_stops(load_df_connections(), df_stops)

#===============================================================


def get_connections_walk(dest_stop_id):
    return df_walks[df_walks['arr_stop_id'] == dest_stop_id].copy()


def get_connections_trans(df_conns, dest_stop_id, end_time_s, prev_trip_id, max_wait_time=MAX_WAIT_TIME, keep_n_cheapest=KEEP_N_CHEAPEST):
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


def get_connections(df_conns, dest_stop_id, end_time_s, prev_trip_id, max_wait_time=MAX_WAIT_TIME, keep_n_cheapest=KEEP_N_CHEAPEST):
    df_conns_trans = get_connections_trans(df_conns, dest_stop_id, end_time_s, prev_trip_id, max_wait_time, keep_n_cheapest)

    # Don't give 2 consecutive walk connections
    if prev_trip_id == 'walk':
        return df_conns_trans
    else:
        df_conns_walk = get_connections_walk(dest_stop_id)
        df_conns_walk['trip_id'] = 'walk'
        df_conns_walk['mean'] = 0
        df_conns_walk['std'] = 0

        return pd.concat([df_conns_trans, df_conns_walk], ignore_index=True)


def neighbors(df_conns, dest_stop_id, end_time_s, prev_trip_id):
    df_conns = get_connections(df_conns, dest_stop_id, end_time_s, prev_trip_id)

    for _, conn in df_conns.iterrows():

        # Read the departure time (for cost/distance calculation)
        dep_time_s = conn['dep_time_s']

        mean_arr_time = conn['arr_time_s'] + conn['mean']
        std_arr_time = conn['std']
        proba = norm.cdf(end_time_s, loc=mean_arr_time, scale=std_arr_time)

        # If departure time is null this is a walking edge then we calculate the
        # 'departure' time according to the next departure time.
        if pd.isnull(dep_time_s):
            dep_time_s = end_time_s - conn['weight']

            proba = 1.0

        conn['proba'] = proba
        yield conn['trip_id'], conn['dep_stop_id'], dep_time_s, proba, conn


#===============================================================




def build_route(prev_trip_id, prev, distances, probas, conn_datas, start_id, end_id):
    """Build the path from start to end. We also populate a pandas dataframe
    for conventiently accessing the route information."""

    if start_id not in prev.keys():
        return None

    # Initalize parameters
    node = start_id
    trip_id = prev_trip_id[node]
    dist = distances[start_id]
    cum_proba = 1.0

    path = []
    path_conn_datas = []
    while node != end_id:
        # Update outputs
        path.append((trip_id, node, dist))
        path_conn_datas.append(conn_datas[node])
        cum_proba *= probas[node]

        # Update next node, trip_id and distance
        node = prev[node]
        trip_id = prev_trip_id[node]
        dist = distances[node]

    # Update outputs 1 last time
    path.append((trip_id, node, dist))
    path_conn_datas.append(conn_datas[node])
    cum_proba *= probas[node]
    return path, cum_proba, pd.concat(path_conn_datas, axis=1).T


# end_id = ZURICH_HB_ID
# end_time = Time(h=10).in_seconds()

def probabilistic_constrained_dijkstra(df_conns, start_id, end_id, end_time, min_confidence=0.8, verbose=False):

    def build_route_proba(interm_id):
        """Build the path to given intermediate_id. Used to obtain probabilty of following
        this route until iterm_id. That probability is then used to only select edges
        that are above our chosen confidence threshold."""
        if interm_id not in prev.keys():
            return None, 1.0

        # Initalize parameters
        node = interm_id
        dist = distances[interm_id]
        cum_proba = 1.0

        path = []
        while node != end_id:

            # Update outputs
            path.append((node, dist))
            cum_proba *= probas[node]

            # Update next node and distance
            node = prev[node]
            dist = distances[node]

        # Update outputs 1 last time
        path.append((node, dist))
        cum_proba *= probas[node]
        return path, cum_proba


    # Initialize parameter and output collections
    distances = {}      # stores travel_times
    prev = {}           # stores predecessor
    prev_trip_id = {}   # stores prev_trip_id
    probas = {}
    conn_datas = {}     # TODO we can replace this with only needed data

    visited = set()     # stores already visited stop_ids
    queue = PriorityQueue()

    # Initialize dicts for destination node
    distances[end_id] = 0
    prev[end_id] = None
    prev_trip_id[end_id] = None
    probas[end_id] = 1
    conn_datas[end_id] = None

    queue.put((distances[end_id], (end_id, end_time)))

    while not queue.empty():

        # Get next node to traverse from queue.
        _, (curr_id, curr_time) = queue.get()

        # Break if we have reached the start.
        if curr_id == start_id:
            break

        # Update visited nodes.
        visited.add(curr_id)

        # Retrive cumulated probabilty until curr_id node
        _, cum_proba = build_route_proba(curr_id)

        # Loop to traverse edges connecting all valid neighbours.
        for trip_id, neighbor_id, neighbor_dep_time_s, proba, conn_data in neighbors(df_conns, curr_id, curr_time, prev_trip_id[curr_id]):

            # Distance calculation that works for both walking and transportation times.
            new_dist = end_time - neighbor_dep_time_s

            # If neighbor is a valid do relaxation.
            if (new_dist < distances.get(neighbor_id, inf)) and (cum_proba * proba > min_confidence):

                # Update all collections.
                distances[neighbor_id] = new_dist
                prev[neighbor_id] = curr_id
                prev_trip_id[neighbor_id] = trip_id
                probas[neighbor_id] = proba
                conn_datas[neighbor_id] = conn_data

                # Update visited nodes if its a new node.
                if neighbor_id not in visited:
                    visited.add(neighbor_id)
                    queue.put((distances[neighbor_id], (neighbor_id, neighbor_dep_time_s)))

                    if verbose:
                        print(f'Visited {len(visited)}/{df_stops.shape[0]}')

    return build_route(prev_trip_id, prev, distances, probas, conn_datas, start_id, end_id)


#===============================================================

def generate_routes(start_id, end_id, end_time:int, min_confidence=0.8, nroutes=5, max_iter=10, verbose=False):
    """Build N routes that go from A to B and arrive by tgt_arrival_time_s with tgt_confidence%"""

    routes_datas = []

    df_conns_dynamic = df_conns.copy()
    # removed_edges = set()

    print("Starting routing")
    i = 0
    while i < max_iter and len(routes_datas) < nroutes:

        temp = probabilistic_constrained_dijkstra(df_conns_dynamic, start_id, end_id, end_time, min_confidence)
        if temp != None:
            _, cum_proba, path_conn_datas = temp
        else:
            if verbose: print("NO MORE ROUTES FOUND")
            break
        # if cum_proba > min_confidence:
        routes_datas.append(path_conn_datas)

        # Drop connection with lowest probability of making it
        lowest_proba_conn = path_conn_datas.sort_values(by='proba', axis=0).index[0]
        df_conns_dynamic.drop(lowest_proba_conn, inplace=True)

        i += 1
        if verbose:
            print(f"Iteration: {i} - Found routes: {len(routes_datas)}")
            print(f"Probability: {cum_proba}")
            display(path_conn_datas)
            print('---------------------------------------------')
            # sys.stdout.flush()


    return routes_datas
