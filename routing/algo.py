
from IPython.display import display
from queue import PriorityQueue
from math import inf
from utils import *
from scipy.stats import norm, gamma

#===============================================================

MAX_WAIT_TIME = Time(m=45).in_seconds()

STOPS_RADIUS = 15000


#===============================================================
# Setup

df_stops = filter_stops_by_distance_from_zurich_hb(load_df_stops(), STOPS_RADIUS)
df_walks = filter_connections_by_stops(load_df_walks(), df_stops)
df_conns = filter_connections_by_stops(load_df_connections(), df_stops)

#===============================================================


def get_connections_walk(dest_stop_id):
    """Get walking connections from precomputed data."""
    return df_walks[df_walks['arr_stop_id'] == dest_stop_id].copy()


def get_connections_trans(df_conns, dest_stop_id, end_time_s, prev_trip_id, max_wait_time=MAX_WAIT_TIME, keep_n_cheapest=1):
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


def get_connections(df_conns, dest_stop_id, end_time_s, prev_trip_id, max_wait_time=MAX_WAIT_TIME, keep_n_cheapest=1):
    df_conns_trans = get_connections_trans(df_conns, dest_stop_id, end_time_s, prev_trip_id, max_wait_time, keep_n_cheapest)

    if prev_trip_id == 'walk':
        # Don't give 2 consecutive walk connections
        return df_conns_trans
    else:
        # Get walk connections and walk trip_id.
        df_conns_walk = get_connections_walk(dest_stop_id)
        df_conns_walk['trip_id'] = 'walk'

        # Walk connections have no distribution
        df_conns_walk['mean'] = 0
        df_conns_walk['std'] = 0

        return pd.concat([df_conns_trans, df_conns_walk], ignore_index=False)


def neighbors(df_conns, dest_stop_id, end_time_s, prev_trip_id, keep_n_cheapest=1):
    """Function to get valid neighbours for constrained dijkstra."""
    df_conns = get_connections(df_conns, dest_stop_id, end_time_s, prev_trip_id, keep_n_cheapest=keep_n_cheapest)

    for _, conn in df_conns.iterrows():

        # If departure time is null this is a walking edge then we calculate the
        # 'departure' time according to the next departure time.
        # if pd.isnull(dep_time_s) or (conn['trip_id'] == prev_trip_id):
        if (conn['trip_id'] == 'walk') or (conn['trip_id'] == prev_trip_id):
            dep_time_s = end_time_s - conn['weight']

            proba = 1.0
        else:
            # Read the departure time (for cost/distance and probability calculation)
            dep_time_s = conn['dep_time_s']

            # Calculate probabilty according to our chosen model.
            mean_arr_time = conn['arr_time_s'] + conn['mean']
            std_arr_time = conn['std']

            # Quick fix of std to avoid division by 0.
            if std_arr_time == 0.0:
                std_arr_time = 0.01

            # Normal distribution
            # proba = norm.cdf(end_time_s, loc=mean_arr_time, scale=std_arr_time)

            # Gamma distribution
            shape = (mean_arr_time/std_arr_time)**2
            scale = (std_arr_time**2)/mean_arr_time
            proba = gamma.cdf(end_time_s, a=shape, scale=scale)

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



def probabilistic_constrained_dijkstra(df_conns, start_id, end_id, end_time, min_confidence=0.8, verbose=False):

    # Initialize parameter and output collections
    distances = {}      # stores travel_times
    prev = {}           # stores predecessor
    prev_trip_id = {}   # stores prev_trip_id
    probas = {}
    cum_probas = {}
    conn_datas = {}     # TODO we can replace this with only needed data

    visited = set()     # stores already visited stop_ids
    queue = PriorityQueue()

    # Initialize dicts for destination node
    distances[end_id] = 0
    prev[end_id] = None
    prev_trip_id[end_id] = None
    probas[end_id] = 1.0
    cum_probas[end_id] = 1.0
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
        # _, cum_proba = build_route_proba(curr_id)
        cum_proba = cum_probas[curr_id]

        # Loop to traverse edges connecting all valid neighbours.
        for trip_id, neighbor_id, neighbor_dep_time_s, proba, conn_data in neighbors(df_conns, curr_id, curr_time, prev_trip_id[curr_id]):

            # Distance calculation that works for both walking and transportation times.
            new_dist = end_time - neighbor_dep_time_s

            neighbor_cum_proba = cum_proba * proba
            # If neighbor is a valid do relaxation.
            if (new_dist < distances.get(neighbor_id, inf)):
            # if (new_dist < distances.get(neighbor_id, inf)) and (neighbor_cum_proba > min_confidence):

                # Update all collections.
                distances[neighbor_id] = new_dist
                prev[neighbor_id] = curr_id
                prev_trip_id[neighbor_id] = trip_id
                probas[neighbor_id] = proba
                cum_probas[neighbor_id] = neighbor_cum_proba
                conn_datas[neighbor_id] = conn_data

                # Update visited nodes if its a new node.
                if neighbor_id not in visited:
                    visited.add(neighbor_id)
                    queue.put((distances[neighbor_id], (neighbor_id, neighbor_dep_time_s)))

                    if verbose:
                        print(f'Visited {len(visited)}/{df_stops.shape[0]}')

    return build_route(prev_trip_id, prev, distances, probas, conn_datas, start_id, end_id)


def probabilistic_constrained_dijkstra_multigraph(df_conns, start_id, end_id, end_time, min_confidence=0.8, fast=True, verbose=False):

    # Initialize parameter and output collections
    distances = {}      # stores travel_times
    prev = {}           # stores predecessor
    prev_trip_id = {}   # stores prev_trip_id
    probas = {}
    cum_probas = {}
    conn_datas = {}     # TODO we can replace this with only needed data

    # visited = set()     # stores already visited stop_ids
    queue = PriorityQueue()

    # Initialize dicts for destination node
    distances[end_id] = 0
    prev[end_id] = None
    prev_trip_id[end_id] = None
    probas[end_id] = 1.0
    cum_probas[end_id] = 1.0
    conn_datas[end_id] = None

    queue.put((distances[end_id], (end_id, end_time)))

    while not queue.empty():

        # Get next node to traverse from queue.
        _, (curr_id, curr_time) = queue.get()

        # Break if we have reached the start.
        if fast and curr_id == start_id:
            break

        # Update visited nodes.
        # visited.add(curr_id)

        # Retrive cumulated probabilty until curr_id node
        # _, cum_proba = build_route_proba(curr_id)
        cum_proba = cum_probas[curr_id]

        # Loop to traverse edges connecting all valid neighbours.
        for trip_id, neighbor_id, neighbor_dep_time_s, proba, conn_data in neighbors(df_conns, curr_id, curr_time, prev_trip_id[curr_id], 3):

            # Distance calculation that works for both walking and transportation times.
            new_dist = end_time - neighbor_dep_time_s

            neighbor_cum_proba = cum_proba * proba

            is_self_loop = curr_id == prev.get(prev[end_id], -1)
            # If neighbor is a valid do relaxation.
            # if (new_dist < distances.get(neighbor_id, inf)):
            if (new_dist < distances.get(neighbor_id, inf)) and (neighbor_cum_proba > min_confidence) and (not is_self_loop):

                # Update all collections.
                distances[neighbor_id] = new_dist
                prev[neighbor_id] = curr_id
                prev_trip_id[neighbor_id] = trip_id
                probas[neighbor_id] = proba
                cum_probas[neighbor_id] = neighbor_cum_proba
                conn_datas[neighbor_id] = conn_data

                # Update visited nodes if its a new node.
                # if neighbor_id not in visited:
                #     visited.add(neighbor_id)
                queue.put((distances[neighbor_id], (neighbor_id, neighbor_dep_time_s)))

                    # if verbose:
                    #     print(f'Visited {len(visited)}/{df_stops.shape[0]}')

    return build_route(prev_trip_id, prev, distances, probas, conn_datas, start_id, end_id)


#===============================================================

def generate_routes(start_id, end_id, end_time:int, day_of_week:int, min_confidence=0.8, nroutes=3, max_iter=10, verbose=False):
    """Build N routes that go from A to B and arrive by tgt_arrival_time_s with tgt_confidence%.

    :param int day_of_week: int value between 1 and 7. (1=Monday, 2=Tuesday...)"""

    routes_datas = []

    df_conns_dynamic = df_conns[df_conns['dayofweek'] == day_of_week].drop('dayofweek', axis=1).copy()
    # removed_edges = set()

    print("Starting routing")
    i = 0
    while i < max_iter and len(routes_datas) < nroutes:

        # temp = probabilistic_constrained_dijkstra(df_conns_dynamic, start_id, end_id, end_time, min_confidence)
        temp = probabilistic_constrained_dijkstra_multigraph(df_conns_dynamic, start_id, end_id, end_time, min_confidence)
        if temp != None:
            path, cum_proba, path_conn_datas = temp
        else:
            if verbose: print("NO MORE ROUTES FOUND")
            break

        if cum_proba > min_confidence:
            routes_datas.append(path_conn_datas)

        # Drop connection with lowest probability of making it
        path_conn_datas_transport = path_conn_datas[path_conn_datas['trip_id'] != 'walk']
        lowest_proba_conn = path_conn_datas_transport.sort_values(by='proba', axis=0).index[0]
        df_conns_dynamic.drop(lowest_proba_conn, inplace=True)

        i += 1
        if verbose:
            print(f"Iteration: {i} - Found routes: {len(routes_datas)}")
            print(f"Probability: {cum_proba}")
            print(f"Route cost: {path[0][2]}")
            display(path_conn_datas)
            print('---------------------------------------------')
            # sys.stdout.flush()


    return routes_datas


def generate_routes_gen(start_id, end_id, end_time:int, day_of_week:int, min_confidence=0.8, nroutes=3, max_iter=10, verbose=False):
    """Build N routes that go from A to B and arrive by tgt_arrival_time_s with tgt_confidence%.

    :param int day_of_week: int value between 1 and 7. (1=Monday, 2=Tuesday...)"""

    routes_datas = []

    df_conns_dynamic = df_conns[df_conns['dayofweek'] == day_of_week].drop('dayofweek', axis=1).copy()
    # removed_edges = set()

    print("Starting routing")
    i = 0
    while i < max_iter and len(routes_datas) < nroutes:

        # temp = probabilistic_constrained_dijkstra(df_conns_dynamic, start_id, end_id, end_time, min_confidence)
        temp = probabilistic_constrained_dijkstra_multigraph(df_conns_dynamic, start_id, end_id, end_time, min_confidence)
        if temp != None:
            path, cum_proba, path_conn_datas = temp
        else:
            if verbose: print("NO MORE ROUTES FOUND")
            break

        if cum_proba > min_confidence:
            routes_datas.append(path_conn_datas)
            # yield path_conn_datas

        # Drop connection with lowest probability of making it
        path_conn_datas_transport = path_conn_datas[path_conn_datas['trip_id'] != 'walk']
        lowest_proba_conn = path_conn_datas_transport.sort_values(by='proba', axis=0).index[0]
        df_conns_dynamic.drop(lowest_proba_conn, inplace=True)

        i += 1
        if verbose:
            print(f"Iteration: {i} - Found routes: {len(routes_datas)}")
            print(f"Probability: {cum_proba}")
            print(f"Route cost: {path[0][2]}")
            display(path_conn_datas)
            print('---------------------------------------------')
            # sys.stdout.flush()

        if cum_proba > min_confidence:
            yield path_conn_datas


###########################################################################
# Copy of generate routes to consider the modification w.r.t Yen's algorithm

def generate_routes_yen(start_id, end_id, end_time:int, day_of_week:int, min_confidence=0.8, nroutes=5, max_iter=10, verbose=False):
    """Build N routes that go from A to B and arrive by tgt_arrival_time_s with tgt_confidence%.

    :param int day_of_week: int value between 1 and 7. (1=Monday, 2=Tuesday...)"""

    routes_datas = []
    nb_routes_found = 0

    df_conns_dynamic = df_conns[df_conns['dayofweek'] == day_of_week].drop('dayofweek', axis=1).copy()
    # booleans to keep track of possible route alternatives and duplicate routes
    route_alternatives_bool = 1
    route_duplicate = 0
    # removed_edges = set()

    print("Starting routing")
    i = 0
    while i < max_iter and nb_routes_found < nroutes:

        temp = probabilistic_constrained_dijkstra(df_conns_dynamic, start_id, end_id, end_time, min_confidence)
        if (temp != None) and route_alternatives_bool:
            path, cum_proba, path_conn_datas = temp
            route_cost = path[0][2]
        else:
            break

        # check if we already stored the same route
        for r in routes_datas:
            if path_conn_datas['trip_id'].equals(r[0]['trip_id']) and path_conn_datas['dep_time_s'].equals(r[0]['dep_time_s']) and len(path_conn_datas) == len(r[0]):
                route_duplicate = 1
        # if not add to the possible routes
        # also check that the probability is above the threshold
        if cum_proba > min_confidence and not route_duplicate:
            routes_datas.append((path_conn_datas, cum_proba, route_cost))
            nb_routes_found = len(routes_datas)

        # save the best route to later find alternatives by dropping connections
        if nb_routes_found == 1:
            first_df_conns_dynamic = df_conns_dynamic.copy()
            path_conn_datas_transport = routes_datas[0][0][routes_datas[0][0]['trip_id'] != 'walk']

        # if nb of found routes < k then look for alternatives
        if nb_routes_found < nroutes:
            # check that alternatives are still possible
            if (nb_routes_found-1 < len(path_conn_datas_transport) and (not route_duplicate or i < len(path_conn_datas_transport))):
                # Drop connections in the order of those in the best path
                conn_to_drop = path_conn_datas_transport.index[nb_routes_found-1]
                df_conns_dynamic = first_df_conns_dynamic.drop(conn_to_drop)
            else:
                route_alternatives_bool = 0
        i += 1

    # sort the routes by total_cost
    routes_datas = sorted(routes_datas, key=lambda x: x[2])

    if verbose:
        for idx, route in enumerate(routes_datas):
            print(f"Found route: {idx+1}")
            print(f"Probability: {route[1]}")
            print(f"Route cost: {route[2]}")
            display(route[0])
            print('---------------------------------------------')
        if (len(routes_datas) < nroutes):
            print("NO MORE ROUTES FOUND")

    return routes_datas
