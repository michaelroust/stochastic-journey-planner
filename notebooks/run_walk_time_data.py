
import pandas as pd
from utils import *

RADIUS = 15000

df_stops = load_df_stops()

print("=== Building walk time data ===")
df_stops = filter_stops_by_distance_from_zurich_hb(load_df_stops(), RADIUS)
df_walks = walk_all_edge_list(df_stops)
print("=== DONE ===")

df_walks.to_csv(PATH_WALK_EDGES_15K)
