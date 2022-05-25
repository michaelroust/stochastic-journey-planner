
import pandas as pd
from utils import *
# fri

df_stops = load_df_stops()

print("=== Building walk time data ===")
df_stops_with_walk_time = build_walk_time_data(df_stops)
print("=== DONE ===")

df_stops_with_walk_time.to_csv(PATH_STOPS_WALK_TIME_15K)
