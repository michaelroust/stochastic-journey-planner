
from utils import *

PATH_WALK_EDGES_15K_2 = '../data/walks_15k.csv'

print('Starting walk (edge) generation')

df_stops = load_df_stops()

df_walks = walk_all_edge_list(df_stops)

df_walks.to_csv(PATH_WALK_EDGES_15K_2)

print('Done')


