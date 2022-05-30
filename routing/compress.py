
from utils import *

PATH_STOPS_15K_PBZ2 = 'data/stops_15k_short.pbz2'
PATH_CONNECTIONS_PBZ2 = 'data/full_timetable.pbz2'
PATH_WALK_EDGES_15K_PBZ2 = 'data/walks_15k.pbz2'

print('Starting compression')

df_s = load_df_stops()
compressed_pickle(PATH_STOPS_15K_PBZ2, df_s)

df_w = load_df_walks()
compressed_pickle(PATH_WALK_EDGES_15K_PBZ2, df_w)

df_c = load_df_connections()
compressed_pickle(PATH_CONNECTIONS_PBZ2, df_c)

print('Done')
