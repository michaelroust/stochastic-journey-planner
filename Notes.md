
### TODO

 - Add desired data into dijkstra algo
 - Print path
 - Add edge dropping logic
 - Add stochastic logic

Dijkstra edge selection taking probability into account.


### Stuff to install

pip install fuzzywuzzy
pip install python-Levenshtein
pip install geopy
pip install networkx


## File structure

 - `algo.py`: Algorithm logic.
 - `utils.py`: Generally useful functions for whole project.
 - `run_walk_time_data.py`: Generate walk_time list for each stop.

## Random

Throughout that algorithm part of the project.
We rigourously stick to the following units:
- Distance: meters
- Time: seconds (except when using Time class)


If routing algo gives a path with multiple sequential walking nodes just drop a node or smth.


`df.stops.merge(df_connections, left_on='stop_id', right_on='dep_stop_id')`


distance.distance((ZURICH_HB_LAT, ZURICH_HB_LON), (ZURICH_WERD_LAT, ZURICH_WERD_LON)).km
