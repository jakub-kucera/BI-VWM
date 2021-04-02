from typing import List

from rtree.database import Database
from rtree.database_entry import DatabaseEntry
from rtree.node import Node
from rtree.default_config import *
import pickle

# maybe rename to Index, use rtree only as module name
class RTree:
    def __init__(self):  # list all possible parameters / use **kwargs

        # path to binary file with saved tree / already opened file object
        self.tree_file = DEFAULT_TREE_FILE_PATH

        self.dimensions = 2

        # size of one parameter/dimension, or same for all?,
        self.sizes_of_dimensions = [4, 4]
        # self.size_of_dimensions = 4

        # maximum children nodes per node, maybe calculate from a given block size (see moodle otazky)
        self.children_per_node = 50

        # tree depth (start from 0 or 1?)
        self.depth

        # node id size in file, probably use 8B
        self.id_size

        self.trunk_node

        # variable size? maybe change based on tree depth?
        self.cache = [self.trunk]

        self.node_size  # = 4 * 1024

        self.offset_size

        # database object (database.py)
        self.database = Database()

        # used if we decide to use parallel searching
        self.max_threads = 8


        # self.

    def __del__(self):
        pass  # maybe delete file

    # search for node in tree based on coordinates
    def search_node(self, coordinates) -> DatabaseEntry: # -> Node:  # maybe allow to return list of NOdes
        pass

    def search_rectangle(self, rectangle) -> List[DatabaseEntry]:
        pass

    def search_nearest_k_neighbours(self, k: int) -> List[DatabaseEntry]:
        pass

    def __get_block_adrress(self, block_id: int) -> int:
        # todo bound check
        current_block = self.offset_size + block_id * self.block_size
        return current_block

    # gets node directly from file, based on id
    def __get_node(self, node_id: int) -> Node:
        pass

    def __get_node_cached(self, node_id: int) -> Node:
        cache_position = len(self.cache) % node_id

        if self.cache[cache_position].node_id == node_id:
            return self.cache[cache_position]
        else:
            self.cache[cache_position] = self.__get_node(node_id)

        return self.cache[cache_position]

    def insert_entry(self, *entries: List[DatabaseEntry]):  # maybe change from *args to list, might be more memory efficient, idk
        for entry in entries:
            pass
            # insert

    def delete_entry(self, *entries: List[DatabaseEntry]):
        for entry in entries:
            pass

    def linear_search(self, parameters):
        pass

    def __delete_node(self):
        pass

    def rebuild(self):
        pass