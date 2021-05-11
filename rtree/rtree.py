import errno
import secrets
from typing import List, Optional, Tuple
import os
from hashlib import sha1
from psutil import cpu_count

from rtree.default_config import *
from rtree.database import Database
from rtree.database_entry import DatabaseEntry
from rtree.mbb import MBB
from rtree.rtree_node import RTreeNode
from rtree.cache import Cache
from rtree.tree_file_handler import TreeFileHandler


class RTree:
    """Main class for RTree indexing"""

    @staticmethod
    def calculate_config_hash(input_params: List[int]) -> bytes:
        """Calculates hash key from given list of integers and from randomly generated value"""
        hash_func = sha1()
        for param in input_params:
            hash_func.update(param.to_bytes(length=4, byteorder=TREE_BYTEORDER))

        return hash_func.digest()

    @staticmethod
    def get_sequence_and_hash_from_file(file_name: str) -> Tuple[bytes, bytes]:
        """Opens binary file and reads config hash and unique sequence from the file beginning"""
        try:
            file = open(file_name, 'r+b')
            unique_sequence = file.read(UNIQUE_SEQUENCE_LENGTH)
            config_hash = file.read(CONFIG_HASH_LENGTH)
            file.close()
            return unique_sequence, config_hash
        except OSError:
            raise OSError(f"Cannot read unique_sequence and config_hash from file: {file_name}")

    @staticmethod
    def try_delete_file(file_name: str) -> bool:
        """Attempts to delete file. Returns True when successful, or if the file didnt exist"""
        try:
            os.remove(file_name)
        except OSError as e:
            return e.errno == errno.ENOENT  # if file didnt exist
        return True

    @staticmethod
    def check_files_load_existing_rtree(tree_file: str, database_file: str, override: bool) -> bool:
        """Checks files if they can be used and if an existing tree is supposed to be loaded"""
        try:
            if not os.path.isdir(WORKING_DIRECTORY):
                os.mkdir(WORKING_DIRECTORY)
        except OSError:
            raise OSError(f"Cannot open or create working directory: {WORKING_DIRECTORY}")

        tree_exists = os.path.isfile(tree_file)
        database_exists = os.path.isfile(database_file)

        # deletes files if they are supposed to be overridden
        if override:
            if not RTree.try_delete_file(tree_file):
                raise OSError(f"Error: Couldn't delete file: {tree_file}")
            if not RTree.try_delete_file(database_file):
                raise OSError(f"Error: Couldn't delete file: {database_file}")
            tree_exists = False
            database_exists = False

        # when only one file is created
        if tree_exists is not database_exists:
            raise ValueError("Only one file is created, the other one is missing")

        # when both files are not created
        if (not tree_exists) and (not database_exists):
            return False

        # when both files are created
        rtree_file_sequence, rtree_file_hash = RTree.get_sequence_and_hash_from_file(tree_file)
        database_file_sequence, database_file_hash = RTree.get_sequence_and_hash_from_file(database_file)
        # if both files are compatible
        if rtree_file_hash == database_file_hash and rtree_file_sequence == database_file_sequence:
            return True
        raise ValueError(f"Error: RTree file ({tree_file}) is not compatible with database file ({database_file})")

    def __init__(self, working_directory: str = WORKING_DIRECTORY,
                 tree_file: str = DEFAULT_TREE_FILE,
                 database_file: str = DEFAULT_DATABASE_FILE,
                 override_file: bool = False,
                 dimensions: int = DEFAULT_DIMENSIONS,
                 parameters_size: int = PARAMETER_RECORD_SIZE,
                 id_size: int = NODE_ID_SIZE,
                 node_size: int = DEFAULT_NODE_SIZE,
                 max_threads: int = None):

        # path to binary file with saved tree / already opened file object
        self.tree_filename = working_directory + tree_file
        self.database_filename = working_directory + database_file

        # checks if files exists and are valid.
        load_from_files = self.check_files_load_existing_rtree(tree_file=self.tree_filename,
                                                               database_file=self.database_filename,
                                                               override=override_file)

        # number of parameters used to index entries
        self.dimensions = dimensions

        # size of one parameter in bytes
        self.parameters_size = parameters_size

        # node id size in file
        self.id_size = id_size

        # size of memory in Bytes to store one tree node
        self.node_size = node_size

        # id of trunk node
        self.trunk_id = 0

        # tree depth todo (start from 0 or 1?)
        self.depth = 0

        self.deleted_db_entries_counter = 0

        # randomly generated sequence of bytes
        self.unique_sequence = secrets.token_bytes(UNIQUE_SEQUENCE_LENGTH)

        # Generate an hash based on RTree parameters
        self.config_hash = self.calculate_config_hash(
            [self.dimensions, self.node_size, self.id_size, self.parameters_size])

        # Number of threads that can be used # todo delete?
        self.max_threads = cpu_count() if max_threads is None else max_threads

        # object that directly interacts with a file where the rtree is stored
        self.tree_handler = TreeFileHandler(filename=self.tree_filename, dimensions=self.dimensions,
                                            node_size=self.node_size, id_size=self.id_size, tree_depth=self.depth,
                                            parameters_size=self.parameters_size, trunk_id=self.trunk_id,
                                            unique_sequence=self.unique_sequence, config_hash=self.config_hash)

        self.children_per_node = self.tree_handler.children_per_node

        if load_from_files:
            # override attributes by values from tree_handler and database
            # print("LOADING FROM FILES")
            self.config_hash = self.tree_handler.config_hash
            self.unique_sequence = self.tree_handler.unique_sequence
            self.node_size = self.tree_handler.node_size
            self.dimensions = self.tree_handler.dimensions
            self.id_size = self.tree_handler.id_size
            self.trunk_id = self.tree_handler.trunk_id
            self.tree_depth = self.tree_handler.tree_depth
            self.parameters_size = self.tree_handler.parameters_size
        else:
            trunk_node_new = RTreeNode.create_emtpy_node(self.dimensions, is_leaf=True)
            self.trunk_id = self.tree_handler.write_node(trunk_node_new)

        # creates database file handler
        self.database = Database(filename=self.database_filename, dimensions=self.dimensions,
                                 parameters_size=self.parameters_size,
                                 unique_sequence=self.unique_sequence, config_hash=self.config_hash)

        # if not load_from_files:
        #     new_entry = DatabaseEntry([0 for coord in range(self.dimensions)],
        #                               data="this is some data")
        #     new_entry_id = self.database.create(new_entry)
        #     trunk_node = self.tree_handler.get_node(self.trunk_id)
        #     if trunk_node is None:
        #         raise Exception("Trunk node not found")
        #
        #     trunk_node.insert_entry_from_entry(new_entry_id, new_entry)
        #
        #     self.tree_handler.update_node(self.trunk_id, trunk_node)

        # cache object (cache.py)
        self.cache = Cache(node_size=self.children_per_node, cache_nodes=CACHE_NODES)

    def __del__(self):
        pass

    # search for node in tree based on coordinates
    def search_node(self, coordinates) \
            -> Tuple[DatabaseEntry, int, int]:  # -> Node:  # maybe allow to return list of NOdes
        # todo visited nodes counter. Same for other searches
        pass

    def search_rectangle(self, rectangle) -> List[DatabaseEntry]:
        pass

    def search_nearest_k_neighbours(self, database_entry: DatabaseEntry, k: int) -> List[DatabaseEntry]:
        pass

    # gets node directly from file, based on id
    def __get_node(self, node_id: int) -> Optional[RTreeNode]:
        cached_node = self.cache.search(node_id)
        if cached_node is not None:
            return cached_node

        node = self.tree_handler.get_node(node_id)
        if node is None:
            raise Exception(f"Node {node_id} not found in tree file")
        self.cache.store(node)
        return node

    # maybe change from *args to list, might be more memory efficient, idk
    def insert_entry(self, *entries: List[DatabaseEntry]):
        # for entry in entries:
        pass

    def __too_many_deleted_entries(self):
        return (self.node_size ** self.depth) / 2 < self.deleted_db_entries_counter

    def __delete_entry(self, entry: DatabaseEntry):
        if self.__too_many_deleted_entries:
            # todo shake tree
            pass
        # todo delete (maybe before shake)
        self.deleted_db_entries_counter += 1

    def delete_entries(self, *entries: DatabaseEntry):
        for entry in entries:
            self.__delete_entry(entry)

    def linear_search(self, parameters):
        pass

    def rebuild(self):
        pass

    def get_all_nodes(self):
        """Returns all RTree nodes. Do not call on larger rtrees."""
        # Maybe use self.search_rectangle(big rectangle)
        # or use self.search_nearest_k_neighbours(random entry, number of nodes / giant number)
        pass
