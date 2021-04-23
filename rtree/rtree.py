import errno
import secrets
from typing import List, Optional, Tuple
import os
from hashlib import sha1
from psutil import cpu_count  # todo <- fix import

from rtree.default_config import *
from rtree.database import Database
from rtree.database_entry import DatabaseEntry
from rtree.node import Node
from rtree.tree_file_handler import TreeFileHandler

# maybe rename to Index, use rtree only as module name


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

    def __init__(self, tree_file: str = DEFAULT_TREE_FILE,
                 database_file: str = DEFAULT_DATABASE_FILE,
                 override_file: bool = False,
                 dimensions: int = DEFAULT_DIMENSIONS,
                 parameters_size: int = PARAMETER_RECORD_SIZE,
                 id_size: int = NODE_ID_SIZE,
                 node_size: int = DEFAULT_NODE_SIZE,
                 max_threads: int = None):

        # path to binary file with saved tree / already opened file object
        self.tree_file = WORKING_DIRECTORY + tree_file
        self.database_file = WORKING_DIRECTORY + database_file

        # checks if files exists and are valid.
        load_from_files = self.check_files_load_existing_rtree(tree_file=self.tree_file,
                                                               database_file=self.database_file, override=override_file)

        # number of parameters used to index entries
        self.dimensions = dimensions

        # size of one parameter in bytes
        self.parameters_size = parameters_size

        # node id size in file
        self.id_size = id_size

        # size of memory in Bytes to store one tree node
        self.node_size = node_size

        self.trunk_id = 0

        # tree depth (start from 0 or 1?)
        self.depth = 0

        # randomly generated sequence of bytes
        unique_sequence = secrets.token_bytes(UNIQUE_SEQUENCE_LENGTH)

        # Generate an hash based on RTree parameters
        self.config_hash = self.calculate_config_hash(
            [self.dimensions, self.node_size, self.id_size, self.parameters_size])
        print(self.config_hash)

        # Number of threads that can be used
        self.max_threads = cpu_count() if max_threads is None else max_threads

        # object that directly interacts with a file where the rtree is stored
        self.tree_handler = TreeFileHandler(filename=self.tree_file, dimensions=self.dimensions,
                                            node_size=self.node_size, id_size=self.id_size, tree_depth=self.depth,
                                            parameters_size=self.parameters_size, trunk_id=self.trunk_id,
                                            unique_sequence=unique_sequence, config_hash=self.config_hash)

        self.children_per_node = self.tree_handler.children_per_node

        # database object (database.py)
        self.database = Database()

        # todo when loading from file, can be
        if load_from_files:
            # override attributes by values from tree_handler and database
            pass
        else:
            # todo add first empty node
            pass

    def __del__(self):
        pass

    # search for node in tree based on coordinates
    def search_node(self, coordinates) -> DatabaseEntry:  # -> Node:  # maybe allow to return list of NOdes
        pass

    def search_rectangle(self, rectangle) -> List[DatabaseEntry]:
        pass

    def search_nearest_k_neighbours(self, database_entry: DatabaseEntry, k: int) -> List[DatabaseEntry]:
        pass

    # gets node directly from file, based on id
    def __get_node(self, node_id: int) -> Optional[Node]:
        return self.tree_handler.get_node(node_id)

    # maybe change from *args to list, might be more memory efficient, idk
    def insert_entry(self, *entries: List[DatabaseEntry]):
        # for entry in entries:
        pass

    def delete_entry(self, *entries: List[DatabaseEntry]):
        # for entry in entries:
        pass

    def linear_search(self, parameters):
        pass

    def __delete_node(self):
        pass

    def rebuild(self):
        pass

    def get_all_nodes(self):
        """Returns all RTree nodes. Do not call on larger rtrees."""
        # Maybe use self.search_rectangle(big rectangle)
        # or use self.search_nearest_k_neighbours(random entry, number of nodes / giant number)
        pass
