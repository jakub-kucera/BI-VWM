import errno
import secrets
import sys
from typing import List, Optional, Tuple
import os
from hashlib import sha1
from psutil import cpu_count

from rtree.data.mbb import MBB
from rtree.default_config import *
from rtree.data.database import Database
from rtree.data.database_entry import DatabaseEntry
from rtree.data.rtree_node import RTreeNode
from rtree.cache import Cache
from rtree.data.tree_file_handler import TreeFileHandler


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

        # id of root node
        self.root_id = 0

        self.depth = 0  # todo delete

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
                                            parameters_size=self.parameters_size, root_id=self.root_id,
                                            unique_sequence=self.unique_sequence, config_hash=self.config_hash)

        # todo delete, only use from RTreeNode
        self.children_per_node = self.tree_handler.children_per_node

        if load_from_files:
            # override attributes by values from tree_handler and database
            # print("LOADING FROM FILES")
            self.config_hash = self.tree_handler.config_hash
            self.sequence = self.tree_handler.unique_sequence
            self.unique_sequence = self.sequence
            self.node_size = self.tree_handler.node_size
            self.dimensions = self.tree_handler.dimensions
            self.id_size = self.tree_handler.id_size
            self.root_id = self.tree_handler.root_id
            self.tree_depth = self.tree_handler.tree_depth
            self.parameters_size = self.tree_handler.parameters_size
        else:
            root_node_new = RTreeNode.create_empty_node(self.dimensions, is_leaf=True, parent_id=0)
            self.root_id = self.tree_handler.create_node(root_node_new)
            if self.root_id != 0:
                raise Exception("Root id in new file is not 0")
            # root_node_new.parent_id = self.root_id
            # root_id_check = self.tree_handler.update_node(self.root_id, root_node_new)
            # if root_id_check != self.root_id:
            #     raise Exception("Incorrect root id check")

        # creates database file handler
        self.database = Database(filename=self.database_filename, dimensions=self.dimensions,
                                 parameters_size=self.parameters_size,
                                 unique_sequence=self.unique_sequence, config_hash=self.config_hash)

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
    def get_node(self, node_id: int) -> Optional[RTreeNode]:
        cached_node = self.cache.search(node_id)
        if cached_node is not None:
            return cached_node

        node = self.tree_handler.get_node(node_id)
        if node is None:
            raise Exception(f"Node {node_id} not found in tree file")
        self.cache.store(node)
        return node

    def rec_search(self, entry_mmb: MBB, node: RTreeNode) -> int:

        if node.is_leaf:
            if node.id is None:
                raise Exception("Node id cannot be None")
            return node.id

        minimum_size_node: Optional[RTreeNode] = None
        minimum_size_value: Optional[int] = maxsize

        for child_id in node.child_nodes:
            child = self.get_node(child_id)
            if child is None:
                raise Exception("Node cannot be None")
            if child.mbb.contains_inner(entry_mmb):
                size = child.mbb.size()
                if size < minimum_size_value:
                    minimum_size_value = size
                    minimum_size_node = child

        if minimum_size_node is not None:
            return self.rec_search(entry_mmb, minimum_size_node)

        minimum_expansion_node: Optional[RTreeNode] = None
        minimum_expansion_value: Optional[int] = maxsize
        for child_id in node.child_nodes:
            child = self.get_node(child_id)
            if child is None:
                raise Exception("Child node cannot be None")

            expansion = child.mbb.size_increase_insert(entry_mmb.box)
            if expansion < minimum_expansion_value:
                minimum_expansion_value = expansion
                minimum_expansion_node = child

        if minimum_expansion_node is None:
            raise Exception("RTree insert error, minimum_expansion_node is None")

        return self.rec_search(entry_mmb, minimum_expansion_node)

        # splitting minimum fill, (best 30-40%)
        # prevent most inserted into only one new mbb

        # return status_code, ok, dont use, full make new node
        # if returned full, and node_id == root_id, increase tree_depth

    def execute_split(self, node: RTreeNode, parent_node: RTreeNode):
        seed_node_1, seed_node_2 = node.get_seed_split_nodes()

        if node is None:
            raise Exception("Node cannot be None")
        if node.id is None:
            raise Exception("Node id cannot be None")

        for child_node_id in node.child_nodes:
            child_node = self.get_node(child_node_id)

            if child_node is None:
                raise Exception("Child node cannot be None")
            if child_node.id is None:
                raise Exception("Child node id cannot be None")

            seed_1_increase = seed_node_1.mbb.size_increase_insert(child_node.mbb.box)
            seed_2_increase = seed_node_2.mbb.size_increase_insert(child_node.mbb.box)

            if seed_1_increase > seed_2_increase or seed_node_1.has_over_balance():
                seed_node_2.mbb.insert_mbb(child_node.mbb.box)

            elif seed_2_increase > seed_1_increase or seed_node_2.has_over_balance():
                seed_node_1.mbb.insert_mbb(child_node.mbb.box)

            elif seed_node_2.mbb.size > seed_node_1.mbb.size:
                seed_node_1.mbb.insert_mbb(child_node.mbb.box)
            else:
                seed_node_2.mbb.insert_mbb(child_node.mbb.box)

            # save seed_node_1 into node
        self.tree_handler.update_node(node.id, seed_node_1)

        # create seed_node_2 as new
        seed_node_2_id = self.tree_handler.create_node(seed_node_2)

        # update parent ids of child nodes of seed_node_2
        for child_node_id in seed_node_2.child_nodes:
            child_node = self.get_node(child_node_id)

            if child_node is None:
                raise Exception("Child node cannot be None")
            if child_node.parent_id is None:
                raise Exception("Child node parent_id cannot be None")

            child_node.parent_id = seed_node_2_id
            self.tree_handler.update_node(seed_node_2_id, seed_node_2)

        # update parent_node
        parent_node.child_nodes.append(seed_node_2_id)
        parent_node.mbb.insert_mbb(seed_node_2.mbb.box)

        if parent_node is None:
            raise Exception("Parent node cannot be None")
        if parent_node.parent_id is None:
            raise Exception("Parent node parent_id cannot be None")

        # save parent_node
        self.tree_handler.update_node(parent_node.parent_id, parent_node)

    def rec_split_node(self, node_id: int):
        node = self.get_node(node_id)
        if node is None:
            raise Exception("split node cannot be None")
        if node.parent_id is None:
            raise Exception("split node parent id cannot be None")

        parent_node = self.get_node(node.parent_id)
        if parent_node is None:
            raise Exception("Parent node cannot be None")
        if parent_node.id is None:
            raise Exception("Parent node id cannot be None")

        if parent_node.is_full():
            if parent_node.id == self.root_id:
                new_root = RTreeNode.create_empty_node(self.dimensions, is_leaf=parent_node.is_leaf)
                new_root.mbb.insert_mbb(parent_node.mbb.box)
                new_root.insert_node_from_node(parent_node.id, parent_node)

                new_root_id = self.tree_handler.create_node(new_root)
                self.root_id = new_root_id
                # child_node = self.get_node(child_node_id)

                # self.tree_handler.update_node(seed_node_2_id, seed_node_2)

                self.execute_split(parent_node, new_root)  # (old_root,new_root) old_root is overfilled, create new_root
                pass

            self.rec_split_node(parent_node.id)
        else:
            self.execute_split(node, parent_node)  # node is overfilled, split into two nodes and append to parent_node

    # jakýkoliv leaf řekne že je zaplněný
    # toto zaplnění se propaguje výše dokud nenarazí na nezaplněné (až do rootu)
    # > v nezaplněné přibude node a entries se rozloží
    # > pokud je zaplněný root tak přidáváme hladinu (zanecháme a přidáme nový root_node)

    # def insert_entry(self, entries: List[DatabaseEntry]):
    def insert_entry(self, entry: DatabaseEntry):

        pass

    def __too_many_deleted_entries(self):
        return (self.node_size ** self.depth) / 2 < self.deleted_db_entries_counter

    def __delete_entry(self, entry: DatabaseEntry):
        # if node has no child nodes after deleting also delete node

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
