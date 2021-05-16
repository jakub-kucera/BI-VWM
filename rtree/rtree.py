import errno
import secrets
import sys
from typing import List, Optional, Tuple
import os
import math
from hashlib import sha1
from psutil import cpu_count
from sys import maxsize

from rtree.data.mbb import MBB
from rtree.data.mbb_dim import MBBDim
from rtree.default_config import *
from rtree.data.database import Database
from rtree.data.database_entry import DatabaseEntry
from rtree.data.rtree_node import RTreeNode
from rtree.data.cache import Cache
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
                raise Exception(f"Root id in new file is {self.root_id}, but should be 0")

        # creates database file handler
        self.database = Database(filename=self.database_filename, dimensions=self.dimensions,
                                 parameters_size=self.parameters_size,
                                 unique_sequence=self.unique_sequence, config_hash=self.config_hash)

        # cache object (cache.py)
        self.cache = Cache(node_size=DEFAULT_NODE_SIZE, child_size=self.tree_handler.children_per_node, cache_memory=CACHE_MEMORY_SIZE)

    def __del__(self):
        pass

    def __rec_search_entry(self, coordinates: MBB, node: RTreeNode, permanent_cache: bool = False) -> Optional[Tuple[DatabaseEntry, int, int]]:
        """Return entry, entry_position and id for node containing entry"""
        if node.id is None:
            raise Exception("node.id cannot be None")

        if node.is_leaf:
            for entry_position in node.child_nodes:
                entry = self.database.search(entry_position)
                if coordinates.contains_inner(entry.get_mbb()):
                    return entry, entry_position, node.id
        else:
            for child_id in node.child_nodes:
                child_node = self.__get_node_fastread(child_id, permanent_cache)
                if child_node is None:
                    raise Exception("Child node cannot be None")
                if child_node.mbb.contains_inner(coordinates):
                    rec_search = self.__rec_search_entry(coordinates, child_node, permanent_cache=False)
                    if rec_search is not None:
                        return rec_search
        return None

    def __search_entry_and_position(self, coordinates: List[int]) -> Optional[Tuple[DatabaseEntry, int, int]]:
        # todo visited nodes counter. Same for other searches
        check_mbb = MBB.create_box_from_entry_list(coordinates)
        root_node = self.__get_node_fastread(self.root_id, permanent_cache=True)
        if root_node is None:
            raise Exception("Root node cannot be None")

        # recursively check all children from root down for matching coordinates
        return self.__rec_search_entry(check_mbb, root_node, permanent_cache=True)

    # look for entry at specific point
    def search_entry(self, coordinates: List[int]) -> Optional[DatabaseEntry]:
        if len(coordinates) != self.dimensions:
            raise Exception("coordinates have incorrect number of dimensions")

        entry = self.__search_entry_and_position(coordinates)
        if entry is None:
            return None

        return entry[0]

    def search_entry_position_parent_position(self, coordinates: List[int]) -> Optional[Tuple[int, int]]:
        entry = self.__search_entry_and_position(coordinates)
        if entry is None:
            return None

        return entry[1], entry[2]

    def update_root_id(self, root_id: int):
        self.root_id = root_id
        self.tree_handler.update_root_id(root_id)

    def __rec_search_area(self, coordinates: MBB, node: RTreeNode, carry: List[DatabaseEntry], permanent_cache: bool = False):
        if node.is_leaf:
            for entry_position in node.child_nodes:
                entry = self.database.search(entry_position)
                if coordinates.contains_inner(entry.get_mbb()):
                    carry.append(entry)
        else:
            for child in node.child_nodes:
                child_node = self.__get_node_fastread(child, permanent_cache)
                if child_node is None:
                    raise Exception("Child node cannot be None")
                if child_node.mbb.overlaps(coordinates):
                    self.__rec_search_area(coordinates, child_node, carry, permanent_cache=False)

    # area defined by two points in N dimensions
    def search_area(self, coordinates_min: List[int], coordinates_max: List[int]) -> List[DatabaseEntry]:
        if len(coordinates_min) != len(coordinates_max) != self.dimensions:
            raise Exception("coordinates have incorrect number of dimensions")

        check_mbb = MBB.create_box_from_entry_list(coordinates_min)
        max_mbb = MBB.create_box_from_entry_list(coordinates_max)
        check_mbb.insert_mbb(max_mbb.box)

        root_node = self.__get_node_fastread(self.root_id, permanent_cache=True)
        if root_node is None:
            raise Exception("Root node cannot be None")

        # recursively check all children from root down for matching coordinates
        matching: List[DatabaseEntry] = []
        self.__rec_search_area(check_mbb, root_node, matching, permanent_cache=True)
        return matching

    # find k entries closest to given point
    def search_knn(self, k: int, coordinates: List[int]) -> List[DatabaseEntry]:

        if len(coordinates) != self.dimensions:
            raise Exception("coordinates have incorrect number of dimensions")

        root_node = self.__get_node_fastread(self.root_id, permanent_cache=True)
        if root_node is None:
            raise Exception("Root node cannot be None")

        # at every step, increase the searchbox one percent of root area
        size_increments = []
        for dim in root_node.mbb.box:
            size_increments.append(math.ceil(dim.get_diff() * 0.01))

        search_mbb = MBB.create_box_from_entry_list(coordinates)

        while True:
            # increase
            new_box: List[MBBDim] = []
            for increment, dimension in zip(size_increments, search_mbb.box):
                new_low = dimension.low - increment
                new_high = dimension.high + increment
                new_box.append(MBBDim(new_low, new_high))
            search_mbb.box = tuple(new_box)

            # search
            nearest_neighbors: List[DatabaseEntry] = []
            self.__rec_search_area(search_mbb, root_node, nearest_neighbors, permanent_cache=True)

            # is it k or more?
            if len(nearest_neighbors) == k:
                return nearest_neighbors

            if len(nearest_neighbors) > k:
                nearest_neighbors.sort(key=lambda x: x.distance_from(coordinates))
                return nearest_neighbors[0:k]

            if search_mbb.contains_inner(root_node.mbb):
                return nearest_neighbors

    # gets node directly from file, based on id
    def __get_node(self, node_id: int) -> Optional[RTreeNode]:
        node = self.tree_handler.get_node(node_id)
        if node is None:
            raise Exception(f"Node {node_id} not found in tree file")
        return node

    # gets node from cached memory
    def __get_node_fastread(self, node_id: int, permanent_cache: bool = False) -> Optional[RTreeNode]:
        cached_node = self.cache.search(node_id, permanent_cache)
        if cached_node is not None:
           return cached_node

        node = self.tree_handler.get_node(node_id)
        if node is None:
            raise Exception(f"Node {node_id} not found in tree file")

        self.cache.store(node, permanent_cache)
        return node

    def rec_search(self, entry_mmb: MBB, node: RTreeNode) -> int:
        if node.is_leaf:
            if node.id is None:
                raise Exception("Node id cannot be None")
            return node.id

        minimum_size_node: Optional[RTreeNode] = None
        minimum_size_value: Optional[int] = maxsize

        for child_id in node.child_nodes:
            child = self.__get_node(child_id)
            if child is None:
                raise Exception("Node cannot be None")
            if child.mbb.contains_inner(entry_mmb):
                size = child.mbb.size
                if size < minimum_size_value:
                    minimum_size_value = size
                    minimum_size_node = child

        if minimum_size_node is not None:
            return self.rec_search(entry_mmb, minimum_size_node)

        minimum_expansion_node: Optional[RTreeNode] = None
        minimum_expansion_value: Optional[int] = maxsize
        for child_id in node.child_nodes:
            child = self.__get_node(child_id)
            if child is None:
                raise Exception("Child node cannot be None")

            expansion = child.mbb.size_increase_insert(entry_mmb.box)
            if expansion < minimum_expansion_value:
                minimum_expansion_value = expansion
                minimum_expansion_node = child

        if minimum_expansion_node is None:
            raise Exception("RTree insert error, minimum_expansion_node is None")

        return self.rec_search(entry_mmb, minimum_expansion_node)

    def execute_split(self, node: RTreeNode):
        seed_node_1, seed_node_2 = node.get_seed_split_nodes()

        if node is None:
            raise Exception("Node cannot be None")
        if node.id is None:
            raise Exception("Node id cannot be None")

        for child_node_id in node.child_nodes:
            child_mbb_box: Tuple[MBBDim, ...] = ()

            if node.is_leaf:
                database_entry = self.database.search(child_node_id)

                if database_entry is None:
                    raise Exception("Child node [of leaf, actually DBEntry] cannot be None")

                child_mbb_box = database_entry.get_mbb().box

            else:
                child_node = self.__get_node(child_node_id)

                if child_node is None:
                    raise Exception("Child node cannot be None")
                if child_node.id is None:
                    raise Exception("Child node id cannot be None")

                child_mbb_box = child_node.mbb.box

            seed_1_increase = seed_node_1.mbb.size_increase_insert(child_mbb_box)
            seed_2_increase = seed_node_2.mbb.size_increase_insert(child_mbb_box)

            if seed_node_1.has_over_balance():
                seed_node_2.insert_box(child_node_id, child_mbb_box)

            elif seed_node_2.has_over_balance():
                seed_node_1.insert_box(child_node_id, child_mbb_box)

            elif seed_1_increase > seed_2_increase:
                seed_node_2.insert_box(child_node_id, child_mbb_box)

            elif seed_2_increase > seed_1_increase:
                seed_node_1.insert_box(child_node_id, child_mbb_box)

            elif seed_node_2.mbb.size > seed_node_1.mbb.size:
                seed_node_1.insert_box(child_node_id, child_mbb_box)
            else:
                seed_node_2.insert_box(child_node_id, child_mbb_box)

        return seed_node_1, seed_node_2

    def update_parent_reference(self, parent_node: RTreeNode):
        if not parent_node.is_leaf:
            for child_id in parent_node.child_nodes:
                child_node = self.__get_node(child_id)
                if child_node is None:
                    raise Exception("Child Node not found")
                child_node.parent_id = parent_node.id
                self.tree_handler.update_node(child_id, child_node)
                self.cache.store(child_node, child_node.parent_id == self.root_id)

    def handle_full_node(self, desired_node: RTreeNode, new_id: int, new_box: Tuple[MBBDim, ...]):
        desired_node.insert_box(new_id, new_box)  # insert into object, not file
        split_node_1, split_node_2 = self.execute_split(desired_node)

        if desired_node.parent_id is None:
            raise Exception("desired_node parent id cannot be none")
        if desired_node.id is None:
            raise Exception("desired_node id cannot be none")

        parent_node = self.__get_node(desired_node.parent_id)

        if parent_node is None:
            raise Exception("parent_node cannot be none")
        if parent_node.id is None:
            raise Exception("parent_node id cannot be none")

        if parent_node.is_full():
            if parent_node.id == desired_node.id == self.root_id:

                new_root = RTreeNode.create_empty_node(self.dimensions, is_leaf=False)
                new_root.parent_id = -1

                smaller_split_node, bigger_split_node = sorted([split_node_1, split_node_2],
                                                               key=lambda x: len(x.child_nodes))

                # save the splits
                smaller_split_node.id = self.tree_handler.create_node(smaller_split_node)
                bigger_split_node.id = self.tree_handler.update_node(desired_node.id, bigger_split_node)

                # change parent
                self.update_parent_reference(smaller_split_node)

                new_root.insert_box(smaller_split_node.id, smaller_split_node.mbb.box)
                new_root.insert_box(bigger_split_node.id, bigger_split_node.mbb.box)

                new_root_id = self.tree_handler.create_node(new_root)
                self.update_root_id(new_root_id)
                new_root.parent_id = self.root_id
                self.tree_handler.update_node(self.root_id, new_root)
                new_root.id = new_root_id
                self.cache.store(new_root, permanent=True)

                smaller_split_node.parent_id = self.root_id
                bigger_split_node.parent_id = self.root_id
                self.tree_handler.update_node(smaller_split_node.id, smaller_split_node)
                self.cache.store(smaller_split_node, permanent=True)
                self.tree_handler.update_node(bigger_split_node.id, bigger_split_node)
                self.cache.store(bigger_split_node, permanent=True)

                self.propagate_stretch(smaller_split_node)
                self.propagate_stretch(bigger_split_node)

            else:
                # parent node is full, but it is not a root node
                smaller_split_node, bigger_split_node = sorted([split_node_1, split_node_2],
                                                               key=lambda x: len(x.child_nodes))

                # save the splits
                smaller_split_node.id = self.tree_handler.create_node(smaller_split_node)
                bigger_split_node.id = self.tree_handler.update_node(desired_node.id, bigger_split_node)

                self.update_parent_reference(smaller_split_node)
                self.cache.store(smaller_split_node, smaller_split_node.parent_id == self.root_id)
                self.cache.store(bigger_split_node, bigger_split_node.parent_id == self.root_id)

                # parent_node.insert_box(smaller_split_node.id, smaller_split_node.mbb.box)
                parent_node.insert_box(bigger_split_node.id, bigger_split_node.mbb.box)
                self.tree_handler.update_node(parent_node.id, parent_node)
                self.cache.store(parent_node, parent_node.parent_id == self.root_id)

                # if their parent is full, split it too -> recursively
                self.handle_full_node(parent_node, smaller_split_node.id, smaller_split_node.mbb.box)

        else:  # desired is full and split, parent is not full, save splits into parent

            smaller_split_node, bigger_split_node = sorted([split_node_1, split_node_2],
                                                           key=lambda x: len(x.child_nodes))

            smaller_split_node.id = self.tree_handler.create_node(smaller_split_node)
            bigger_split_node.id = self.tree_handler.update_node(desired_node.id, bigger_split_node)

            self.update_parent_reference(smaller_split_node)

            parent_node.insert_box(bigger_split_node.id, bigger_split_node.mbb.box)
            parent_node.insert_box(smaller_split_node.id, smaller_split_node.mbb.box)

            self.tree_handler.update_node(parent_node.id, parent_node)
            self.cache.store(parent_node, parent_node.parent_id == self.root_id)

            self.propagate_stretch(smaller_split_node)
            self.propagate_stretch(bigger_split_node)

    def insert_entry(self, new_entry: DatabaseEntry, given_position: int = -1):
        if given_position == -1:
            new_entry_position = self.database.create(new_entry)
        else:
            new_entry_position = given_position

        root_node = self.__get_node(self.root_id)
        if root_node is None:
            raise Exception("root node cannot be None")

        desired_node_id = self.rec_search(new_entry.get_mbb(), root_node)
        desired_node = self.__get_node(desired_node_id)

        if desired_node is None:
            raise Exception("desired_node cannot be None")
        if desired_node.parent_id is None:
            raise Exception("desired_node parent_id cannot be None")

        if desired_node.is_full():
            self.handle_full_node(desired_node, new_entry_position, new_entry.get_mbb().box)
            self.cache.store(desired_node, desired_node.parent_id == self.root_id)
            self.propagate_stretch(desired_node)
        else:
            desired_node.insert_box(new_entry_position, new_entry.get_mbb().box)
            self.tree_handler.update_node(desired_node_id, desired_node)
            self.cache.store(desired_node, desired_node.parent_id == self.root_id)
            self.propagate_stretch(desired_node)

    def propagate_stretch(self, node: RTreeNode):
        if node.parent_id is None:
            raise Exception("parent ID cannot be none")

        parent_node = self.__get_node(node.parent_id)
        if parent_node is None or parent_node.id is None:
            raise Exception("Parent_node cannot be none")

        if not parent_node.contains_inner(node):

            if len(parent_node.child_nodes) == 0:
                raise Exception("Parent_node cannot have 0 children")

            contained_id = parent_node.child_nodes[0]
            parent_node.insert_box(contained_id, node.mbb.box)
            self.tree_handler.update_node(parent_node.id, parent_node)
            self.cache.store(parent_node, parent_node.parent_id == self.root_id)
            self.propagate_stretch(parent_node)

    def __too_many_deleted_entries(self):
        return (self.node_size ** self.depth) / 2 < self.deleted_db_entries_counter

    def delete_entry(self, coordinates: List[int]) -> bool:  # todo change to List[int]
        # if node has no child nodes after deleting also delete node
        if self.__too_many_deleted_entries:
            # todo shake tree
            pass

        response = self.search_entry_position_parent_position(coordinates)
        if response is None:
            return False
        entry_position, node_id = response

        node = self.__get_node(node_id)
        if node is None:
            raise Exception("Node cannot be None")

        if entry_position not in node.child_nodes:
            raise Exception("Entry position must be in its parent node")

        node.child_nodes.remove(entry_position)
        self.tree_handler.update_node(node_id, node)
        self.cache.store(node, node.parent_id == self.root_id)

        self.database.mark_to_delete(byte_position=entry_position)

        self.deleted_db_entries_counter += 1
        return True

    def __rec_rebuild(self, node: RTreeNode, carry: List[int], permanent_cache: bool = False):
        if node.is_leaf:
            for entry_position in node.child_nodes:
                carry.append(entry_position)
        else:
            for child in node.child_nodes:
                child_node = self.__get_node_fastread(child, permanent_cache)
                if child_node is None:
                    raise Exception("Child node cannot be None")
                self.__rec_rebuild(child_node, carry, permanent_cache=False)

    def rebuild(self):
        root_node = self.__get_node_fastread(self.root_id, True)
        if root_node is None:
            raise Exception("Root node cannot be None")

        all_positions: List[int] = []
        self.__rec_rebuild(root_node, all_positions, True)
        print(f"helpa: {len(all_positions)} : {all_positions}")

        # TODO remove old tree and create a new one

        # for entry_position in all_positions:
        #     entry = self.database.search(entry_position)
        #     self.insert_entry(entry, entry_position)

    def rec_get_all_nodes(self, node: RTreeNode, depth: int) -> List[Tuple[RTreeNode, int]]:

        if node.is_leaf:
            return [(node, depth)]

        nodes_list: List[Tuple[RTreeNode, int]] = []
        nodes_list.append((node, depth))

        depth += 1
        for child_id in node.child_nodes:
            child_node = self.__get_node(child_id)
            if child_node is None:
                raise Exception("Node cannot be None")

            nodes_list.extend(self.rec_get_all_nodes(child_node, depth))

        return nodes_list

    def get_all_nodes(self) -> List[Tuple[RTreeNode, int]]:
        """Returns all RTree nodes. Do not call on larger rtrees."""
        root_node = self.__get_node(self.root_id)
        if root_node is None:
            raise Exception("Root node cannot be None")

        return self.rec_get_all_nodes(root_node, 0)
