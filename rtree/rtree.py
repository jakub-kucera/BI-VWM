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
                raise Exception(f"Root id in new file is {self.root_id}, but should be 0")
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

    def __rec_search_node(self, coordinates: MBB, node: RTreeNode) -> Optional[DatabaseEntry]:
        if node.is_leaf:
            for entry_position in node.child_nodes:
                entry = self.database.search(entry_position)
                if coordinates.contains_inner(entry.get_mbb()):
                    return entry
        else:
            for child_id in node.child_nodes:
                child_node = self.__get_node(child_id)
                if child_node is None:
                    raise Exception("Child node cannot be None")
                if child_node.mbb.contains_inner(coordinates):
                    return self.__rec_search_node(coordinates, child_node)
        return None

    # single point in N dimensions
    def search_node(self, coordinates: List[int]) -> Optional[DatabaseEntry]:
        # todo visited nodes counter. Same for other searches
        check_mbb = MBB.create_box_from_entry_list(coordinates)
        root_node = self.__get_node(self.root_id)
        if root_node is None:
            raise Exception("Root node cannot be None")

        # recursively check all children from root down for matching coordinates
        return self.__rec_search_node(check_mbb, root_node)

    def __rec_search_rectangle(self, coordinates: MBB, node: RTreeNode, carry: List[DatabaseEntry]):
        if node.is_leaf:
            for entry_position in node.child_nodes:
                entry = self.database.search(entry_position)
                if coordinates.contains_inner(entry.get_mbb()):
                    carry.append(entry)
        else:
            for child in node.child_nodes:
                child_node = self.__get_node(child)
                if child_node is None:
                    raise Exception("Child node cannot be None")
                if child_node.mbb.contains_inner(coordinates):
                    self.__rec_search_rectangle(coordinates, child_node, carry)

    # area defined by two points in N dimensions
    def search_rectangle(self, coordinates_min: List[int], coordinates_max: List[int]) -> List[DatabaseEntry]:
        check_mbb = MBB.create_box_from_entry_list(coordinates_min)
        max_mbb = MBB.create_box_from_entry_list(coordinates_max)
        check_mbb.insert_mbb(max_mbb.box)

        root_node = self.__get_node(self.root_id)
        if root_node is None:
            raise Exception("Root node cannot be None")

        # recursively check all children from root down for matching coordinates
        matching: List[DatabaseEntry] = []
        self.__rec_search_rectangle(check_mbb, root_node, matching)
        return matching

    def search_k_nearest_neighbours(self, k: int, coordinates: List[int]) -> List[DatabaseEntry]:
        # if k > all DatabaseEntries: raise Exception
        #
        # search for matching leaf_node
        # sort all child_nodes (DatabaseEntries) and select k of them
        #   if more than k is available, goto parent and select closest leaf_nodes
        #   sort all closely surrounding DatabaseEntries to select the rest to k
        #
        # math.ceil([absolutní velikost jedné dimenze] * 0.01)

        root_node = self.__get_node(self.root_id)
        if root_node is None:
            raise Exception("Root node cannot be None")

        size_increments = []
        for dim in root_node.mbb.box:
            size_increments.append(math.ceil(dim.get_diff() * 0.01))

        search_mbb = MBB.create_box_from_entry_list(coordinates)

        # if not root_node.contains_inner(search_box.mmb)

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
            self.__rec_search_rectangle(search_mbb, root_node, nearest_neighbors)

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
        # cached_node = self.cache.search(node_id)
        # if cached_node is not None:
        #    return cached_node

        node = self.tree_handler.get_node(node_id)
        if node is None:
            raise Exception(f"Node {node_id} not found in tree file")
        # self.cache.store(node)
        return node

    def rec_search(self, entry_mmb: MBB, node: RTreeNode) -> int:
        # print(f"rec search node_id={node.id}, is_leaf: {node.is_leaf} with children {len(node.child_nodes)}:", node.child_nodes)

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
                # print("calling mbb")
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

        # splitting minimum fill, (best 30-40%)
        # prevent most inserted into only one new mbb

        # return status_code, ok, dont use, full make new node
        # if returned full, and node_id == root_id, increase tree_depth

    def execute_split(self, node: RTreeNode):
        # print("execute split")
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
                # print("** insert 2")
                seed_node_2.insert_box(child_node_id, child_mbb_box)

            elif seed_node_2.has_over_balance():
                # print("* insert 1")
                seed_node_1.insert_box(child_node_id, child_mbb_box)

            elif seed_1_increase > seed_2_increase:
                # print("++ insert 2")
                seed_node_2.insert_box(child_node_id, child_mbb_box)

            elif seed_2_increase > seed_1_increase:
                # print("+ insert 1")
                seed_node_1.insert_box(child_node_id, child_mbb_box)

            elif seed_node_2.mbb.size > seed_node_1.mbb.size:
                # print("- insert 1")
                seed_node_1.insert_box(child_node_id, child_mbb_box)
            else:
                # print("-- insert 2")
                seed_node_2.insert_box(child_node_id, child_mbb_box)

        return seed_node_1, seed_node_2

    def update_parent_reference(self, parent_node: RTreeNode):
        # print("update")
        # return
        if not parent_node.is_leaf:
            for child_id in parent_node.child_nodes:
                child_node = self.__get_node(child_id)
                if child_node is None:
                    raise Exception("Child Node not found")
                # print("Updating child")
                child_node.parent_id = parent_node.id
                self.tree_handler.update_node(child_id, child_node)

    def handle_full_node(self, desired_node: RTreeNode, new_id: int, new_box: Tuple[MBBDim, ...]):
        # print("handle")
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
                self.root_id = new_root_id
                new_root.parent_id = self.root_id
                self.tree_handler.update_node(self.root_id, new_root)

                smaller_split_node.parent_id = self.root_id
                bigger_split_node.parent_id = self.root_id
                self.tree_handler.update_node(smaller_split_node.id, smaller_split_node)
                self.tree_handler.update_node(bigger_split_node.id, bigger_split_node)

                # print(smaller_split_node.is_leaf, "is leaf : smaller split (id ", smaller_split_node.id, ")", len(smaller_split_node.child_nodes), smaller_split_node.child_nodes)
                # print(bigger_split_node.is_leaf, "is leaf : bigger split (id ", bigger_split_node.id, ")", len(bigger_split_node.child_nodes), bigger_split_node.child_nodes)
                # print(new_root.is_leaf, "is leaf : new_root (id ", new_root_id, ")", len(new_root.child_nodes), new_root.child_nodes)
                # print("::::")

                self.propagate_stretch(smaller_split_node)
                self.propagate_stretch(bigger_split_node)

                # smaller_split_node = self.__get_node(smaller_split_node.id)
                # bigger_split_node = self.__get_node(bigger_split_node.id)
                # new_root = self.__get_node(new_root_id)

                # print(smaller_split_node.is_leaf, "is leaf : (parent: ", smaller_split_node.parent_id, ") smaller split", len(smaller_split_node.child_nodes), smaller_split_node.child_nodes)
                # print(bigger_split_node.is_leaf, "is leaf : (parent: ", bigger_split_node.parent_id, ") bigger split", len(bigger_split_node.child_nodes), bigger_split_node.child_nodes)
                # print(new_root.is_leaf, "is leaf : (parent: ", new_root.parent_id, ") new_root", len(new_root.child_nodes), new_root.child_nodes)
            else:
                # parent node is full, but it is not a root node
                smaller_split_node, bigger_split_node = sorted([split_node_1, split_node_2],
                                                               key=lambda x: len(x.child_nodes))

                # save the splits
                smaller_split_node.id = self.tree_handler.create_node(smaller_split_node)
                bigger_split_node.id = self.tree_handler.update_node(desired_node.id, bigger_split_node)

                self.update_parent_reference(smaller_split_node)

                # parent_node.insert_box(smaller_split_node.id, smaller_split_node.mbb.box)
                parent_node.insert_box(bigger_split_node.id, bigger_split_node.mbb.box)
                self.tree_handler.update_node(parent_node.id, parent_node)

                # if their parent is full, split it too -> recursively
                self.handle_full_node(parent_node, smaller_split_node.id, smaller_split_node.mbb.box)

                # raise Exception("parent node is full")
        else:  # desired is full and split, parent is not full, save splits into parent

            smaller_split_node, bigger_split_node = sorted([split_node_1, split_node_2],
                                                           key=lambda x: len(x.child_nodes))

            smaller_split_node.id = self.tree_handler.create_node(smaller_split_node)
            bigger_split_node.id = self.tree_handler.update_node(desired_node.id, bigger_split_node)

            self.update_parent_reference(smaller_split_node)

            parent_node.insert_box(bigger_split_node.id, bigger_split_node.mbb.box)
            parent_node.insert_box(smaller_split_node.id, smaller_split_node.mbb.box)

            self.tree_handler.update_node(parent_node.id, parent_node)

            root_node = self.__get_node(self.root_id)
            # print("root children", root_node.child_nodes)
            # print("small", smaller_split_node.id, "bigger", bigger_split_node.id, "parent", parent_node.id)

            self.propagate_stretch(smaller_split_node)
            self.propagate_stretch(bigger_split_node)

    def insert_entry(self, new_entry: DatabaseEntry):
        # print("insert =================================================================== <- ", new_entry.get_mbb())
        new_entry_position = self.database.create(new_entry)

        root_node = self.__get_node(self.root_id)
        if root_node is None:
            raise Exception("root node cannot be None")

        desired_node_id = self.rec_search(new_entry.get_mbb(), root_node)
        desired_node = self.__get_node(desired_node_id)

        if desired_node is None:
            raise Exception("desired_node cannot be None")

        if desired_node.is_full():

            self.handle_full_node(desired_node, new_entry_position, new_entry.get_mbb().box)
            root_node = self.__get_node(self.root_id)
            # print("root children", root_node.child_nodes)
            self.propagate_stretch(desired_node)

        else:
            desired_node.insert_box(new_entry_position, new_entry.get_mbb().box)
            self.tree_handler.update_node(desired_node_id, desired_node)
            # print("re-desired ", desired_node_id, ":", len(desired_node.child_nodes), desired_node.child_nodes)
            self.propagate_stretch(desired_node)

    def propagate_stretch(self, node: RTreeNode):
        # print("propagate")
        parent_node = self.__get_node(node.parent_id)
        # print("maybe stretch", parent_node.id, "until", parent_node.mbb, node.mbb, "    root node being", self.root_id, " with ", parent_node.child_nodes)
        if not parent_node.contains_inner(node):
            # print("do stretch")
            contained_id = parent_node.child_nodes[0]
            parent_node.insert_box(contained_id, node.mbb.box)
            # print(f"propagate {node.id} into parent {parent_node.id}")
            self.tree_handler.update_node(parent_node.id, parent_node)
            # print("after stretch", parent_node.id, "until", parent_node.mbb, node.mbb)
            self.propagate_stretch(parent_node)

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
