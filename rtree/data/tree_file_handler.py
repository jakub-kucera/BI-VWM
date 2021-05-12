import os
from typing import Optional

from rtree.data.rtree_node import RTreeNode, MBBDim, MBB
from rtree.default_config import *


class TreeFileHandler:
    def __init__(self,  # todo delete default values, always called from RTree with all args
                 filename: str = WORKING_DIRECTORY + DEFAULT_TREE_FILE,
                 dimensions: int = DEFAULT_DIMENSIONS,
                 node_size: int = DEFAULT_NODE_SIZE,
                 id_size: int = NODE_ID_SIZE,
                 parameters_size: int = PARAMETER_RECORD_SIZE,
                 tree_depth: int = 0,
                 root_id: int = 0,
                 unique_sequence: bytes = DEMO_UNIQUE_SEQUENCE,
                 config_hash: bytes = DEMO_CONFIG_HASH):
        # init default values, will be changed when loading from existing file
        self.filename = filename
        self.dimensions = dimensions
        self.node_size = node_size
        self.parameters_size = parameters_size
        self.id_size = id_size
        self.null_node_id = NULL_NODE_ID
        self.node_flag_size = NODE_FLAG_SIZE
        self.offset_size = 0
        self.highest_id = NULL_NODE_ID
        self.tree_depth = tree_depth
        self.root_id = root_id
        self.unique_sequence = unique_sequence
        self.config_hash = config_hash

        if len(self.unique_sequence) != UNIQUE_SEQUENCE_LENGTH:
            raise ValueError(f"Invalid unique sequence length: {len(self.unique_sequence)}")
        if len(self.config_hash) != CONFIG_HASH_LENGTH:
            raise ValueError(f"Invalid unique sequence length: {len(self.config_hash)}")

        # create file if not exists
        save_header_to_file = False
        if not os.path.isfile(self.filename):
            save_header_to_file = True
            with open(self.filename, 'w+b'):
                pass

        try:
            self.file = open(self.filename, 'r+b')
        except IOError:
            raise IOError(f"File cannot be opened: {self.filename}")

        # save/load config from file
        if save_header_to_file:
            self.write_header()
        else:
            self.read_header()

        # calculate remaining attributes
        self.children_per_node = int(
            (self.node_size - self.node_flag_size - self.id_size - (
                    self.dimensions * self.parameters_size * 2)) / self.id_size)
        self.node_padding = self.node_size - (
                self.node_flag_size + self.id_size + (self.dimensions * self.parameters_size * 2)
                + (self.children_per_node * self.id_size))

        # sets the maximum number of entries in the Node class
        RTreeNode.max_entries_count = self.children_per_node

        self.filesize = 0
        self.__update_file_size()

        highest_id_check = int((self.filesize - self.offset_size) / self.node_size) - 1

        if highest_id_check != self.highest_id:
            raise Exception(
                f"""Invalid TreeFileHandler config.)
                Saved highest_id = {self.highest_id},
                calculated highest_id: {highest_id_check}""")

        self.current_position = self.offset_size + (self.highest_id + 1) * self.id_size

        self.nodes_read_count = 0
        self.nodes_written_count = 0

    def __del__(self):
        if not self.file.closed:
            self.file.flush()
            self.write_header()
            self.file.close()

        if DELETE_TREE_INDEX_FILE:
            os.remove(self.filename)

    def __str__(self):
        return str(self.__dict__)

    def __update_file_size(self):
        self.filesize = os.path.getsize(self.filename)

    def read_header(self) -> int:
        header_size = 0
        self.file.seek(0, 0)

        # read random sequence and config hash
        header_attributes_bytes_sizes = (
            ('unique_sequence', UNIQUE_SEQUENCE_LENGTH),
            ('config_hash', CONFIG_HASH_LENGTH)
        )
        for attribute, size in header_attributes_bytes_sizes:
            header_size += size
            self.__dict__[attribute] = self.file.read(size)

        # read and set size of ids
        header_size += 1
        self.id_size = int.from_bytes(self.file.read(1), byteorder=TREE_BYTEORDER, signed=True)

        # Tuple with the rest of values that need to be read from header of rtree file
        header_attributes_int_sizes = (
            # ('id_size', 1, True),
            ('dimensions', 4, False),
            ('node_size', 4, False),
            ('highest_id', self.id_size, True),
            ('null_node_id', self.id_size, True),
            ('root_id', self.id_size, False),
            ('parameters_size', 1, False),
            ('tree_depth', 4, False),
        )

        for attribute, size, signed in header_attributes_int_sizes:
            header_size += size
            self.__dict__[attribute] = int.from_bytes(self.file.read(size), byteorder=TREE_BYTEORDER, signed=signed)

        self.offset_size = header_size
        return header_size

    def write_header(self) -> int:
        self.file.seek(0, 0)
        header_attributes_bytes_sizes = (
            (self.unique_sequence, UNIQUE_SEQUENCE_LENGTH),
            (self.config_hash, CONFIG_HASH_LENGTH)
        )
        header_attributes_int_sizes = (
            (self.id_size, 1, True),
            (self.dimensions, 4, False),
            (self.node_size, 4, False),
            (self.highest_id, self.id_size, True),
            (self.null_node_id, self.id_size, True),
            (self.root_id, self.id_size, False),
            (self.parameters_size, 1, False),
            (self.tree_depth, 4, False)
        )

        header_size = 0
        for attribute_b, size in header_attributes_bytes_sizes:
            header_size += size
            self.file.write(attribute_b)
        for attribute, size, signed in header_attributes_int_sizes:
            header_size += size
            self.file.write(attribute.to_bytes(size, byteorder=TREE_BYTEORDER, signed=signed))

        self.file.flush()
        self.offset_size = header_size
        return header_size

    def __get_node_address(self, node_id: int):
        self.current_position = self.offset_size + (node_id * self.node_size)
        return self.current_position

    def __get_next_node_address(self) -> int:
        old_position = self.current_position
        self.current_position = self.current_position + self.node_size
        return old_position

    def __get_node_object(self, node_id: Optional[int]) -> RTreeNode:

        # read flag, that determines whether the node is a leaf
        is_leaf = bool(bool.from_bytes(self.file.read(self.node_flag_size), byteorder=TREE_BYTEORDER, signed=False))
        # is_leaf = int.from_bytes(self.file.read(NODE_FLAG_SIZE), byteorder=TREE_BYTEORDER, signed=False) == 1

        parent_id = int.from_bytes(self.file.read(self.id_size), byteorder=TREE_BYTEORDER, signed=True)

        # reads the range in each dimension
        rectangle = []
        for _ in range(self.dimensions):
            low = int.from_bytes(self.file.read(self.parameters_size), byteorder=TREE_BYTEORDER, signed=True)
            high = int.from_bytes(self.file.read(self.parameters_size), byteorder=TREE_BYTEORDER, signed=True)
            rectangle.append(MBBDim(low, high))

        # reads the ids of children nodes.
        child_nodes = []
        for _ in range(self.children_per_node):
            child_id = int.from_bytes(self.file.read(self.id_size), byteorder=TREE_BYTEORDER, signed=True)
            if child_id != self.null_node_id:
                child_nodes.append(child_id)
            # else:
            #     break

        return RTreeNode(node_id=node_id, parent_id=parent_id, mbb=MBB(tuple(rectangle)), child_nodes=child_nodes,
                         is_leaf=is_leaf)

    def get_node(self, node_id: int) -> Optional[RTreeNode]:
        if node_id > self.highest_id:
            return None
        address = self.__get_node_address(node_id)

        self.file.seek(address, 0)
        self.nodes_read_count += 1
        return self.__get_node_object(node_id)

    # probably deprecated
    def get_next_node(self) -> Optional[RTreeNode]:
        address = self.__get_next_node_address()
        if self.current_position >= self.filesize:
            return None

        self.file.seek(address, 0)
        self.nodes_read_count += 1
        return self.__get_node_object(None)

    def write_node_on_position(self):
        pass

    def insert_node(self, node: RTreeNode):
        """Writes node on the current position of the file head."""
        flag = int(node.is_leaf)
        self.file.write(flag.to_bytes(self.node_flag_size, byteorder=TREE_BYTEORDER, signed=False))

        if node.parent_id is None:
            raise Exception("Parent id cannot be None when saving to file.")

        self.file.write(node.parent_id.to_bytes(self.id_size, byteorder=TREE_BYTEORDER, signed=True))

        for one_dim in node.mbb.box:
            # for record in one_dimension:
            #     self.file.write(record.to_bytes(self.parameter_size, byteorder=TREE_BYTEORDER, signed=True))
            self.file.write(one_dim.low.to_bytes(self.parameters_size, byteorder=TREE_BYTEORDER, signed=True))
            self.file.write(one_dim.high.to_bytes(self.parameters_size, byteorder=TREE_BYTEORDER, signed=True))

        # write child nodes
        for child in node.child_nodes:
            self.file.write(child.to_bytes(self.id_size, byteorder=TREE_BYTEORDER, signed=True))

        # write null child nodes
        for child in range(self.children_per_node - len(node.child_nodes)):
            self.file.write(int(self.null_node_id).to_bytes(self.id_size, byteorder=TREE_BYTEORDER, signed=True))

        # writes padding
        self.file.write(int(0).to_bytes(self.node_padding, byteorder=TREE_BYTEORDER, signed=False))

        self.file.flush()
        # if not update_only:
        #     self.highest_id += 1
        # node_id = self.highest_id
        self.__update_file_size()
        self.current_position = self.file.tell()

        self.nodes_written_count += 1
        #
        # return node_id

    def create_node(self, node: RTreeNode) -> int:
        """Writes node on the current position of the file head."""
        # moves the file handle to the end of file
        self.file.seek(0, 2)
        self.insert_node(node)
        self.highest_id += 1
        node_id = self.highest_id
        return node_id

    def update_tree_depth(self, tree_depth: int):
        # todo delete
        self.tree_depth = tree_depth
        # write_header()

    def update_node(self, node_id: int, node: RTreeNode):
        address = self.__get_node_address(node_id)
        self.file.seek(address, 0)
        self.nodes_written_count += 1
        self.insert_node(node)
        return node_id
