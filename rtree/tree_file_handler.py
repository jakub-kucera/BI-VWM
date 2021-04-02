import os
from typing import Optional

from rtree.node import Node
from rtree.default_config import *


class TreeFileHandler:  # todo move more stuff to RTree
    def __init__(self,  # no need to use positional arguments, is always called from RTree with all arguments
                 filename: str = DEFAULT_TREE_FILE_PATH,
                 dimensions: int = DEFAULT_DIMENSIONS,
                 node_size: int = DEFAULT_NODE_SIZE):
        self.filename = filename
        self.dimensions = dimensions
        self.node_size = node_size

        self.parameter_record_size = PARAMETER_RECORD_SIZE  # todo replace, add to header
        self.node_id_size = NODE_ID_SIZE
        self.null_node_id = NULL_NODE_ID

        self.children_per_node = int(
            (self.node_size - NODE_FLAG_SIZE - (self.dimensions * PARAMETER_RECORD_SIZE * 2)) / NODE_ID_SIZE)

        # since children_per_node is calculated from node_size, then there is probably a few bytes left
        self.node_padding = self.node_size - (NODE_FLAG_SIZE + (self.dimensions * PARAMETER_RECORD_SIZE * 2) + (
                self.children_per_node * NODE_ID_SIZE))
        # if exists
        if os.path.isfile(self.filename):  # maybe move to RTree
            # todo load from file
            pass
        else:
            # create file
            with open(self.filename, 'w+b') as f:
                pass
        # file opened for reading by default. Reading from file is more common
        try:
            self.file = open(self.filename, 'r+b')  # todo file check
        except IOError:
            input("File cannot be opened")

        # todo self.offset_size = write header

        # self.file_reading = True
        self.offset_size = 0

        # flag size + coordinates_rectangle size + array of children size
        self.highest_id = NULL_NODE_ID  # when loading from existing file, calculate from file size

        # gets size of file
        self.file.seek(0, 2)
        self.filesize = self.file.tell()
        self.file.seek(0, 0)
        highest_id_check = int((self.filesize - self.offset_size) / self.node_size)

        self.current_position = self.offset_size

    def __del__(self):
        # self.file.flush()
        self.file.close()
        if DELETE_TREE_INDEX_FILE:
            os.remove(self.filename)

    def __str__(self):
        return str(self.__dict__)

    def read_header(self):
        dimensions = self.dimensions  # 4B
        node_size = self.node_size  # $B
        highest_id = self.highest_id  # 8B
        parameter_record_size = self.parameter_record_size  # 1B
        node_id_size = self.node_id_size  # 1B
        null_node_id = self.null_node_id  # 8B
        pass

    def write_header(self):
        dimensions = self.dimensions  # 4B
        node_size = self.node_size  # $B
        highest_id = self.highest_id  # 8B
        parameter_record_size = self.parameter_record_size  # 1B
        node_id_size = self.node_id_size  # 1B
        null_node_id = self.null_node_id  # 8B
        pass

    def __get_node_address(self, node_id: int):
        self.current_position = self.offset_size + (node_id * self.node_size)
        return self.current_position

    def __get_next_node_address(self):
        old_position = self.current_position
        self.current_position = self.current_position + self.node_size
        # todo bound check
        return old_position

    """
    (leaf_flag), N * [min, max], K * id
    (leaf_flag), N * [min, max], K * byte address
    """

    def __get_node_object(self) -> Node:

        # read flag, that determines whether the node is a leaf
        is_leaf = bool.from_bytes(self.file.read(NODE_FLAG_SIZE), byteorder=TREE_BYTEORDER, signed=False)
        # is_leaf = int.from_bytes(self.file.read(NODE_FLAG_SIZE), byteorder=TREE_BYTEORDER, signed=False) == 1

        # reads the range in each dimension
        rectangle = []
        for _ in range(self.dimensions):  # todo maybe use float by default
            mini = int.from_bytes(self.file.read(PARAMETER_RECORD_SIZE), byteorder=TREE_BYTEORDER, signed=True)
            maxi = int.from_bytes(self.file.read(PARAMETER_RECORD_SIZE), byteorder=TREE_BYTEORDER, signed=True)
            rectangle.append([mini, maxi])

        # reads the ids of children nodes.
        child_nodes = []
        for _ in range(self.children_per_node):
            child_id = int.from_bytes(self.file.read(NODE_ID_SIZE), byteorder=TREE_BYTEORDER, signed=True)
            if child_id != NULL_NODE_ID:
                child_nodes.append(child_id)
            # else:
            #     break

        return Node(is_leaf, rectangle, child_nodes)

    def get_node(self, node_id: int) -> Optional[Node]:
        # if node_id > self.highest_id:
        #     return None
        address = self.__get_node_address(node_id)

        # if address > self.filesize:
        #     return None
        # self.file.seek(address - self.current_position, 1)

        self.file.seek(address, 0)
        return self.__get_node_object()

    def __encode_flag(self, is_leaf: bool, is_):
        pass

    #
    def __decode_flag(self, byte_flag: bytes):
        pass

    def write_node(self, node: Node) -> int:

        # moves the file handle to the end of file
        self.file.seek(0, 2)

        flag = int(node.is_leaf)
        self.file.write(flag.to_bytes(1, byteorder=TREE_BYTEORDER, signed=False))

        for one_dimension in node.coordinates_rectangle:
            for record in one_dimension:
                self.file.write(record.to_bytes(PARAMETER_RECORD_SIZE, byteorder=TREE_BYTEORDER, signed=True))

        # write child nodes
        for child in node.children:
            self.file.write(child.to_bytes(NODE_ID_SIZE, byteorder=TREE_BYTEORDER, signed=True))

        # write null child nodes
        for child in range(self.children_per_node - len(node.children)):
            self.file.write(int(NULL_NODE_ID).to_bytes(NODE_ID_SIZE, byteorder=TREE_BYTEORDER, signed=True))

        # writes padding
        self.file.write(int(0).to_bytes(self.node_padding, byteorder=TREE_BYTEORDER, signed=False))

        self.highest_id += 1
        node_id = self.highest_id

        return node_id
