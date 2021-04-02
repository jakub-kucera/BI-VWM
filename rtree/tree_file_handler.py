import os
from typing import Optional

from rtree.node import Node

DEFAULT_DIMENSIONS = 2
DEFAULT_NODE_SIZE = 1024
DEFAULT_TREE_FILE_NAME = 'rtree.bin'

NODE_ID_SIZE = 8  # todo move to separate file
PARAMETER_RECORD_SIZE = 4
NODE_FLAG_SIZE = 1
TREE_BYTEORDER = 'little'


class TreeFileHandler:  # todo move more stuff to RTree
    def __init__(self,  # no need to use positional arguments, is always called from RTree with all arguments
                 filename: str = DEFAULT_TREE_FILE_NAME,
                 dimensions: int = DEFAULT_DIMENSIONS,
                 node_size: int = DEFAULT_NODE_SIZE):
        self.filename = filename
        self.dimensions = dimensions
        self.node_size = node_size
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
        self.file = open(self.filename, 'r+b')  # todo file check
        # self.file_reading = True
        self.offset_size = 0

        # flag size + coordinates_rectangle size + array of children size
        self.highest_id = -1  # when loading from existing file, calculate from file size

        # gets size of file
        self.file.seek(0, 2)
        self.filesize = self.file.tell()
        self.file.seek(0, 0)
        highest_id_check = int((self.filesize - self.offset_size) / self.node_size)

        self.current_position = self.offset_size

    def __del__(self):
        self.file.close()

    def __str__(self):
        return str(self.__dict__)

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
            if child_id != 0:  # todo change how to differentiate between null nodes, maybe use same 1st byte as is_leaf (7 unused bits)
                child_nodes.append(child_id)

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
        # close file opened for reading
        self.file.close()

        # opens file for writing
        with open(self.filename, 'r+b') as writing_file:
            # moves the file handle to the end of file
            writing_file.seek(0, 2)

            flag = int(node.is_leaf)
            writing_file.write(flag.to_bytes(1, byteorder=TREE_BYTEORDER, signed=False))

            for one_dimension in node.coordinates_rectangle:
                for record in one_dimension:
                    writing_file.write(record.to_bytes(PARAMETER_RECORD_SIZE, byteorder=TREE_BYTEORDER, signed=True))

            for child in node.children:
                writing_file.write(child.to_bytes(NODE_ID_SIZE, byteorder=TREE_BYTEORDER, signed=True))

            for child in range(self.children_per_node - len(node.children)):
                writing_file.write(int(0).to_bytes(NODE_ID_SIZE, byteorder=TREE_BYTEORDER, signed=True))

            # writes padding
            writing_file.write(int(0).to_bytes(self.node_padding, byteorder=TREE_BYTEORDER, signed=False))

            self.highest_id += 1
            node_id = self.highest_id

        # opens file for reading again
        self.file = open(self.filename, 'r+b')
        return node_id
