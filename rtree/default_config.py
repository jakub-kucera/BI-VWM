"""File with default values"""

WORKING_DIRECTORY = "saved_data/"
DEFAULT_TREE_FILE = "rtree.bin"
DEFAULT_DATABASE_FILE = "database.bin"
TREE_BYTEORDER = 'little'
DATABASE_BYTEORDER = 'little'

DEFAULT_DIMENSIONS = 2
DEFAULT_NODE_SIZE = 1024
NODE_ID_SIZE = 8
PARAMETER_RECORD_SIZE = 4
RECORD_FLAG_SIZE = 1
NODE_FLAG_SIZE = 1
NULL_NODE_ID = -1
DELETE_TREE_INDEX_FILE = False
UNIQUE_SEQUENCE_LENGTH = 20
CONFIG_HASH_LENGTH = 20  # length of hash from SHA1 function

# Testing
TREE_FILE_TEST = "testingTree.bin"
PRINT_OUTPUT_TEST = False
