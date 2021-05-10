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

CACHE_NODES = 16

# Testing
TESTING_DIRECTORY = "tests/testing_data/"
TREE_FILE_TEST = "testingTree.bin"
DATABASE_FILE_TEST = "test_database.bin"
PRINT_OUTPUT_TEST = False
DEMO_UNIQUE_SEQUENCE = b'\xf1\x97L\xc9\xe7\x1c\x98\x88\xf1\x97L\xc9\xe7\x1c\x98\x88\xe7\x1c\x98\x34'
DEMO_CONFIG_HASH = b'\xf1\x97L\xc9\xe7\x1c\x98\x88\xf1\x97L\xc9\xe7\x1c\x98\x88\xe7\x1c\x98\x21'
