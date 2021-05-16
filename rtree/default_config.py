"""File with default values"""
from typing import Final

WORKING_DIRECTORY: Final[str] = "saved_data/"
DEFAULT_TREE_FILE: Final[str] = "rtree.bin"
DEFAULT_DATABASE_FILE: Final[str] = "database.bin"
TREE_BYTEORDER: Final[str] = 'little'
DATABASE_BYTEORDER: Final[str] = 'little'

DEFAULT_DIMENSIONS: Final[int] = 2
DEFAULT_NODE_SIZE: Final[int] = 1024  # 512  # 1024
NODE_ID_SIZE: Final[int] = 8
PARAMETER_RECORD_SIZE: Final[int] = 4
RECORD_FLAG_SIZE: Final[int] = 1
NODE_FLAG_SIZE: Final[int] = 1
NULL_NODE_ID: Final[int] = -1
DELETE_TREE_INDEX_FILE = False
UNIQUE_SEQUENCE_LENGTH: Final[int] = 20
CONFIG_HASH_LENGTH: Final[int] = 20  # length of hash from SHA1 function
MINIMUM_NODE_FILL: Final[float] = 0.35

CACHE_MEMORY_SIZE: Final[int] = 8 * 1024  # 8MB for allocated cache

# Testing
TESTING_DIRECTORY: Final[str] = "tests/testing_data/"
TREE_FILE_TEST: Final[str] = "testingTree.bin"
DATABASE_FILE_TEST: Final[str] = "test_database.bin"
PRINT_OUTPUT_TEST = False
DEMO_UNIQUE_SEQUENCE = b'\xf1\x97L\xc9\xe7\x1c\x98\x88\xf1\x97L\xc9\xe7\x1c\x98\x88\xe7\x1c\x98\x34'
DEMO_CONFIG_HASH = b'\xf1\x97L\xc9\xe7\x1c\x98\x88\xf1\x97L\xc9\xe7\x1c\x98\x88\xe7\x1c\x98\x21'
