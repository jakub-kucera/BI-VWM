import os

from rtree import rtree, database_entry
from rtree.node import Node
from rtree.tree_file_handler import TreeFileHandler

if __name__ == '__main__':
    print("hello, friend")

    # dimensions, bytesize-of-dims,
    #          2,       List[x, y],

    tree = rtree.RTree()
"""

(leaf_flag), N * [min, max], K * id
(leaf_flag), N * [min, max], K * byte address

flag, N * [min, max], (pickled) data
"""
#  K * byte address
"""
same cache for both, or 2 separate caches?

"""