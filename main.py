from rtree import rtree
from rtree.mbb import MBBDim, MBB

from rtree.tree_file_handler import TreeFileHandler

if __name__ == '__main__':
    print("hello, friend")
    # print(cpu_count())
    tree = rtree.RTree()

"""
Non-leaf node:
(leaf_flag), N * [min, max], K * id
Leaf node:
(leaf_flag), N * [min, max], K * byte address

Database entry:
flag(marked as deleted), N * [min, max], (pickled) data
"""

"""
Database
RTREE main functionality (high level stuff)
Load from files RTree
Rebuilding
Cache

Demo CLI interface for user

test search speed with linear search
    a) directly go through database file. Slow. Have to call pickle all the time
    b) pre-generate a file with only coordinates/box and byte address to database (dont count into measured time)
        measure time going through this new file

MatPlotLib

insert DatabaseEntry from JSON
"""
