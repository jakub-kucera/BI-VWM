import os
import pathlib

from rtree.default_config import WORKING_DIRECTORY
from rtree.rtree import RTree


def delete_saved_rtree():
    file_dir = pathlib.Path(WORKING_DIRECTORY)
    for entry in file_dir.iterdir():
        if entry.is_file():
            # print(entry)
            os.remove(entry)


if __name__ == '__main__':
    print("hello, friend")
    # print(cpu_count())
    tree = RTree()
    print("tree.trunk_id")
    print(tree.trunk_id)
    print("tree.get_node(tree.trunk_id)")
    print(tree.get_node(tree.trunk_id))
    # visualize(r_tree=tree)
    # delete_saved_rtree()

"""
Non-leaf node:
(leaf_flag), N * [min, max], K * id
Leaf node:
(leaf_flag), N * [min, max], K * byte address

Database entry:
flag(marked as deleted), N * [min, max], (pickled) data
"""

"""
todo suggestions:
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

(insert DatabaseEntry from JSON)
"""
