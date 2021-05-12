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


# rtree 1102B
# database 40B

# rtree 1102B
# database 81B

if __name__ == '__main__':
    print("hello, friend")
    # print(cpu_count())
    tree = RTree()
    print("tree.root_id")
    print(tree.root_id)
    print("tree.get_node(tree.root_id)")
    print(tree._RTree__get_node(tree.root_id))
    # visualize(r_tree=tree)
    # delete_saved_rtree()

    # if not load_from_files:
    #     new_entry = DatabaseEntry([0 for coord in range(self.dimensions)],
    #                               data="this is some data")
    #     new_entry_id = self.database.create(new_entry)
    #     root_node = self.tree_handler.get_node(self.root_id)
    #     if root_node is None:
    #         raise Exception("root node not found")
    #
    #     root_node.insert_entry_from_entry(new_entry_id, new_entry)
    #
    #     self.tree_handler.update_node(self.root_id, root_node)

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
