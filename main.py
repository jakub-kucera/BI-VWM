import os
import pathlib
import traceback
import random
from time import sleep

from rtree.data.database_entry import DatabaseEntry
from rtree.default_config import WORKING_DIRECTORY, TESTING_DIRECTORY, TREE_FILE_TEST, DATABASE_FILE_TEST
from rtree.rtree import RTree
from rtree.ui.visualiser import visualize


def delete_saved_rtree():
    file_dir = pathlib.Path(WORKING_DIRECTORY)
    for entry in file_dir.iterdir():
        if entry.is_file():
            # print(entry)
            os.remove(entry)


def create_new_tree(delete_after: bool = True, new_nodes_count: int = 100, low: int = 0, high: int = 100):
    print("hello, friend")
    # print(cpu_count())
    tree = RTree()

    x, y = -1, -1
    total_insert_count = 0
    try:
        # random_x = list(range(0, 500))
        # random.shuffle(random_x)
        # random_x = random_x[0:6]
        # for x in random_x:
        #     random_y = list(range(0, 500))
        #     random.shuffle(random_y)
        #     random_y = random_y[0:6]
        #     for y in random_y:
        #         # print(f"x: {x}; y: {y}")
        #         total_insert_count += 1
        #         tree.insert_entry(DatabaseEntry(coordinates=[x, y], data=f"This is generated x: {x}; y: {y}"))
        #
        for _ in range(0, new_nodes_count):
            x = random.randint(low, high)
            y = random.randint(low, high)

            total_insert_count += 1
            tree.insert_entry(DatabaseEntry(coordinates=[x, y], data=f"This is generated x: {x}; y: {y}"))

        # print("\n==========================")
        # for found_node in tree.search_rectangle(coordinates_min=[0, 0], coordinates_max=[20, 20]):
        #     print(found_node)
        # print("\n==========================")
        # for found_node in tree.search_k_nearest_neighbours(4, coordinates=[0, 4]):
        #     print(found_node)

        visualize(tree, show_mbbs_only=False)  # False)

    except Exception as e:
        print(f"x: {x}; y: {y}")
        print(f"total_insert_count {total_insert_count}")
        traceback.print_exc()

    print(f"total_insert_count {total_insert_count}")
    # tree.tree_handler.file.close()
    # tree.database.file.close()
    del tree
    if delete_after:
        delete_saved_rtree()


def load_tree(delete_after: bool = True):
    try:
        tree = RTree()
        visualize(tree, show_mbbs_only=False)
    except Exception:
        traceback.print_exc()
    del tree
    if delete_after:
        delete_saved_rtree()


if __name__ == '__main__':
    create_new_tree(delete_after=False, low=100, high=200)
    load_tree(delete_after=True)

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
