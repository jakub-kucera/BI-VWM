import os
import pathlib
import time
import traceback
import random
from time import sleep
from typing import Tuple, List

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
    # tree = RTree()
    tree = RTree(tree_file="bigtree.bin", database_file="bigdatabase.bin")

    x, y = -1, -1
    total_insert_count = 0
    try:
        for _ in range(0, new_nodes_count):
            x = random.randint(low, high)
            y = random.randint(low, high)

            total_insert_count += 1
            tree.insert_entry(DatabaseEntry(coordinates=[x, y], data=f"This is generated x: {x}; y: {y}"))

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


def search_tree_knn(delete_after: bool = True, attempt_count: int = 2,
                    find_k_entries: int = 10, low: int = 0, high: int = 100):
    try:
        # tree = RTree()
        tree = RTree(tree_file="bigtree.bin", database_file="bigdatabase.bin")

        for c in range(attempt_count):
            x = random.randint(low, high)
            y = random.randint(low, high)

            normal_start = time.time()
            found_entries = tree.search_knn(find_k_entries, [x, y])
            normal_end = time.time()
            found_entries_coords = [entry.coordinates for entry in found_entries]

            lin_start = time.time()
            lin_found_entries = tree.database.linear_search_knn(find_k_entries, [x, y])
            lin_end = time.time()
            lin_found_entries_coords = [entry.coordinates for entry in lin_found_entries]

            print(f"KNN normal took: {normal_end - normal_start}, entries: {found_entries_coords}")
            print(f"KNN linear took: {lin_end - lin_start}, entries: {lin_found_entries_coords}")
            print("===================================================")
    except Exception:
        traceback.print_exc()
    del tree
    if delete_after:
        delete_saved_rtree()


def search_tree_area(delete_after: bool = True, attempt_count: int = 2,
                     low: List[int] = [0, 0], high: List[int] = [200, 200]):
    try:
        tree = RTree()
        # tree = RTree(tree_file="bigtree.bin", database_file="bigdatabase.bin")

        for c in range(attempt_count):
            normal_start = time.time()
            found_entries = tree.search_area(low, high)
            normal_end = time.time()
            found_entries_coords = [entry.coordinates for entry in found_entries]

            lin_start = time.time()
            lin_found_entries = tree.database.linear_search_area(low, high)
            lin_end = time.time()
            lin_found_entries_coords = [entry.coordinates for entry in lin_found_entries]

            print(f"Area search normal took: {normal_end - normal_start}, entries: {found_entries_coords}")
            print(f"Area search took: {lin_end - lin_start}, entries: {lin_found_entries_coords}")
            print("===================================================")
    except Exception:
        traceback.print_exc()
    del tree
    if delete_after:
        delete_saved_rtree()


if __name__ == '__main__':
    # delete_saved_rtree()
    create_new_tree(delete_after=False, new_nodes_count=1000000, low=-100000, high=200000)
    # search_tree_knn(delete_after=False)
    # search_tree_area(delete_after=False)

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
