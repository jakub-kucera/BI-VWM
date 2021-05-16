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

        print("/")
        for found_node in tree.search_area([100, 140], [130, 140]):
            print(found_node)
        print("/")
        for found_node in tree.search_knn(4, coordinates=[130, 140]):
            print(found_node)
    except Exception:
        traceback.print_exc()
    del tree
    if delete_after:
        delete_saved_rtree()


def test_rtree_create_entry(dimensions: int, count: int, low: int, high: int):
    try:
        os.remove(TESTING_DIRECTORY + TREE_FILE_TEST)
        os.remove(TESTING_DIRECTORY + DATABASE_FILE_TEST)
    except FileNotFoundError:
        pass

    tree = RTree(working_directory=TESTING_DIRECTORY,
                 tree_file=TREE_FILE_TEST,
                 database_file=DATABASE_FILE_TEST,
                 dimensions=dimensions)
    total_insert_count = 0

    random.seed(2)

    coordinates_all = []
    for _ in range(dimensions):
        coords_dim = list(range(low, high))
        random.shuffle(coords_dim)
        coordinates_all.append(coords_dim[0:count])

    for c in range(0, count):
        # coordinates = [random.randint(low, high) for _ in range(0, dimensions)]
        coordinates = []
        for d in range(dimensions):
            coordinates.append(coordinates_all[d][c])

        total_insert_count += 1
        data = f"c: {c} coords: {coordinates}"
        tree.insert_entry(DatabaseEntry(coordinates=coordinates, data=data))

        found_entry = tree.search_entry(coordinates)

        if found_entry is None:
            print(f"missing {c} coordinates {coordinates} data {data}")
            # visualize(tree, show_mbbs_only=False, open_img=True)
        # assert found_entry is not None
        # assert found_entry.data == data
        # assert found_entry.coordinates == coordinates
        # assert found_entry.is_present is True

    del tree
    os.remove(TESTING_DIRECTORY + TREE_FILE_TEST)
    os.remove(TESTING_DIRECTORY + DATABASE_FILE_TEST)


if __name__ == '__main__':
    # create_new_tree(delete_after=False, low=100, high=200)
    # load_tree(delete_after=False)
    # create_new_tree(delete_after=False, low=0, high=100)
    load_tree(delete_after=False)
    # test_rtree_create_entry(2, 100, 0, 100)

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
