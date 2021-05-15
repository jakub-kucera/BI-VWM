import os
import random
from time import sleep
from typing import Tuple

import pytest

from rtree.data.database_entry import DatabaseEntry
from rtree.rtree import RTree
from rtree.default_config import *
from rtree.ui.visualiser import visualize


@pytest.mark.parametrize('rtree_args', [
    {"dimensions": -5},
    {"dimensions": 10000000000},
    {"parameters_size": -5},
    {"parameters_size": 300},
    {"id_size": -5},
    {"id_size": 10000000000},
    {"node_size": -5},
    {"node_size": 10000000000},
    {
        "node_size": 128,
        "parameters_size": 100
    },
])
@pytest.mark.filterwarnings("ignore:")
def test_tree_create_invalid(rtree_args):
    try:
        os.remove(TESTING_DIRECTORY + TREE_FILE_TEST)
        os.remove(TESTING_DIRECTORY + DATABASE_FILE_TEST)
    except FileNotFoundError:
        pass

    with pytest.raises(Exception):
        tree = RTree(working_directory=TESTING_DIRECTORY,
                     tree_file=TREE_FILE_TEST,
                     database_file=DATABASE_FILE_TEST,
                     **rtree_args)

    try:
        os.remove(TESTING_DIRECTORY + DATABASE_FILE_TEST)
    except Exception:
        pass
    try:
        os.remove(TESTING_DIRECTORY + TREE_FILE_TEST)
    except Exception:
        pass


@pytest.mark.parametrize('rtree_args', [
    dict(),
    {"override_file": True},
    {"override_file": False},
    {"dimensions": 5},
    {"parameters_size": 5},
    {"id_size": 10},
    {
        "node_size": 1024,
        "parameters_size": 100
    },
    {
        "override_file": False,
        "dimensions": 5,
        "parameters_size": 5,
        "id_size": 10
    }
])
def test_tree_create(rtree_args):
    try:
        os.remove(TESTING_DIRECTORY + TREE_FILE_TEST)
        os.remove(TESTING_DIRECTORY + DATABASE_FILE_TEST)
    except FileNotFoundError:
        pass

    tree = RTree(working_directory=TESTING_DIRECTORY,
                 tree_file=TREE_FILE_TEST,
                 database_file=DATABASE_FILE_TEST,
                 **rtree_args)
    del tree
    os.remove(TESTING_DIRECTORY + TREE_FILE_TEST)
    os.remove(TESTING_DIRECTORY + DATABASE_FILE_TEST)


@pytest.mark.parametrize('rtree_args', [
    dict(),
    # {"override_file": False},
    {"dimensions": 5},
    {"parameters_size": 5},
    {
        "node_size": 1024,
        "parameters_size": 100
    },
    {"id_size": 10},
    {
        # "override_file": False,
        "dimensions": 5,
        "parameters_size": 5,
        "id_size": 10
    }
])
def test_tree_create_save_load(rtree_args):
    try:
        os.remove(TESTING_DIRECTORY + TREE_FILE_TEST)
        os.remove(TESTING_DIRECTORY + DATABASE_FILE_TEST)
    except FileNotFoundError:
        pass

    RTree(working_directory=TESTING_DIRECTORY,
          tree_file=TREE_FILE_TEST,
          database_file=DATABASE_FILE_TEST,
          **rtree_args)

    loaded_tree = RTree(working_directory=TESTING_DIRECTORY,
                        tree_file=TREE_FILE_TEST,
                        database_file=DATABASE_FILE_TEST)

    for key, value in rtree_args.items():
        if key in loaded_tree.__dict__.keys():
            assert loaded_tree.__dict__[key] == value

    del loaded_tree
    os.remove(TESTING_DIRECTORY + TREE_FILE_TEST)
    os.remove(TESTING_DIRECTORY + DATABASE_FILE_TEST)


@pytest.mark.parametrize('rtree_args', [
    dict(),
    {
        "dimensions": 5,
    },
    {
        "parameters_size": 5,
    },
    {
        "node_size": 1024,
        "parameters_size": 100
    },
    {"id_size": 10},
    {
        "dimensions": 5,
        "parameters_size": 5,
        "id_size": 10
    }
])
def test_tree_create_save_override(rtree_args):
    try:
        os.remove(TESTING_DIRECTORY + TREE_FILE_TEST)
        os.remove(TESTING_DIRECTORY + DATABASE_FILE_TEST)
    except FileNotFoundError:
        pass

    RTree(working_directory=TESTING_DIRECTORY,
          tree_file=TREE_FILE_TEST,
          database_file=DATABASE_FILE_TEST,
          **rtree_args)

    loaded_tree = RTree(working_directory=TESTING_DIRECTORY,
                        tree_file=TREE_FILE_TEST,
                        database_file=DATABASE_FILE_TEST,
                        override_file=True)

    for key, value in rtree_args.items():
        if key in loaded_tree.__dict__.keys():
            assert loaded_tree.__dict__[key] != value

    del loaded_tree
    os.remove(TESTING_DIRECTORY + TREE_FILE_TEST)
    os.remove(TESTING_DIRECTORY + DATABASE_FILE_TEST)


@pytest.mark.parametrize('dimensions, count, low, high', [
    # (1, 100, 0, 100),
    (2, 100, 0, 100),
    # (3, 100, 0, 100),
    # (10, 100, 0, 100),
    # (2, 300, 0, 100),
    # (2, 100, -100, 500),
    # (3, 1000, -1000, 5000),
])
def test_rtree_create_entry(dimensions: int, count: int, low: int, high: int):
    try:
        os.remove(TESTING_DIRECTORY + TREE_FILE_TEST)
        os.remove(TESTING_DIRECTORY + DATABASE_FILE_TEST)
    except FileNotFoundError:
        pass

    tree = RTree(working_directory=TESTING_DIRECTORY,
                 tree_file=TREE_FILE_TEST,
                 database_file=DATABASE_FILE_TEST,
                 dimensions=dimensions,
                 override_file=True)
    total_insert_count = 0

    # random.seed(2)

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

        print(coordinates)

        total_insert_count += 1
        data = f"c: {c} coords: {coordinates}"
        tree.insert_entry(DatabaseEntry(coordinates=coordinates, data=data))

        found_entry = tree.search_entry(coordinates)

        if found_entry is None:
            print(f"c:{c} missing: {coordinates}")
            # visualize(tree, show_mbbs_only=False, open_img=True)
        # assert found_entry is not None
        # assert found_entry.data == data
        # assert found_entry.coordinates == coordinates
        # assert found_entry.is_present is True

    # assert len(tree.search_k_nearest_neighbours(200, [0, 0])) == count
    # print("===============================================================")
    # # map(lambda x: print(x.coordinates), tree.search_k_nearest_neighbours(200, [0, 0]))
    # for count, neighbor in enumerate(sorted(tree.search_k_nearest_neighbours(200, [0, 0]), key=lambda x: x.coordinates[0])):
    #     print(f"c:{count} missing: {neighbor.coordinates}")
    # assert False

    print("===============================================================")
    for c in range(0, count):
        # coordinates = [random.randint(low, high) for _ in range(0, dimensions)]
        coordinates = []
        for d in range(dimensions):
            coordinates.append(coordinates_all[d][c])

        found_entry = tree.search_entry(coordinates)

        if found_entry is None:
            print(f"c:{c} missing: {coordinates}")

    print("===============================================================")
    for c in range(0, count):
        # coordinates = [random.randint(low, high) for _ in range(0, dimensions)]
        coordinates = []
        for d in range(dimensions):
            coordinates.append(coordinates_all[d][c])

        found_entry = tree.search_entry(coordinates)

        if found_entry is None:
            print(f"c:{c} missing: {coordinates}")

    assert False

    del tree
    os.remove(TESTING_DIRECTORY + TREE_FILE_TEST)
    os.remove(TESTING_DIRECTORY + DATABASE_FILE_TEST)

# @pytest.mark.parametrize('entries', [
#     (
#         DatabaseEntry([0, 0], [1, 2])
#     )
# ])
# def test_rtree_delete_entry(entries: Tuple[DatabaseEntry]):
#     pass
