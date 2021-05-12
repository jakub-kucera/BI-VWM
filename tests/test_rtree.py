import os

import pytest

from rtree.rtree import RTree
from rtree.default_config import *


@pytest.mark.parametrize('rtree_args', [
    {"dimensions": -5},
    {"dimensions": 10000000000},
    {"parameters_size": -5},
    {"parameters_size": 300},
    {"id_size": -5},
    {"id_size": 10000000000},
    {"node_size": -5},
    {"node_size": 10000000000},
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
    {"parameters_size": 100},
    {"id_size": 10},
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
    {"parameters_size": 100},
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
        "parameters_size": 100,
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
