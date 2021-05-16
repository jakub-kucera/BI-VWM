import os
import random
import secrets
from typing import Tuple, List

import pytest

from rtree.data.database_entry import DatabaseEntry
from rtree.data.rtree_node import RTreeNode, MBB, MBBDim
from rtree.data.tree_file_handler import TreeFileHandler
from rtree.default_config import *

# testing_unique_sequence = b"\x00\xb0\x83\x82\x8c\x05\x7b\xc3\x38\x69\xff\xde\xed\xba\xd6\x02\x69\xd2\x80\x94"
# testing_config_hash = b"\x4e\x9a\xe1\xf1\xe8\x91\xe9\x41\x95\x64\x29\x89\x88\x86\x53\xf1\x22\xa9\xde\xb9"
testing_unique_sequence = secrets.token_bytes(UNIQUE_SEQUENCE_LENGTH)
testing_config_hash = secrets.token_bytes(CONFIG_HASH_LENGTH)

RTreeNode.max_entries_count = 50


def compare_tree_file_handler_dictionary(a_dict, b_dict):
    return (a_dict['filename'] == b_dict['filename'] and
            a_dict['dimensions'] == b_dict['dimensions'] and
            a_dict['node_size'] == b_dict['node_size'] and
            a_dict['parameters_size'] == b_dict['parameters_size'] and
            a_dict['id_size'] == b_dict['id_size'] and
            a_dict['null_node_id'] == b_dict['null_node_id'] and
            a_dict['node_flag_size'] == b_dict['node_flag_size'] and
            a_dict['highest_id'] == b_dict['highest_id'] and
            a_dict['children_per_node'] == b_dict['children_per_node'] and
            a_dict['node_padding'] == b_dict['node_padding'] and
            a_dict['tree_depth'] == b_dict['tree_depth'] and
            a_dict['unique_sequence'] == b_dict['unique_sequence'] and
            a_dict['config_hash'] == b_dict['config_hash'] and
            a_dict['filesize'] == b_dict['filesize'])  # not current position


@pytest.mark.parametrize('rtree_args', [
    {"dimensions": -5},
    {"dimensions": 10000000000},
    {"parameters_size": -5},
    {"parameters_size": 10000000000},
    {"id_size": -5},
    {"id_size": 10000000000},
    {"node_size": -5},
    {"node_size": 10000000000},
    {"tree_depth": -5},
    {"tree_depth": 10000000000},
    {"root_id": -5000000000000000000000},
    {"root_id": 100000000000000000000},
    {"unique_sequence": -5},
    {"unique_sequence": 10000},
    {"config_hash": -5},
    {"config_hash": 10000},
    {"unique_sequence": b'\xf1\x97L\xc9\xe7\x1c\x98\x88\xf1\x97L\xc9\xe7\x1c\x98\x88\xe7\x1c\x98'},
    {"unique_sequence": b'\xf1\x97L\xc9\xe7\x1c\x98\x88\xf1\x97L\xc9\xe7\x1c\x98\x88\xe7\x1c\x98\xe7\x1c'},
    {"config_hash": b'\xf1\x97L\xc9\xe7\x1c\x98\x88\xf1\x97L\xc9\xe7\x1c\x98\x88\xe7\x1c\x98\xe7\x1c'},
    {"config_hash": b'\xf1\x97L\xc9\xe7\x1c\x98\x88\xf1\x97L\xc9\xe7\x1c\x98\x88\xe7\x1c\x98'},  # wrong size
    {"unique_sequence": 10000},
])
@pytest.mark.filterwarnings("ignore:")
def test_create_handler_invalid(rtree_args):
    try:
        os.remove(TESTING_DIRECTORY + TREE_FILE_TEST)
    except FileNotFoundError:
        pass

    with pytest.raises(Exception):
        tree_handler = TreeFileHandler(filename=TESTING_DIRECTORY + TREE_FILE_TEST, **rtree_args)
        del tree_handler

    try:
        os.remove(TESTING_DIRECTORY + TREE_FILE_TEST)
    except Exception:
        pass


@pytest.mark.parametrize('tree_file_handler_args, written_nodes', [
    (
            dict(filename=TESTING_DIRECTORY + TREE_FILE_TEST, dimensions=2, node_size=1024),
            (
                    RTreeNode(node_id=0, parent_id=0, mbb=MBB((MBBDim(1, 1), MBBDim(2, 2))),
                              child_nodes=[5, 5, 2, 2, 2],
                              is_leaf=True),
                    RTreeNode(node_id=1, parent_id=0, mbb=MBB((MBBDim(5, 3), MBBDim(3, 2))), child_nodes=[1, 2],
                              is_leaf=True)
            )
    ),
    (
            dict(filename=TESTING_DIRECTORY + TREE_FILE_TEST),
            (
                    RTreeNode(node_id=0, parent_id=0, mbb=MBB((MBBDim(1, 1), MBBDim(2, 2))), child_nodes=[],
                              is_leaf=False),
                    RTreeNode(node_id=1, parent_id=0, mbb=MBB((MBBDim(5, 4), MBBDim(3, 2))), child_nodes=[1, 2],
                              is_leaf=True)
            )
    ),
    (
            dict(filename=TESTING_DIRECTORY + TREE_FILE_TEST, dimensions=5, node_size=4 * 1024, id_size=4,
                 parameters_size=8, tree_depth=3,
                 root_id=10, unique_sequence=testing_unique_sequence, config_hash=testing_config_hash),
            (
                    RTreeNode(node_id=0, parent_id=0,
                              mbb=MBB((MBBDim(1, 1), MBBDim(2, 2), MBBDim(3, 3), MBBDim(4, 4), MBBDim(5, 5))),
                              child_nodes=[],
                              is_leaf=False),
                    RTreeNode(node_id=1, parent_id=0,
                              mbb=MBB((MBBDim(9, 8), MBBDim(7, 6), MBBDim(5, 4), MBBDim(3, 2), MBBDim(1, 0))),
                              child_nodes=[1, 2], is_leaf=True)
            )
    ),
])
def test_write_read_node_one_handler(tree_file_handler_args, written_nodes):
    if os.path.isfile(TESTING_DIRECTORY + TREE_FILE_TEST):
        os.remove(TESTING_DIRECTORY + TREE_FILE_TEST)

    tree_file_handler = TreeFileHandler(**tree_file_handler_args)

    node_ids = []
    for node in written_nodes:
        new_id = tree_file_handler.create_node(node)
        node_ids += [new_id]

    read_nodes_lst = []
    for node_id in node_ids:
        read_node = tree_file_handler.get_node(node_id)
        read_nodes_lst += [read_node]

    read_nodes = tuple(read_nodes_lst)

    for written, read in zip(written_nodes, read_nodes):
        assert written.__dict__ == read.__dict__

    del tree_file_handler

    if os.path.isfile(TESTING_DIRECTORY + TREE_FILE_TEST):
        os.remove(TESTING_DIRECTORY + TREE_FILE_TEST)


@pytest.mark.parametrize('tree_file_handler_writer_args, written_nodes, tree_file_handler_reader_args', [
    (
            dict(filename=TESTING_DIRECTORY + TREE_FILE_TEST),
            (
                    RTreeNode(node_id=0, parent_id=0, mbb=MBB((MBBDim(1, 1), MBBDim(2, 2))), child_nodes=[],
                              is_leaf=False),
                    RTreeNode(node_id=1, parent_id=0, mbb=MBB((MBBDim(5, 4), MBBDim(3, 2))), child_nodes=[1, 2],
                              is_leaf=True)
            ),
            dict(filename=TESTING_DIRECTORY + TREE_FILE_TEST)
    ),
    (
            dict(filename=TESTING_DIRECTORY + TREE_FILE_TEST, dimensions=5, node_size=4 * 1024, id_size=4,
                 parameters_size=8, tree_depth=3,
                 root_id=5, unique_sequence=testing_unique_sequence, config_hash=testing_config_hash),
            (
                    RTreeNode(node_id=0, parent_id=0,
                              mbb=MBB((MBBDim(1, 1), MBBDim(2, 2), MBBDim(3, 3), MBBDim(4, 4), MBBDim(5, 5))),
                              child_nodes=[],
                              is_leaf=False),
                    RTreeNode(node_id=1, parent_id=0,
                              mbb=MBB((MBBDim(9, 8), MBBDim(7, 6), MBBDim(5, 4), MBBDim(3, 2), MBBDim(1, 0))),
                              child_nodes=[1, 2], is_leaf=True)
            ),
            dict(filename=TESTING_DIRECTORY + TREE_FILE_TEST, dimensions=5, node_size=4 * 1024, id_size=4,
                 parameters_size=8, tree_depth=3,
                 root_id=5, unique_sequence=testing_unique_sequence, config_hash=testing_config_hash)
    ),
])
def test_write_read_node_two_handler(tree_file_handler_writer_args,
                                     written_nodes: Tuple[RTreeNode, ...],
                                     tree_file_handler_reader_args):
    if os.path.isfile(TESTING_DIRECTORY + TREE_FILE_TEST):
        os.remove(TESTING_DIRECTORY + TREE_FILE_TEST)

    tree_file_handler_writer = TreeFileHandler(**tree_file_handler_writer_args)

    node_ids = []
    for node in written_nodes:
        new_id = tree_file_handler_writer.create_node(node)
        node_ids += [new_id]

    writer_dict = tree_file_handler_writer.__dict__
    del tree_file_handler_writer

    tree_file_handler_reader = TreeFileHandler(**tree_file_handler_reader_args)

    read_nodes_lst = []
    for node_id in node_ids:
        read_node = tree_file_handler_reader.get_node(node_id)
        read_nodes_lst += [read_node]

    read_nodes = tuple(read_nodes_lst)

    for written, read in zip(written_nodes, read_nodes):
        assert written.__dict__ == read.__dict__

    assert compare_tree_file_handler_dictionary(writer_dict, tree_file_handler_reader.__dict__)

    del tree_file_handler_reader

    if os.path.isfile(TESTING_DIRECTORY + TREE_FILE_TEST):
        os.remove(TESTING_DIRECTORY + TREE_FILE_TEST)


@pytest.mark.parametrize('tree_file_handler_args, written_nodes', [
    (
            dict(filename=TESTING_DIRECTORY + TREE_FILE_TEST, dimensions=2, node_size=1024),
            (
                    RTreeNode(node_id=0, parent_id=0, mbb=MBB((MBBDim(1, 1), MBBDim(2, 2))),
                              child_nodes=[5, 5, 2, 2, 2],
                              is_leaf=True),
                    RTreeNode(node_id=1, parent_id=0, mbb=MBB((MBBDim(5, 3), MBBDim(3, 2))), child_nodes=[1, 2],
                              is_leaf=True)
            )
    ),
    (
            dict(filename=TESTING_DIRECTORY + TREE_FILE_TEST),
            (
                    RTreeNode(node_id=0, parent_id=0, mbb=MBB((MBBDim(1, 1), MBBDim(2, 2))), child_nodes=[],
                              is_leaf=False),
                    RTreeNode(node_id=1, parent_id=0, mbb=MBB((MBBDim(5, 4), MBBDim(3, 2))), child_nodes=[1, 2],
                              is_leaf=True)
            )
    ),
    (
            dict(filename=TESTING_DIRECTORY + TREE_FILE_TEST),
            (
                    RTreeNode(node_id=0, parent_id=0, mbb=MBB((MBBDim(1, 1), MBBDim(2, 2))), child_nodes=[],
                              is_leaf=False),
                    RTreeNode(node_id=1, parent_id=0, mbb=MBB((MBBDim(5, 4), MBBDim(3, 2))), child_nodes=[1, 2],
                              is_leaf=True),
                    RTreeNode(node_id=2, parent_id=0, mbb=MBB((MBBDim(7, 6), MBBDim(4, 5))), child_nodes=[3, 4],
                              is_leaf=False),
                    RTreeNode(node_id=3, parent_id=0, mbb=MBB((MBBDim(71, 623), MBBDim(49, 50))),
                              child_nodes=[3, 4, 5, 3, 1],
                              is_leaf=False),
                    RTreeNode(node_id=4, parent_id=0, mbb=MBB((MBBDim(-3, 0), MBBDim(-4, 3434))),
                              child_nodes=[323434, 43434, 5, 3, 1, 343, 1, 2, 3, 33, 123123, 234], is_leaf=False),
            )
    ),
    (
            dict(filename=TESTING_DIRECTORY + TREE_FILE_TEST, dimensions=5, node_size=4 * 1024, id_size=4,
                 parameters_size=8, tree_depth=3,
                 root_id=10, unique_sequence=testing_unique_sequence, config_hash=testing_config_hash),
            (
                    RTreeNode(node_id=0, parent_id=0,
                              mbb=MBB((MBBDim(1, 1), MBBDim(2, 2), MBBDim(3, 3), MBBDim(4, 4), MBBDim(5, 5))),
                              child_nodes=[],
                              is_leaf=False),
                    RTreeNode(node_id=1, parent_id=0,
                              mbb=MBB((MBBDim(9, 8), MBBDim(7, 6), MBBDim(5, 4), MBBDim(3, 2), MBBDim(1, 0))),
                              child_nodes=[1, 2], is_leaf=True)
            )
    ),
])
def test_write_update_nodes_swap(tree_file_handler_args, written_nodes):
    if os.path.isfile(TESTING_DIRECTORY + TREE_FILE_TEST):
        os.remove(TESTING_DIRECTORY + TREE_FILE_TEST)

    tree_file_handler = TreeFileHandler(**tree_file_handler_args)

    node_ids = []
    for node in written_nodes:
        new_id = tree_file_handler.create_node(node)
        node_ids += [new_id]

    read_nodes_lst = []
    for node_id in node_ids:
        read_node = tree_file_handler.get_node(node_id)
        read_nodes_lst += [read_node]

    read_nodes_inverted = tuple(read_nodes_lst[::-1])

    for node_id, updated_node in zip(node_ids, read_nodes_inverted):
        tree_file_handler.update_node(node_id, updated_node)

    updated_nodes_lst = []
    for node_id in node_ids:
        updated_node = tree_file_handler.get_node(node_id)
        updated_nodes_lst += [updated_node]

    updated_nodes = tuple(updated_nodes_lst)

    for written, read in zip(written_nodes[::-1], updated_nodes):
        assert written.mbb == read.mbb
        assert written.child_nodes == read.child_nodes
        assert written.is_leaf == read.is_leaf
        # assert written.__dict__ == read.__dict__

    del tree_file_handler

    if os.path.isfile(TESTING_DIRECTORY + TREE_FILE_TEST):
        os.remove(TESTING_DIRECTORY + TREE_FILE_TEST)


@pytest.mark.parametrize('dimensions, count, low, high, diff', [
    (1, 100, 0, 100, 10),
    (2, 100, 0, 100, 10),
    (2, 100, 0, 100, 0),
    (2, 100, 0, 100, 100),
    (3, 100, 0, 100, 10),
    (10, 100, 0, 100, 10),
    (2, 300, 0, 100, 10),
    (2, 100, -100, 500, 10),
    (3, 1000, -1000, 5000, 10),
])
def test_tree_handler_create_node(dimensions: int, count: int, low: int, high: int, diff: int):
    try:
        os.remove(TESTING_DIRECTORY + TREE_FILE_TEST)
    except FileNotFoundError:
        pass

    tree_handler = TreeFileHandler(filename=TESTING_DIRECTORY + TREE_FILE_TEST,
                                   dimensions=dimensions)
    total_insert_count = 0

    for c in range(0, count):
        total_insert_count += 1

        box = []
        for _ in range(dimensions):
            lower = random.randint(low, high)
            higher = lower + random.randint(0, diff)
            box.append(MBBDim(lower, higher))
        print(box)
        mbb = MBB(tuple(box))

        child_nodes = [random.randint(0, 2 ** (tree_handler.id_size * 8) / 2) for _ in
                       range(0, random.randint(0, tree_handler.children_per_node))]
        is_leaf = bool(random.getrandbits(1))

        node_id = tree_handler.create_node(RTreeNode(mbb=mbb, parent_id=c, child_nodes=child_nodes, is_leaf=is_leaf))
        found_node = tree_handler.get_node(node_id)

        if found_node is None:
            print(f"c:{c} missing: {box}")
            # visualize(tree, show_mbbs_only=False, open_img=True)
        assert found_node is not None
        assert found_node.mbb == mbb
        assert found_node.id == node_id
        assert found_node.child_nodes == child_nodes
        assert found_node.parent_id == c
        # assert found_node.is_leaf == node_id


@pytest.mark.parametrize('dimensions, count, low, high, diff', [
    (1, 100, 0, 100, 10),
    (2, 10, 0, 100, 5),
    (2, 100, 0, 100, 10),
    (2, 100, 0, 100, 0),
    (2, 100, 0, 100, 100),
    (3, 100, 0, 100, 10),
    (10, 100, 0, 100, 10),
    (2, 300, 0, 100, 10),
    (2, 100, -100, 500, 10),
    (3, 1000, -1000, 5000, 1000),
])
def test_tree_handler_update_node(dimensions: int, count: int, low: int, high: int, diff: int):
    try:
        os.remove(TESTING_DIRECTORY + TREE_FILE_TEST)
    except FileNotFoundError:
        pass

    tree_handler = TreeFileHandler(filename=TESTING_DIRECTORY + TREE_FILE_TEST,
                                   dimensions=dimensions, )
    total_insert_count = 0

    for c in range(0, count):

        try:
            total_insert_count += 1

            box = []
            for _ in range(dimensions):
                lower = random.randint(low, high)
                higher = lower + random.randint(0, diff)
                box.append(MBBDim(lower, higher))
            mbb = MBB(tuple(box))
            print(mbb)

            child_nodes = [random.randint(0, 2 ** (tree_handler.id_size * 8) / 2) for _ in
                           range(0, random.randint(0, tree_handler.children_per_node))]

            is_leaf = bool(random.getrandbits(1))

            node_id = tree_handler.create_node(
                RTreeNode(mbb=mbb, parent_id=c, child_nodes=child_nodes, is_leaf=is_leaf))
            found_node = tree_handler.get_node(node_id)

            if found_node is None:
                print(f"c:{c} missing: {box}")
                # visualize(tree, show_mbbs_only=False, open_img=True)
            assert found_node is not None
            # assert found_node.mbb == mbb
            assert found_node.id == node_id
            assert found_node.child_nodes == child_nodes
            assert found_node.parent_id == c
            assert found_node.is_leaf == is_leaf
        except Exception as e:
            raise e

        ##########################################update random node##########################################

        if c < 10:
            continue

        rnd_node_id = random.randint(0, tree_handler.highest_id)
        rnd_node = tree_handler.get_node(rnd_node_id)
        if rnd_node is None:
            print(f"c:{c} rand_node not found id: {rnd_node_id}")
            raise Exception("rnd_node cannot be None")

        print("rnd_node.__dict__")
        print(rnd_node.__dict__)

        new_box: List[MBBDim] = []
        for _ in range(dimensions):
            lower = random.randint(low, high)
            higher = lower + random.randint(0, diff)
            new_box.append(MBBDim(lower, higher))
        print(new_box)
        rnd_node.mbb = MBB(tuple(new_box))

        rnd_node.child_nodes = [random.randint(0, 2 ** (tree_handler.id_size * 7)) for _ in
                                range(0, random.randint(0, tree_handler.children_per_node))]
        rnd_node.is_leaf = bool(random.getrandbits(1))
        rnd_node.parent_id = random.randint(0, 1000000000)
        updated_node_id = tree_handler.update_node(rnd_node_id, rnd_node)
        updated_rnd_node = tree_handler.get_node(updated_node_id)

        if updated_rnd_node is None:
            print(f"c:{c} rand_node not found id: {updated_node_id}")
            # visualize(tree, show_mbbs_only=False, open_img=True)
        assert updated_rnd_node is not None
        assert updated_rnd_node.mbb.box == rnd_node.mbb.box
        assert updated_rnd_node.id == rnd_node.id == rnd_node_id
        assert updated_rnd_node.child_nodes == rnd_node.child_nodes
        assert updated_rnd_node.parent_id == rnd_node.parent_id
        assert updated_rnd_node.is_leaf == rnd_node.is_leaf
