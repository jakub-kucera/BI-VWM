import os
import secrets
from typing import Tuple

import pytest

from rtree.node import Node, MBB, MBBDim
from rtree.tree_file_handler import TreeFileHandler
from rtree.default_config import *

# testing_unique_sequence = b"\x00\xb0\x83\x82\x8c\x05\x7b\xc3\x38\x69\xff\xde\xed\xba\xd6\x02\x69\xd2\x80\x94"
# testing_config_hash = b"\x4e\x9a\xe1\xf1\xe8\x91\xe9\x41\x95\x64\x29\x89\x88\x86\x53\xf1\x22\xa9\xde\xb9"
testing_unique_sequence = secrets.token_bytes(UNIQUE_SEQUENCE_LENGTH)
testing_config_hash = secrets.token_bytes(CONFIG_HASH_LENGTH)

Node.max_entries_count = 50


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
    {"trunk_id": -5},
    {"trunk_id": 100000000000000000000},
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
        tree_handler = TreeFileHandler(filename=TESTING_DIRECTORY + TREE_FILE_TEST,
                                       **rtree_args)
        del tree_handler

    try:
        os.remove(TESTING_DIRECTORY + TREE_FILE_TEST)
    except FileNotFoundError:
        pass


@pytest.mark.parametrize('tree_file_handler_args, written_nodes', [
    (
            dict(filename=TREE_FILE_TEST, dimensions=2, node_size=1024),
            (
                    Node(node_id=0, mbb=MBB((MBBDim(1, 1), MBBDim(2, 2))), entry_ids=[5, 5, 2, 2, 2], is_leaf=True),
                    Node(node_id=1,mbb=MBB((MBBDim(5, 3), MBBDim(3, 2))), entry_ids=[1, 2], is_leaf=True)
            )
    ),
    (
            dict(filename=TREE_FILE_TEST),
            (
                    Node(node_id=0, mbb=MBB((MBBDim(1, 1), MBBDim(2, 2))), entry_ids=[], is_leaf=False),
                    Node(node_id=1, mbb=MBB((MBBDim(5, 4), MBBDim(3, 2))), entry_ids=[1, 2], is_leaf=True)
            )
    ),
    (
            dict(filename=TREE_FILE_TEST, dimensions=5, node_size=4 * 1024, id_size=4, parameters_size=8, tree_depth=3,
                 trunk_id=10, unique_sequence=testing_unique_sequence, config_hash=testing_config_hash),
            (
                    Node(node_id=0, mbb=MBB((MBBDim(1, 1), MBBDim(2, 2), MBBDim(3, 3), MBBDim(4, 4), MBBDim(5, 5))), entry_ids=[],
                         is_leaf=False),
                    Node(node_id=1, mbb=MBB((MBBDim(9, 8), MBBDim(7, 6), MBBDim(5, 4), MBBDim(3, 2), MBBDim(1, 0))),
                         entry_ids=[1, 2], is_leaf=True)
            )
    ),
])
def test_write_read_node_one_handler(tree_file_handler_args, written_nodes):
    if os.path.isfile(TREE_FILE_TEST):
        os.remove(TREE_FILE_TEST)

    tree_file_handler = TreeFileHandler(**tree_file_handler_args)

    node_ids = []
    for node in written_nodes:
        new_id = tree_file_handler.insert_node(node)
        node_ids += [new_id]

    read_nodes_lst = []
    for node_id in node_ids:
        read_node = tree_file_handler.get_node(node_id)
        read_nodes_lst += [read_node]

    read_nodes = tuple(read_nodes_lst)

    for written, read in zip(written_nodes, read_nodes):
        assert written.__dict__ == read.__dict__

    del tree_file_handler

    if os.path.isfile(TREE_FILE_TEST):
        os.remove(TREE_FILE_TEST)


@pytest.mark.parametrize('tree_file_handler_writer_args, written_nodes, tree_file_handler_reader_args', [
    (
            dict(filename=TREE_FILE_TEST),
            (
                    Node(node_id=0, mbb=MBB((MBBDim(1, 1), MBBDim(2, 2))), entry_ids=[], is_leaf=False),
                    Node(node_id=1, mbb=MBB((MBBDim(5, 4), MBBDim(3, 2))), entry_ids=[1, 2], is_leaf=True)
            ),
            dict(filename=TREE_FILE_TEST)
    ),
    (
            dict(filename=TREE_FILE_TEST, dimensions=5, node_size=4 * 1024, id_size=4, parameters_size=8, tree_depth=3,
                 trunk_id=5, unique_sequence=testing_unique_sequence, config_hash=testing_config_hash),
            (
                    Node(node_id=0, mbb=MBB((MBBDim(1, 1), MBBDim(2, 2), MBBDim(3, 3), MBBDim(4, 4), MBBDim(5, 5))), entry_ids=[],
                         is_leaf=False),
                    Node(node_id=1, mbb=MBB((MBBDim(9, 8), MBBDim(7, 6), MBBDim(5, 4), MBBDim(3, 2), MBBDim(1, 0))),
                         entry_ids=[1, 2], is_leaf=True)
            ),
            dict(filename=TREE_FILE_TEST, dimensions=5, node_size=4 * 1024, id_size=4, parameters_size=8, tree_depth=3,
                 trunk_id=5, unique_sequence=testing_unique_sequence, config_hash=testing_config_hash)
    ),
])
def test_write_read_node_two_handler(tree_file_handler_writer_args,
                                     written_nodes: Tuple[Node, ...],
                                     tree_file_handler_reader_args):
    if os.path.isfile(TREE_FILE_TEST):
        os.remove(TREE_FILE_TEST)

    tree_file_handler_writer = TreeFileHandler(**tree_file_handler_writer_args)

    node_ids = []
    for node in written_nodes:
        new_id = tree_file_handler_writer.insert_node(node)
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

    if os.path.isfile(TREE_FILE_TEST):
        os.remove(TREE_FILE_TEST)


@pytest.mark.parametrize('tree_file_handler_args, written_nodes', [
    (
            dict(filename=TREE_FILE_TEST, dimensions=2, node_size=1024),
            (
                    Node(node_id=0, mbb=MBB((MBBDim(1, 1), MBBDim(2, 2))), entry_ids=[5, 5, 2, 2, 2], is_leaf=True),
                    Node(node_id=1, mbb=MBB((MBBDim(5, 3), MBBDim(3, 2))), entry_ids=[1, 2], is_leaf=True)
            )
    ),
    (
            dict(filename=TREE_FILE_TEST),
            (
                    Node(node_id=0, mbb=MBB((MBBDim(1, 1), MBBDim(2, 2))), entry_ids=[], is_leaf=False),
                    Node(node_id=1, mbb=MBB((MBBDim(5, 4), MBBDim(3, 2))), entry_ids=[1, 2], is_leaf=True)
            )
    ),
    (
            dict(filename=TREE_FILE_TEST),
            (
                    Node(node_id=0, mbb=MBB((MBBDim(1, 1), MBBDim(2, 2))), entry_ids=[], is_leaf=False),
                    Node(node_id=1, mbb=MBB((MBBDim(5, 4), MBBDim(3, 2))), entry_ids=[1, 2], is_leaf=True),
                    Node(node_id=2, mbb=MBB((MBBDim(7, 6), MBBDim(4, 5))), entry_ids=[3, 4], is_leaf=False),
                    Node(node_id=3, mbb=MBB((MBBDim(71, 623), MBBDim(49, 50))), entry_ids=[3, 4, 5, 3, 1], is_leaf=False),
                    Node(node_id=4, mbb=MBB((MBBDim(-3, 0), MBBDim(-4, 3434))),
                         entry_ids=[323434, 43434, 5, 3, 1, 343, 1, 2, 3, 33, 123123, 234], is_leaf=False),
            )
    ),
    (
            dict(filename=TREE_FILE_TEST, dimensions=5, node_size=4 * 1024, id_size=4, parameters_size=8, tree_depth=3,
                 trunk_id=10, unique_sequence=testing_unique_sequence, config_hash=testing_config_hash),
            (
                    Node(node_id=0, mbb=MBB((MBBDim(1, 1), MBBDim(2, 2), MBBDim(3, 3), MBBDim(4, 4), MBBDim(5, 5))), entry_ids=[],
                         is_leaf=False),
                    Node(node_id=1, mbb=MBB((MBBDim(9, 8), MBBDim(7, 6), MBBDim(5, 4), MBBDim(3, 2), MBBDim(1, 0))),
                         entry_ids=[1, 2], is_leaf=True)
            )
    ),
])
def test_write_update_nodes_swap(tree_file_handler_args, written_nodes):
    if os.path.isfile(TREE_FILE_TEST):
        os.remove(TREE_FILE_TEST)

    tree_file_handler = TreeFileHandler(**tree_file_handler_args)

    node_ids = []
    for node in written_nodes:
        new_id = tree_file_handler.insert_node(node)
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
        assert written.__dict__ == read.__dict__

    del tree_file_handler

    if os.path.isfile(TREE_FILE_TEST):
        os.remove(TREE_FILE_TEST)
