import os
from unittest import TestCase

from rtree.node import Node
from rtree.tree_file_handler import TreeFileHandler
from rtree.default_config import *


# def test_tree_file_handler_write(): # move to separate file
#     os.remove(TREE_FILE_TEST)
#     tree_file_handler = TreeFileHandler(TREE_FILE_TEST, 2, 1024)
#     new_node1 = Node(False, [[1, 1], [2, 2]], [])
#     print(new_node1)
#     new_node2 = Node(True, [[5, 4], [3, 2]], [1, 2])
#     print(new_node2)
#     print(tree_file_handler)
#     print(tree_file_handler.write_node(new_node1))
#     print(tree_file_handler.write_node(new_node2))
#     print(tree_file_handler)
#
# def test_tree_file_handler_read():  # move to separate file
#     tree_file_handler = TreeFileHandler(TREE_FILE_TEST, 2, 1024)
#     print(tree_file_handler)
#     # tree_file_handler.write_node(new_node)
#     i = 0
#     while i < 2:
#         node_from_file = tree_file_handler.get_node(i)
#         i += 1
#         print(tree_file_handler)
#         print(node_from_file)
#         if node_from_file is None:
#             break

class TestTreeFileHandler(TestCase):
    def compare_tree_file_handler_dictionary(self, a, b):
        return (a['filename'] == b['filename'] and
                a['dimensions'] == b['dimensions'] and
                a['node_size'] == b['node_size'] and
                a['parameter_record_size'] == b['parameter_record_size'] and
                a['node_id_size'] == b['node_id_size'] and
                a['null_node_id'] == b['null_node_id'] and
                a['node_flag_size'] == b['node_flag_size'] and
                a['highest_id'] == b['highest_id'] and
                a['children_per_node'] == b['children_per_node'] and
                a['node_padding'] == b['node_padding'] and
                a['filesize'] == b['filesize'])  # not current position

    def test_write_read_node_one_handler_instance_default_args(self):
        print("TEST test_write_read_node_one_handler_instance_default_args: ") if PRINT_OUTPUT_TEST else None
        if os.path.isfile(TREE_FILE_TEST):
            os.remove(TREE_FILE_TEST)

        tree_file_handler_writer = TreeFileHandler(TREE_FILE_TEST, 2, 1024)
        written_node_1 = Node(False, [[1, 1], [2, 2]], [5, 5, 2, 2, 2])
        written_node_2 = Node(True, [[5, 4], [3, 2]], [1, 2])
        id1 = tree_file_handler_writer.insert_node(written_node_1)
        id2 = tree_file_handler_writer.insert_node(written_node_2)

        read_node1 = tree_file_handler_writer.get_node(id1)
        read_node2 = tree_file_handler_writer.get_node(id2)

        print(written_node_1) if PRINT_OUTPUT_TEST else None
        print(read_node1) if PRINT_OUTPUT_TEST else None
        print(written_node_2) if PRINT_OUTPUT_TEST else None
        print(read_node2) if PRINT_OUTPUT_TEST else None

        assert written_node_1.__dict__ == read_node1.__dict__
        assert written_node_2.__dict__ == read_node2.__dict__

    def test_write_read_node_two_handler_instances_default_args(self):
        print("TEST test_write_read_node_two_handler_instances_default_args") if PRINT_OUTPUT_TEST else None
        print("Writing: ") if PRINT_OUTPUT_TEST else None
        if os.path.isfile(TREE_FILE_TEST):
            os.remove(TREE_FILE_TEST)

        tree_file_handler_writer = TreeFileHandler(TREE_FILE_TEST)
        print(tree_file_handler_writer) if PRINT_OUTPUT_TEST else None
        written_node_1 = Node(False, [[1, 1], [2, 2]], [])
        written_node_2 = Node(True, [[5, 4], [3, 2]], [1, 2])
        id1 = tree_file_handler_writer.insert_node(written_node_1)
        id2 = tree_file_handler_writer.insert_node(written_node_2)

        print(tree_file_handler_writer) if PRINT_OUTPUT_TEST else None
        del tree_file_handler_writer

        print("Reading: ") if PRINT_OUTPUT_TEST else None
        tree_file_handler_reader = TreeFileHandler(TREE_FILE_TEST)
        print(tree_file_handler_reader) if PRINT_OUTPUT_TEST else None

        read_node1 = tree_file_handler_reader.get_node(id1)
        read_node2 = tree_file_handler_reader.get_node(id2)
        print(tree_file_handler_reader) if PRINT_OUTPUT_TEST else None

        print(written_node_1) if PRINT_OUTPUT_TEST else None
        print(read_node1) if PRINT_OUTPUT_TEST else None
        print(written_node_2) if PRINT_OUTPUT_TEST else None
        print(read_node2) if PRINT_OUTPUT_TEST else None

        assert written_node_1.__dict__ == read_node1.__dict__
        assert written_node_2.__dict__ == read_node2.__dict__

    def test_write_read_node_one_handler_instance_not_default_args(self):
        print("TEST test_write_read_node_one_handler_instance_not_default_args: ") if PRINT_OUTPUT_TEST else None
        if os.path.isfile(TREE_FILE_TEST):
            os.remove(TREE_FILE_TEST)

        tree_file_handler_writer = TreeFileHandler(TREE_FILE_TEST, 5, 4 * 1024)
        written_node_1 = Node(False, [[1, 1], [2, 2], [3, 3], [4, 4], [5, 5]], [5, 5, 2, 2, 2])
        written_node_2 = Node(True, [[9, 8], [7, 6], [5, 4], [3, 2], [1, 0]], [1, 2])
        id1 = tree_file_handler_writer.insert_node(written_node_1)
        id2 = tree_file_handler_writer.insert_node(written_node_2)

        read_node1 = tree_file_handler_writer.get_node(id1)
        read_node2 = tree_file_handler_writer.get_node(id2)

        print(written_node_1) if PRINT_OUTPUT_TEST else None
        print(read_node1) if PRINT_OUTPUT_TEST else None
        print(written_node_2) if PRINT_OUTPUT_TEST else None
        print(read_node2) if PRINT_OUTPUT_TEST else None

        assert written_node_1.__dict__ == read_node1.__dict__
        assert written_node_2.__dict__ == read_node2.__dict__

    def test_write_read_node_two_handler_instances_not_default_args(self):
        print("TEST test_write_read_node_two_handler_instances_not_default_args") if PRINT_OUTPUT_TEST else None
        print("Writing: ") if PRINT_OUTPUT_TEST else None
        if os.path.isfile(TREE_FILE_TEST):
            os.remove(TREE_FILE_TEST)

        tree_file_handler_writer = TreeFileHandler(TREE_FILE_TEST, 5, 4 * 1024)
        print(tree_file_handler_writer) if PRINT_OUTPUT_TEST else None
        written_node_1 = Node(False, [[1, 1], [2, 2], [3, 3], [4, 4], [5, 5]], [])
        written_node_2 = Node(True, [[9, 8], [7, 6], [5, 4], [3, 2], [1, 0]], [1, 2])
        id1 = tree_file_handler_writer.insert_node(written_node_1)
        id2 = tree_file_handler_writer.insert_node(written_node_2)

        print(tree_file_handler_writer) if PRINT_OUTPUT_TEST else None
        writer_dict = tree_file_handler_writer.__dict__
        # print(writer_dict) if PRINT_OUTPUT_TEST else None
        del tree_file_handler_writer

        print("Reading: ") if PRINT_OUTPUT_TEST else None
        tree_file_handler_reader = TreeFileHandler(TREE_FILE_TEST, 5, 4 * 1024)
        print(tree_file_handler_reader) if PRINT_OUTPUT_TEST else None

        read_node1 = tree_file_handler_reader.get_node(id1)
        read_node2 = tree_file_handler_reader.get_node(id2)
        print(tree_file_handler_reader) if PRINT_OUTPUT_TEST else None

        print(written_node_1) if PRINT_OUTPUT_TEST else None
        print(read_node1) if PRINT_OUTPUT_TEST else None
        print(written_node_2) if PRINT_OUTPUT_TEST else None
        print(read_node2) if PRINT_OUTPUT_TEST else None

        assert written_node_2.__dict__ == read_node2.__dict__
        assert written_node_2.__dict__ == read_node2.__dict__
        print(writer_dict) if PRINT_OUTPUT_TEST else None
        print(tree_file_handler_reader) if PRINT_OUTPUT_TEST else None
        assert self.compare_tree_file_handler_dictionary(writer_dict, tree_file_handler_reader.__dict__)
        # assert writer_dict == tree_file_handler_reader.__dict__
        # tree_file_handler_reader.__del__()
