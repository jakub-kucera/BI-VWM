import os
from unittest import TestCase

from rtree.node import Node
from rtree.tree_file_handler import TreeFileHandler

TREE_FILE_TEST = "testingTree.bin"


#
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
    def test_write_read_node_one_handler_instance(self):
        os.remove(TREE_FILE_TEST)
        tree_file_handler_writer = TreeFileHandler(TREE_FILE_TEST, 2, 1024)
        written_node_1 = Node(False, [[1, 1], [2, 2]], [5, 5, 2, 2, 2])
        written_node_2 = Node(True, [[5, 4], [3, 2]], [1, 2])
        id1 = tree_file_handler_writer.write_node(written_node_1)
        id2 = tree_file_handler_writer.write_node(written_node_2)
        tree_file_handler_writer.__del__()

        tree_file_handler_writer = TreeFileHandler(TREE_FILE_TEST, 2, 1024)

        read_node1 = tree_file_handler_writer.get_node(id1)
        read_node2 = tree_file_handler_writer.get_node(id2)

        print(written_node_1)
        print(read_node1)

        assert written_node_1.__dict__ == read_node1.__dict__
        assert written_node_2.__dict__ == read_node2.__dict__

    def test_write_read_node_two_handler_instances(self):
        os.remove(TREE_FILE_TEST)
        tree_file_handler_writer = TreeFileHandler(TREE_FILE_TEST, 2, 1024)
        written_node_1 = Node(False, [[1, 1], [2, 2]], [])
        written_node_2 = Node(True, [[5, 4], [3, 2]], [1, 2])
        id1 = tree_file_handler_writer.write_node(written_node_1)
        id2 = tree_file_handler_writer.write_node(written_node_2)
        tree_file_handler_writer.__del__()

        tree_file_handler_reader = TreeFileHandler(TREE_FILE_TEST, 2, 1024)

        read_node1 = tree_file_handler_reader.get_node(id1)
        read_node2 = tree_file_handler_reader.get_node(id2)

        print(written_node_1)
        print(read_node1)

        assert written_node_1.__dict__ == read_node1.__dict__
        assert written_node_2.__dict__ == read_node2.__dict__
