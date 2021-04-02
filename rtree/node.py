from typing import List


class Node:
    def __init__(self, is_leaf: bool = False, rectangle: List[List[int]] = [[]], children: List[int] = []):
        self.is_leaf = is_leaf
        self.coordinates_rectangle = rectangle
        self.children = children  # set max size or check from rtree?

    def __str__(self):
        return str(self.__dict__)
