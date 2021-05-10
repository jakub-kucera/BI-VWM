import os
from math import floor
from typing import Optional

from rtree.node import Node
from rtree.default_config import *


class Cache:
    def __init__(self,
                 node_size: int = DEFAULT_NODE_SIZE,
                 cache_nodes: int = CACHE_NODES):
        self.node_size = node_size
        self.cache_nodes = cache_nodes

        self.cache_size = self.cache_nodes * self.node_size
        self.memory = [None] * self.cache_size

    def __str__(self):
        return str(self.__dict__)

    def __hashIn(self, data: int) -> int:
        return data % self.cache_size

    def __is_free(self, node_id: int) -> bool:
        hashed = self.__hashIn(node_id)
        return self.memory[hashed] == None

    def search(self, node_id: int) -> Optional[Node]:
        if __is_free(node_id):
            return None
        hashed = self.__hashIn(node_id)
        node = self.memory[hashed]
        return node if node.id == node_id else None

    def store(self, new_node: Node):
        hashed = self.__hashIn(new_node.id)
        self.memory[hashed] = new_node
