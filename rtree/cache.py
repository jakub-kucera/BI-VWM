from typing import Optional, List

from rtree.data.rtree_node import RTreeNode
from rtree.default_config import *


class Cache:
    def __init__(self,
                 node_size: int = DEFAULT_NODE_SIZE,
                 cache_nodes: int = CACHE_NODES):
        self.node_size = node_size
        self.cache_nodes = cache_nodes

        self.cache_size = self.cache_nodes * self.node_size
        self.memory: List[Optional[RTreeNode]] = [None] * self.cache_size

    def __str__(self):
        return str(self.__dict__)

    def __hash_in(self, data: int) -> int:
        return data % self.cache_size

    # def __is_free(self, node_id: int) -> bool:
    #     hashed = self.__hashIn(node_id)
    #     return self.memory[hashed] is None
    #
    # def search(self, node_id: int) -> Optional[Node]:
    #     if self.__is_free(node_id):
    #         return None
    #     hashed = self.__hashIn(node_id)
    #     node = self.memory[hashed]
    #     return node if node.id == node_id else None

    def search(self, node_id: int) -> Optional[RTreeNode]:
        hashed = self.__hash_in(node_id)
        cache_for_hashed = self.memory[hashed]

        if cache_for_hashed is None:
            return None

        return cache_for_hashed if cache_for_hashed.id == node_id else None

    def store(self, new_node: RTreeNode):
        if new_node.id is None:
            raise Exception("Cannot call cache store on Node with no ID")

        hashed = self.__hash_in(new_node.id)
        self.memory[hashed] = new_node
