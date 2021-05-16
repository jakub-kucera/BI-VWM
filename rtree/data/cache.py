from math import floor
from typing import Optional, List
from rtree.default_config import *
from rtree.data.rtree_node import RTreeNode


class Cache:
    def __init__(self,
                 child_size: int,
                 node_size: int = DEFAULT_NODE_SIZE,
                 cache_memory: int = CACHE_MEMORY_SIZE):
        self.child_size = child_size
        self.node_size = node_size
        self.cache_memory = cache_memory

        self.cache_size = floor(self.cache_memory / self.node_size)

        # make sure the first tree level (root and its children) is always cached
        self.memory_permanent: List[Optional[RTreeNode]] = [None] * (self.child_size + 1)

        # other levels are free to rewrite at any point
        self.memory_variable: List[Optional[RTreeNode]] = [None] * self.cache_size

    def __str__(self):
        return str(self.__dict__)

    def __hash_per(self, data: int) -> int:
        return data % self.child_size

    def __hash_var(self, data: int) -> int:
        return data % self.cache_size

    def search(self, node_id: int, permanent: bool = False) -> Optional[RTreeNode]:
        cached_node: Optional[RTreeNode] = None

        if permanent:
            hashed = self.__hash_var(node_id)
            cached_node = self.memory_permanent[hashed]
        else:
            hashed = self.__hash_var(node_id)
            cached_node = self.memory_variable[hashed]

        if cached_node is None:
            return None

        return cached_node if cached_node.id == node_id else None

    def store(self, new_node: RTreeNode, permanent: bool = False):
        if new_node is None or new_node.id is None:
            raise Exception("Cannot call cache store on Node with no ID")

        if permanent:
            hashed = self.__hash_per(new_node.id)
            self.memory_permanent[hashed] = new_node
        else:
            hashed = self.__hash_var(new_node.id)
            self.memory_variable[hashed] = new_node
