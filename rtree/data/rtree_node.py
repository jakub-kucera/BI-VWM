from __future__ import annotations

from typing import List, Tuple, Optional

from rtree.data.database_entry import DatabaseEntry
from rtree.data.mbb import MBBDim, MBB
from rtree.default_config import MINIMUM_NODE_FILL


class RTreeNode:
    """Class represents a single node in the R-tree structure"""
    max_entries_count = 0

    @staticmethod
    def create_empty_node(dimensions: int, is_leaf: bool, parent_id: int = None) -> RTreeNode:
        return RTreeNode(parent_id=parent_id, mbb=MBB(tuple(MBBDim(0, 0) for dim in range(dimensions))),
                         is_leaf=is_leaf)

    def __init__(self, mbb: MBB, node_id: Optional[int] = None, parent_id: Optional[int] = None,
                 child_nodes: List[int] = None, is_leaf: bool = False):
        if child_nodes is None:
            child_nodes = []

        if len(child_nodes) > self.max_entries_count:
            raise ValueError(
                f"Node cannot have {len(child_nodes)} entries, maximum allowed is: {self.max_entries_count}")

        self.mbb = mbb
        self.id = node_id
        self.child_nodes = child_nodes
        self.is_leaf = is_leaf
        self.parent_id = parent_id

    def __str__(self):
        return str(self.__dict__)

    def child_count(self) -> int:
        return len(self.child_nodes)

    def is_full(self) -> bool:
        return self.child_count() >= self.max_entries_count

    def has_over_balance(self) -> bool:
        return self.child_count() >= ((1 - MINIMUM_NODE_FILL) * self.max_entries_count)

    def contains_inner(self, inner: RTreeNode):
        return self.mbb.contains_inner(inner.mbb)

    def overlaps(self, inner: RTreeNode):
        return self.mbb.overlaps(inner.mbb)

    def insert_box(self, new_node_id: int, new_box: Tuple[MBBDim, ...]) -> bool:
        # if self.is_leaf:
        #     raise Exception("Cannot insert child node into leaf node.")

        if len(self.mbb.box) != len(new_box):
            raise ValueError(f"new_entry has size of {len(new_box)}, but it should be {len(self.mbb.box)}")

        if new_node_id in self.child_nodes:
            # raise ValueError(f"Entry with ID={new_node_id} is already in node entries")
            pass
        else:
            self.child_nodes.append(new_node_id)

        # if len(self.child_nodes) + 1 > self.max_entries_count:
        #     return False
        self.mbb.insert_mbb(new_box)

        return True

    # def insert_node_from_node(self, new_node_id: int, new_node: RTreeNode) -> bool:
    #     return self.insert_box(new_node_id, new_node.mbb.box)

    def insert_entry_from_entry(self, new_entry_position: int, new_entry: DatabaseEntry) -> bool:
        if not self.is_leaf:
            raise Exception("Cannot insert database entry into non-leaf node.")

        # new_mbb = MBB.create_box_from_entry_list(entry.coordinates)
        new_box = tuple(MBBDim(coords, coords) for coords in new_entry.coordinates)

        if len(self.mbb.box) != len(new_box):
            raise ValueError(f"new_entry has size of {len(new_box)}, but it should be {len(self.mbb.box)}")

        if new_entry_position in self.child_nodes:
            raise ValueError(f"Entry at position {new_entry_position} is already in node entries")

        if len(self.child_nodes) + 1 > self.max_entries_count:
            return False

        self.child_nodes.append(new_entry_position)
        self.mbb.insert_mbb(new_box)

        return True

    def get_seed_split_nodes(self) -> Tuple[RTreeNode, RTreeNode]:
        new_mbb_1: List[MBBDim] = []
        new_mbb_2: List[MBBDim] = []

        for dim in self.mbb.box:
            new_mbb_1.append(MBBDim(dim.low, dim.low))
            new_mbb_2.append(MBBDim(dim.high, dim.high))

        return RTreeNode(mbb=MBB(tuple(new_mbb_1)), parent_id=self.parent_id, is_leaf=self.is_leaf), \
               RTreeNode(mbb=MBB(tuple(new_mbb_2)), parent_id=self.parent_id, is_leaf=self.is_leaf)
