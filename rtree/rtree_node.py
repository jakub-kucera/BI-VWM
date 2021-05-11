from __future__ import annotations

from typing import List, Tuple, Optional

from rtree.database_entry import DatabaseEntry
from rtree.mbb import MBBDim, MBB


class RTreeNode:
    """Class represents a single node in the R-tree structure"""
    max_entries_count = 0

    @staticmethod
    def create_emtpy_node(dimensions: int, is_leaf: bool) -> RTreeNode:
        return RTreeNode(MBB(tuple(MBBDim(0, 0) for dim in range(dimensions))),
                         is_leaf=is_leaf)

    def __init__(self, mbb: MBB, node_id: Optional[int] = None, entry_ids: List[int] = None, is_leaf: bool = False):
        if entry_ids is None:
            entry_ids = []

        if len(entry_ids) > self.max_entries_count:
            raise ValueError(f"Node cannot have {len(entry_ids)} entries, maximum allowed is: {self.max_entries_count}")

        self.mbb = mbb
        self.id = node_id
        self.entries = entry_ids
        self.is_leaf = is_leaf

    def __str__(self):
        return str(self.__dict__)

    def contains_inner(self, inner: RTreeNode):
        return self.mbb.contains_inner(inner.mbb)

    def overlaps(self, inner: RTreeNode):
        return self.mbb.overlaps(inner.mbb)

    def insert_node_from_box(self, new_node_id: int, new_box: Tuple[MBBDim, ...]) -> bool:
        if self.is_leaf:
            raise Exception("Cannot insert child node into leaf node.")

        if len(self.mbb.box) != len(new_box):
            raise ValueError(f"new_entry has size of {len(new_box)}, but it should be {len(self.mbb.box)}")

        if new_node_id in self.entries:
            raise ValueError(f"Entry with ID={new_node_id} is already in node entries")

        if len(self.entries) + 1 > self.max_entries_count:
            return False

        self.entries.append(new_node_id)
        self.mbb.insert_entry(new_box)

        return True

    def insert_node_from_node(self, new_node_id: int, new_node: RTreeNode) -> bool:
        return self.insert_node_from_box(new_node_id, new_node.mbb.box)

    def insert_entry_from_entry(self, new_entry_position: int, new_entry: DatabaseEntry) -> bool:
        if not self.is_leaf:
            raise Exception("Cannot insert database entry into non-leaf node.")

        # new_mbb = MBB.create_box_from_entry_list(entry.coordinates)
        new_box = tuple(MBBDim(coords, coords) for coords in new_entry.coordinates)

        if len(self.mbb.box) != len(new_box):
            raise ValueError(f"new_entry has size of {len(new_box)}, but it should be {len(self.mbb.box)}")

        if new_entry_position in self.entries:
            raise ValueError(f"Entry at position {new_entry_position} is already in node entries")

        if len(self.entries) + 1 > self.max_entries_count:
            return False

        self.entries.append(new_entry_position)
        self.mbb.insert_entry(new_box)

        return True
