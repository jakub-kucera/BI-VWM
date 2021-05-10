from __future__ import annotations

from typing import List, Tuple

from rtree.mbb import MBBDim, MBB


class Node:
    """Class represents a single node in the R-tree structure"""
    max_entries_count = 0

    def __init__(self, mbb: MBB, entry_ids: List[int] = None, is_leaf: bool = False):
        if entry_ids is None:
            entry_ids = []

        if len(entry_ids) > self.max_entries_count:
            raise ValueError(f"Node cannot have {len(entry_ids)} entries, maximum allowed is: {self.max_entries_count}")

        self.mbb = mbb
        self.entries = entry_ids
        self.is_leaf = is_leaf
        self.id = None

    def __str__(self):
        return str(self.__dict__)

    def contains_inner(self, inner: Node):
        return self.mbb.contains_inner(inner.mbb)

    def overlaps(self, inner: Node):
        return self.mbb.overlaps(inner.mbb)

    def insert_entry_from_box(self, new_id: int, new_box: Tuple[MBBDim, ...]) -> bool:
        if len(self.mbb.box) != len(new_box):
            raise ValueError(f"new_entry has size of {len(new_box)}, but it should be {len(self.mbb.box)}")

        if new_id in self.entries:
            raise ValueError(f"Entry with ID={new_id} is already in node entries")

        if len(self.entries) + 1 > self.max_entries_count:
            return False

        self.entries += [new_id]
        self.mbb.insert_entry(new_box)

        return True

    def insert_entry_from_node(self, new_entry_id, new_entry_node: Node) -> bool:
        return self.insert_entry_from_box(new_entry_id, new_entry_node.mbb.box)
