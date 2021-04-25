"""Includes class for MMB and its helper class MMBDim which represents one dimension"""
from __future__ import annotations

from typing import Tuple


class MBBDim:
    """Class represents a single dimension in a MMB."""

    def __init__(self, low: int, high: int):
        if low < high:
            self.low = low
            self.high = high
        else:
            self.low = high
            self.high = low

    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    def get_diff(self):
        return self.high - self.low

    def contains(self, inner: MBBDim):
        return inner.low >= self.low and inner.high <= self.high

    def overlaps(self, other: MBBDim):
        return (other.low <= self.low <= other.high) \
               or (other.low <= self.high <= other.high) \
               or (self.low <= other.low <= self.high) \
               or (self.low <= other.high <= self.high)


class MBB:
    """Class represents a minimal bounding box. Its a 'box' containing child nodes"""

    @staticmethod
    def get_size(box: Tuple[MBBDim, ...]):
        """Calculates the size of volume of MBB"""
        # maybe use different metric
        size = 1
        for dim in box:
            size *= dim.get_diff()
        return size

    def __init__(self, dimensions: Tuple[MBBDim, ...]):
        self.box = dimensions
        self.size = self.get_size(self.box)

    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    def overlaps(self, other: MBB) -> bool:
        """Checks if passed MBB is overlapping with this MBB"""
        for mbb1, mbb2 in zip(self.box, other.box):
            if not mbb1.overlaps(mbb2):
                return False
        return True

    def contains_inner(self, inner_mbb: MBB):
        """Checks if passed MBB is inside this MBB"""
        for inner, outer in zip(self.box, inner_mbb.box):
            if not outer.contains(inner):
                return False
        return True

    def insert_entry(self, new_entry: Tuple[MBBDim, ...]):
        """Inserts a new entry (Child node) into"""
        new_box: Tuple[MBBDim, ...] = ()
        for new_entry_dim, old_box_dim in zip(new_entry, self.box):
            new_box += (MBBDim(min(new_entry_dim.low, old_box_dim.low), (max(new_entry_dim.high, old_box_dim.high))),)

        self.box = new_box
        self.size = self.get_size(self.box)

    # todo also check overlap between different MBBs

    def size_increase_insert(self, new_entry: Tuple[MBBDim, ...]):
        """Calculates size of MBB if a new entry were to be inserted"""
        old_size = self.size
        new_mmb = MBB(self.box)

        new_mmb.insert_entry(new_entry)
        new_size = new_mmb.size

        return new_size - old_size
