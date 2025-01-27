from __future__ import annotations

from typing import Tuple, List

from rtree.data.mbb_dim import MBBDim


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

    @staticmethod
    def get_metric_size(box: Tuple[MBBDim, ...]):
        """Calculates the size of volume of MBB"""
        # maybe use different metric
        size = 1
        for dim in box:
            diff = dim.get_diff()
            if diff != 0:
                size *= diff

        return size

    @staticmethod
    def create_box_from_entry_list(coordinates: List[int]) -> MBB:
        """Creates MBB from database entry coordinates (1D list)"""
        return MBB(tuple(MBBDim(coord, coord) for coord in coordinates))

    def __init__(self, dimensions: Tuple[MBBDim, ...]):
        self.box = dimensions
        self.size = self.get_size(self.box)

    def __str__(self):
        string_out = "["
        for dim in self.box:
            string_out += f"[low: {dim.low}, high: {dim.high}], "
        string_out += "]"
        return string_out

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
        if len(self.box) != len(inner_mbb.box):
            return False

        for outer, inner in zip(self.box, inner_mbb.box):
            if not outer.contains(inner):
                return False
        return True

    def insert_mbb(self, new_mbb: Tuple[MBBDim, ...]):
        """Inserts a new entry (Child node) into"""
        new_box: Tuple[MBBDim, ...] = ()
        for new_entry_dim, old_box_dim in zip(new_mbb, self.box):
            new_box += (MBBDim(min(new_entry_dim.low, old_box_dim.low), (max(new_entry_dim.high, old_box_dim.high))),)

        self.box = new_box
        self.size = self.get_size(self.box)

    def size_increase_insert(self, new_box: Tuple[MBBDim, ...]):
        """Calculates size of MBB if a new entry were to be inserted"""

        new_mmb = MBB(self.box)
        new_mmb.insert_mbb(new_box)

        # return new_mmb.size - self.size

        return self.get_metric_size(new_mmb.box) - self.get_metric_size(self.box)
