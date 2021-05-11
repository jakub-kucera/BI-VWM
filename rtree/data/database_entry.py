from typing import List

from rtree.data.mbb import MBB
from rtree.data.mbb_dim import MBBDim


class DatabaseEntry:
    def __init__(self, coordinates: List[int], data: object = None, is_present: bool = True):
        self.is_present = is_present
        self.coordinates = coordinates
        self.data = data  # can be empty

    def get_mbb(self):
        return MBB(tuple(MBBDim(coords, coords) for coords in self.coordinates))
