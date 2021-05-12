import math
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

    def distance_from(self, coordinates: List[int]) -> float:
        diff_sum = 0
        for a, b in zip(self.coordinates, coordinates):
            diff_sum += (a - b) ** 2
        return math.sqrt(diff_sum)
