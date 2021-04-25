from typing import List


class DatabaseEntry:
    def __init__(self, coordinates: List[int], data: object = None, is_present: bool = True):
        self.is_present = is_present
        self.coordinates = coordinates
        self.data = data # can be empty
