from typing import List


class DatabaseEntry:
    def __init__(self, coordinates: List[int], data: object = None):
        self.to_be_removed = False
        self.coordinates = coordinates
        self.data = data # can be empty
