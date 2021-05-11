from __future__ import annotations


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
