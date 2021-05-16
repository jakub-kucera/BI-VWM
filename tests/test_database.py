import os
import random
from typing import List

import pytest

from rtree.data.database import Database
from rtree.data.database_entry import DatabaseEntry
from rtree.default_config import TESTING_DIRECTORY, TREE_FILE_TEST, DATABASE_FILE_TEST


@pytest.mark.parametrize('dimensions, count, low, high', [
    (1, 100, 0, 100),
    (2, 100, 0, 100),
    (3, 100, 0, 100),
    (10, 100, 0, 100),
    (2, 300, 0, 300),
    (2, 100, -100, 500),
    (3, 1000, -1000, 5000),
])
def test_rtree_create_entry(dimensions: int, count: int, low: int, high: int):
    try:
        os.remove(TESTING_DIRECTORY + DATABASE_FILE_TEST)
    except FileNotFoundError:
        pass

    database = Database(filename=TESTING_DIRECTORY + DATABASE_FILE_TEST,
                        dimensions=dimensions)

    total_insert_count = 0

    for c in range(0, count):
        coordinates = [random.randint(low, high) for _ in range(0, dimensions)]

        # print(coordinates)

        total_insert_count += 1
        data = f"c: {c} coords: {coordinates}"
        position = database.create(DatabaseEntry(coordinates=coordinates, data=data))

        found_entry = database.search(position)

        if found_entry is None:
            print(f"c:{c} missing: {coordinates}")
            # visualize(tree, show_mbbs_only=False, open_img=True)
        assert found_entry is not None
        assert found_entry.data == data
        assert found_entry.coordinates == coordinates
        assert found_entry.is_present

    del database
    os.remove(TESTING_DIRECTORY + DATABASE_FILE_TEST)


@pytest.mark.parametrize('dimensions, count, low, high', [
    (1, 100, 0, 100),
    (2, 100, 0, 100),
    (3, 100, 0, 100),
    (10, 100, 0, 100),
    (2, 300, 0, 300),
    (2, 100, -100, 500),
    (3, 1000, -1000, 5000),
])
def test_rtree_create_entry_read(dimensions: int, count: int, low: int, high: int):
    try:
        os.remove(TESTING_DIRECTORY + DATABASE_FILE_TEST)
    except FileNotFoundError:
        pass

    database = Database(filename=TESTING_DIRECTORY + DATABASE_FILE_TEST,
                        dimensions=dimensions)

    total_insert_count = 0
    created_entries_positions: List[int] = []

    for c in range(0, count):
        coordinates = [random.randint(low, high) for _ in range(0, dimensions)]

        # print(coordinates)

        total_insert_count += 1
        data = {'c': c, 'coordinates': coordinates}
        position = database.create(DatabaseEntry(coordinates=coordinates, data=data))
        created_entries_positions.append(position)

        found_entry = database.search(position)

        if found_entry is None:
            print(f"c:{c} missing: {coordinates}")
            # visualize(tree, show_mbbs_only=False, open_img=True)
        assert found_entry is not None
        assert found_entry.data == data
        assert found_entry.coordinates == coordinates
        assert found_entry.is_present

    for c, position in enumerate(created_entries_positions):
        found_entry = database.search(position)
        assert found_entry is not None

        assert found_entry.data["c"] == c
        assert found_entry.data["coordinates"] == found_entry.coordinates
        print(f"{found_entry.data['c']} == {c}")

    del database
    os.remove(TESTING_DIRECTORY + DATABASE_FILE_TEST)
