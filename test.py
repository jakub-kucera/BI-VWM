import os

from rtree.data.database import Database
from rtree.data.database_entry import DatabaseEntry
from rtree.default_config import WORKING_DIRECTORY, DEFAULT_DATABASE_FILE


def database_example():
    db = Database()
    positon = db.create(DatabaseEntry([1, 4], {'id': 1, 'firstname': 'Pavel', 'lastname': 'Šimon'}))

    throw_away = db.create(DatabaseEntry([1, 1], {'id': 2, 'firstname': 'Pavel', 'lastname': 'Šimon'}))
    throw_away = db.create(DatabaseEntry([1, 0], {'id': 3, 'firstname': 'Pavel', 'lastname': 'Šimon'}))

    db.mark_to_delete(throw_away)

    throw_away = db.create(DatabaseEntry([0, 1], {'id': 4, 'firstname': 'Pavel', 'lastname': 'Šimon'}))

    back = db.search(positon)
    print(back.coordinates, back.data)

    target = db.FUTURE_linear_search_for([1, 1])
    print(target.coordinates, target.data)

    target = db.FUTURE_linear_search_for([7, 7])
    print(target)

    try:
        os.remove(WORKING_DIRECTORY + DEFAULT_DATABASE_FILE)
    except FileNotFoundError:
        pass


if __name__ == '__main__':
    database_example()
    # tree = RTree()
