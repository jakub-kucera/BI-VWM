from rtree.database import Database
from rtree.database_entry import DatabaseEntry


if __name__ == '__main__':
    db = Database()
    positon = db.create(DatabaseEntry([1,4], {'id': 1, 'firstname': 'Pavel', 'lastname': 'Šimon'}))

    throw_away = db.create(DatabaseEntry([1,1], {'id': 2, 'firstname': 'Pavel', 'lastname': 'Šimon'}))
    throw_away = db.create(DatabaseEntry([1,0], {'id': 3, 'firstname': 'Pavel', 'lastname': 'Šimon'}))
    throw_away = db.create(DatabaseEntry([0,1], {'id': 4, 'firstname': 'Pavel', 'lastname': 'Šimon'}))

    back = db.search(positon)
    print(back.coordinates, back.data)