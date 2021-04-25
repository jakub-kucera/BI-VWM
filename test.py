from rtree.database import Database
from rtree.database_entry import DatabaseEntry


if __name__ == '__main__':
    db = Database()
    positon = db.create(DatabaseEntry([1,4], {'id': 1, 'firstname': 'Pavel', 'lastname': 'Šimon'}))

    throw_away = db.create(DatabaseEntry([1,1], {'id': 2, 'firstname': 'Pavel', 'lastname': 'Šimon'}))
    throw_away = db.create(DatabaseEntry([1,0], {'id': 3, 'firstname': 'Pavel', 'lastname': 'Šimon'}))

    db.mark_to_delete(throw_away)

    throw_away = db.create(DatabaseEntry([0,1], {'id': 4, 'firstname': 'Pavel', 'lastname': 'Šimon'}))

    back = db.search(positon)
    print(back.coordinates, back.data)

    target = db.FUTURE_linear_search_for([1, 1])
    print(target.coordinates, target.data)

    target = db.FUTURE_linear_search_for([7, 7])
    print(target)
