from rtree.database_entry import DatabaseEntry


class Database:  # maybe rename
    def __init__(self, id_size: int = 8, ):
        self.database_file
        self.offset_size
        self.id_size
        self.header_size
        # self.cache = []

    def search(self, entry_id: int) -> DatabaseEntry:  # maybe also use cache # , only_header: bool = True
        pass

    def delete(self):
        pass

    def create(self):
        pass

    def mark_to_delete(self, entry_id: int):
        pass

    # deletes marked entries and
    def recalculate(self):
        pass
