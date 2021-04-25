import os
import pickle
from typing import Optional

from rtree.database_entry import DatabaseEntry
from rtree.default_config import *


class Database:
    def __init__(self,
                 filename: str = DEFAULT_DATABASE_FILE,
                 dimensions: int = DEFAULT_DIMENSIONS,
                 parameters_size: int = PARAMETER_RECORD_SIZE,
                 unique_sequence: int = 1326475809,
                 config_hash: int = 9085746231):
        self.filename = WORKING_DIRECTORY + filename
        self.dimensions = dimensions
        self.parameter_record_size = parameters_size
        self.header_size = UNIQUE_SEQUENCE_LENGTH + CONFIG_HASH_LENGTH

        # create file if not exists
        save_header_to_file = False
        if not os.path.isfile(self.filename):  # todo maybe move to RTree
            save_header_to_file = True
            with open(self.filename, 'w+b'):
                pass

        # open file
        try:
            self.file = open(self.filename, 'r+b')
        except IOError:
            input(f"File cannot be opened: {self.filename}")

        # verify file
        if save_header_to_file:
            self.__set_header(unique_sequence, config_hash)
        else:
            (unique, config) = self.__get_header()
            if(unique != unique_sequence or config != config_hash):
                # throw
                print("Invalid database file! Header not matching the rtree definition.")

        self.filesize = 0
        self.__update_file_size()

        self.current_position = self.filesize

    
    def __del__(self):
        if not self.file.closed:
            self.file.flush()
            self.file.close()

        if DELETE_TREE_INDEX_FILE:
            os.remove(self.filename)

    def __str__(self):
        return str(self.__dict__)

    def __update_file_size(self):
        self.filesize = os.path.getsize(self.filename)

    def __set_header(self, unique: int, config: int):
        self.file.seek(0, 0)
        
        self.file.write(unique.to_bytes(UNIQUE_SEQUENCE_LENGTH, byteorder=DATABASE_BYTEORDER, signed=False))
        self.file.write(config.to_bytes(CONFIG_HASH_LENGTH, byteorder=DATABASE_BYTEORDER, signed=False))
        
        self.file.flush()

    def __get_header(self) -> (int, int):
        self.file.seek(0, 0)
        
        unique = int.from_bytes(self.file.read(UNIQUE_SEQUENCE_LENGTH), byteorder=DATABASE_BYTEORDER, signed=False)
        config = int.from_bytes(self.file.read(CONFIG_HASH_LENGTH), byteorder=DATABASE_BYTEORDER, signed=False)
        
        self.file.flush()
        self.current_position = self.header_size
        
        return (unique, config)


    def search(self, positon: int) -> DatabaseEntry:
        self.file.seek(positon, 0)

        # reads the range in each dimension
        coordinates = []
        for _ in range(self.dimensions):  # todo maybe use float by default
            dim = int.from_bytes(self.file.read(self.parameter_record_size), byteorder=DATABASE_BYTEORDER, signed=True)
            coordinates.append(dim)

        data = pickle.load(self.file)
        
        return DatabaseEntry(coordinates, data)

    def delete(self):
        pass

    def create(self, record: DatabaseEntry) -> int: # return entry id or -1/throw on error
        if(len(record.coordinates) != self.dimensions):
            print("Data creation error! recieved incorrent dimensions.", len(record.coordinates), self.dimensions)

        self.__update_file_size()
        beginning = self.filesize

        self.file.seek(0, 2)

        for dimension in record.coordinates:
            self.file.write(dimension.to_bytes(self.parameter_record_size, byteorder=DATABASE_BYTEORDER, signed=True))

        # ??
        pickle.dump(record.data, self.file)

        self.file.flush()
        self.__update_file_size()
        self.current_position = self.file.tell()

        return beginning

    def mark_to_delete(self, entry_id: int):
        pass

    # deletes marked entries and
    def recalculate(self):
        pass
