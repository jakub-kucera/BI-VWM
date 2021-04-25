import os
import pickle
from typing import Optional, List, Tuple

from rtree.database_entry import DatabaseEntry
from rtree.default_config import *


class Database:
    def __init__(self,
                 filename: str = DEFAULT_DATABASE_FILE,
                 dimensions: int = DEFAULT_DIMENSIONS,
                 parameters_size: int = PARAMETER_RECORD_SIZE,
                 unique_sequence: bytes = DEMO_UNIQUE_SEQUENCE,
                 config_hash: bytes = DEMO_CONFIG_HASH):
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

    def __set_header(self, unique: bytes, config: bytes):
        self.file.seek(0, 0)

        self.file.write(unique)
        self.file.write(config)

        self.file.flush()

    def __get_header(self) -> Tuple[bytes, bytes]:
        self.file.seek(0, 0)

        unique = self.file.read(UNIQUE_SEQUENCE_LENGTH)
        config = self.file.read(CONFIG_HASH_LENGTH)

        self.file.flush()
        self.current_position = self.header_size

        return (unique, config)

    def __verify_bit_position(self, bit_positon: int) -> bool:
        # remove flag and dimensions from the "self.filesize", data is variable and must be handled differently
        return (bit_positon < self.filesize)

    def search(self, byte_position: int) -> Optional[DatabaseEntry]:
        if not self.__verify_bit_position(byte_position):
            # throw
            print("Database error! Requesting position outside the file.")
            return None

        self.file.seek(byte_position, 0)

        is_present = bool.from_bytes(self.file.read(RECORD_FLAG_SIZE), byteorder=DATABASE_BYTEORDER, signed=False)

        # reads the range in each dimension
        coordinates = []
        for _ in range(self.dimensions):  # todo maybe use float by default
            dim = int.from_bytes(self.file.read(self.parameter_record_size), byteorder=DATABASE_BYTEORDER, signed=True)
            coordinates.append(dim)

        data = pickle.load(self.file)

        self.file.flush()
        
        return DatabaseEntry(coordinates, data, is_present)

    def delete(self):
        pass

    def create(self, new_record: DatabaseEntry) -> int:
        if(len(new_record.coordinates) != self.dimensions):
            # throw
            print("Data creation error! recieved incorrent dimensions.", len(new_record.coordinates), self.dimensions)

        self.__update_file_size()
        beginning = self.filesize

        # append to the end of database file
        self.file.seek(0, 2)

        self.file.write(new_record.is_present.to_bytes(RECORD_FLAG_SIZE, byteorder=DATABASE_BYTEORDER, signed=False))

        for dimension in new_record.coordinates:
            self.file.write(dimension.to_bytes(self.parameter_record_size, byteorder=DATABASE_BYTEORDER, signed=True))

        pickle.dump(new_record.data, self.file)

        self.file.flush()
        self.__update_file_size()

        return beginning

    def mark_to_delete(self, byte_position: int):
        if (not self.__verify_bit_position(byte_position)):
            # throw
            print("Database error! Requesting position outside the file.")
            return

        self.file.seek(byte_position, 0)

        self.file.write(False.to_bytes(RECORD_FLAG_SIZE, byteorder=DATABASE_BYTEORDER, signed=False))

        self.file.flush()

    # deletes marked entries and
    def recalculate(self):
        pass

    # future linear search stuffu

    def __get_next_entry(self) -> Optional[DatabaseEntry]:

        is_present = bool.from_bytes(self.file.read(RECORD_FLAG_SIZE), byteorder=DATABASE_BYTEORDER, signed=False)

        if not is_present:  # this entry is to be deleted, not return its value
            return None

        coordinates = []
        for _ in range(self.dimensions):
            dim = int.from_bytes(self.file.read(self.parameter_record_size), byteorder=DATABASE_BYTEORDER, signed=True)
            coordinates.append(dim)

        data = pickle.load(self.file)

        self.file.flush()

        return DatabaseEntry(coordinates, data, is_present)

    def FUTURE_linear_search_for(self, coordinates: List[int]) -> Optional[DatabaseEntry]:
        # point at start
        self.file.seek(self.header_size, 0)

        try:
            tmp = self.__get_next_entry()

            while(tmp == None or tmp.coordinates != coordinates):
                tmp = self.__get_next_entry()

            # unnecessary if here?
            if(tmp.coordinates == coordinates):
                return tmp

        except:
            print("probably reached EOF, all entries were compared")
        
        # coordinates not matched
        return None
