import os
import pickle
from typing import Optional, List, Tuple

from rtree.data.database_entry import DatabaseEntry
from rtree.default_config import *


class Database:
    def __init__(self,
                 filename: str = WORKING_DIRECTORY + DEFAULT_DATABASE_FILE,
                 dimensions: int = DEFAULT_DIMENSIONS,
                 parameters_size: int = PARAMETER_RECORD_SIZE,
                 unique_sequence: bytes = DEMO_UNIQUE_SEQUENCE,
                 config_hash: bytes = DEMO_CONFIG_HASH):
        self.filename = filename
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
            if unique != unique_sequence or config != config_hash:
                raise Exception("Invalid database file! Header not matching the rtree definition.")

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

        return unique, config

    def __verify_byte_position(self, byte_position: int) -> bool:
        # remove flag and dimensions from the "self.filesize", data is variable and must be handled differently
        return byte_position < ((self.filesize - RECORD_FLAG_SIZE) - (self.dimensions * self.parameter_record_size))

    def search(self, byte_position: int) -> DatabaseEntry:
        if not self.__verify_byte_position(byte_position):
            raise ValueError("Database error! Requesting position outside the file.")

        self.file.seek(byte_position, 0)

        is_present = bool.from_bytes(self.file.read(RECORD_FLAG_SIZE), byteorder=DATABASE_BYTEORDER, signed=False)

        # reads the range in each dimension
        coordinates = []
        for _ in range(self.dimensions):
            dim = int.from_bytes(self.file.read(self.parameter_record_size), byteorder=DATABASE_BYTEORDER, signed=True)
            coordinates.append(dim)

        try:
            data = pickle.load(self.file)
        except Exception as e:
            print(f"Error when calling pickle on position {byte_position}")
            raise e

        self.file.flush()

        return DatabaseEntry(coordinates, data, is_present)

    def create(self, new_record: DatabaseEntry) -> int:
        if len(new_record.coordinates) != self.dimensions:
            raise ValueError("Data creation error! received incorrect dimensions.")

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
        if not self.__verify_byte_position(byte_position):
            raise ValueError("Database error! Requesting position outside the file.")

        self.file.seek(byte_position, 0)

        self.file.write(False.to_bytes(RECORD_FLAG_SIZE, byteorder=DATABASE_BYTEORDER, signed=False))

        self.file.flush()

    # deletes marked entries and
    def recalculate(self):
        pass

    # future linear search stuffu

    def __point_at_first(self):
        self.current_position = self.header_size
        self.file.seek(self.current_position, 0)

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

    def linear_search_entry(self, coordinates: List[int]) -> Optional[DatabaseEntry]:
        self.__point_at_first()
        try:
            entry = self.__get_next_entry()
            if entry is None:
                return None
            while entry.coordinates != coordinates:
                entry = self.__get_next_entry()
                if entry is None:
                    return None
            return entry
        except:
            print("probably reached EOF, all entries were compared")
        # coordinates not matched
        return None

    def linear_search_area(self, coordinates_min: List[int], coordinates_max: List[int]) -> List[DatabaseEntry]:
        self.__point_at_first()
        matching: List[DatabaseEntry] = []
        try:
            entry = self.__get_next_entry()
            if entry is None:
                return matching
            while True:
                tmp = 0
                for mbb_dim in range(self.dimensions):
                    if coordinates_min[mbb_dim] <= entry.coordinates[mbb_dim] <= coordinates_max[mbb_dim]:
                        tmp += 1
                if tmp == self.dimensions:
                    matching.append(entry)
                entry = self.__get_next_entry()
                if entry is None:
                    return matching
        except:
            print("probably reached EOF, all entries were compared")
        return matching

    def linear_search_knn(self, k: int, coordinates: List[int]) -> List[DatabaseEntry]:
        self.__point_at_first()
        entry_list: List[DatabaseEntry] = []

        try:
            while True:
                entry = self.__get_next_entry()
                if entry is None:
                    break
                entry_list.append(entry)
        except:
            print("probably reached EOF, all entries were compared")

        if len(entry_list) <= k:
            return entry_list
        else:
            entry_list.sort(key=lambda x: x.distance_from(coordinates))
            return entry_list[0:k]