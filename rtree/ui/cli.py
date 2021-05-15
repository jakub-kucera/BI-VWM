# from rtree.rtree import RTree
# from rtree.default_config import *

import os

from typing import List
from rtree.rtree import RTree
from rtree.data.database_entry import DatabaseEntry
from rtree.ui.cli_support import *


class CLI:
    def __init__(self):
        self.file_name = ""
        self.tree = None
        self.dimensions = 2

        self.__init_setup()

    def __init_setup(self):
        print("+==========================================================+\n"
              "|                  R-Tree CLI  beta-0.3                    |\n"
              "| FIT CVUT 2021                                            |\n"
              "| BI-VWM                                kucerj56, jezdimat |\n"
              "+==========================================================+\n"
              " tip: You can type 'exit' or 'help' at any time\n"
              "\n"
              "Create new database and rtree or load existing ones?\n"
              "1> Create\n"
              "2> Load")
        self.__setup_database_file()
        self.__default_menu()

    def __setup_database_file(self):
        db_choice = get_input()
        if matches(db_choice, ["c", "1"]):

            print("Set name for new database")
            while True:
                self.file_name = get_input()
                if is_valid_db_name(self.file_name):  # exists?
                    break
                else:
                    print("Try again")

            print(f"Created database file 'saved_data/{self.file_name}.bin'\n"
                  f"Created r-tree file 'saved_data/{self.file_name}.rtree.bin'")

            print("How many dimensions should r-tree use? (1 - 16)")
            while True:
                try:
                    self.dimensions = int(get_input())
                except TypeError:
                    print("Try again")

                if 1 <= self.dimensions <= 16:
                    break
                else:
                    print("Try again")

            print(f"R-tree has {self.dimensions} 64-bit Int dimensions")

            self.tree = RTree()
            '''self.tree = RTree(tree_file=self.file_name + ".rtree.bin",
                               database_file=self.file_name + ".bin",
                               dimensions=self.dimensions,
                               parameters_size=8)'''

        elif matches(db_choice, ["l", "2"]):

            print("Select database file from saved_data/[name]")  # print existing databases

            while True:
                self.file_name = get_input()
                if is_valid_db_name(self.file_name):  # exists?
                    break
                else:
                    print("Try again")

            print(f"Loaded database file 'saved_data/{self.file_name}.bin'\n"
                  f"Loaded r-tree file 'saved_data/{self.file_name}.rtree.bin'")

            '''self.tree = RTree(tree_file=self.file_name + ".rtree.bin",
                               database_file=self.file_name + ".bin")'''

        else:
            print("Unrecognized input, try again!")
            self.__setup_database_file()

    def __default_menu(self):
        print("\n============================================================\n"
              f"files: {self.file_name}, dimensions: {self.dimensions}, entries: NaN\n"
              "1> Add to database\n"
              "TODO Remove from database\n"
              "3> Search for Point\n"
              "TODO Search for Range\n"
              "TODO Search for Nearest neighbour\n"
              "TODO Print graph\n"
              "TODO Delete Tree and Rebuild\n"
              "TODO Delete Tree and Database")
        self.__default_loop()

    def __default_loop(self):
        action = get_input()
        if matches(action, ["a", "1"]):

            self.__add_entry()

        elif matches(action, ["r", "2"]):

            print("Set dimension 1, 2, 3, .., n - or node_id")
            # self.__remove_entry()

        elif matches(action, ["sp", "3"]):

            self.__search_point()

        elif matches(action, ["sr", "4"]):

            print("Set dimension rage 1, 2, 3, .., n * 2")
            # self.__search_range()

        elif matches(action, ["sn", "5"]):

            print("Set k - Set dimensions range 1, 2, 3, .., n * 2")
            # self.__search_knn()

        elif matches(action, ["p", "6"]):

            print("Display visual")
            # self.__graph()

        elif matches(action, ["dtr", "13"]):

            print("Delete Tree and Rebuild")
            # self.__graph()

        elif matches(action, ["dtd", "19"]):

            print("Delete Tree and Database")
            # self.__graph()

        else:
            print("Unrecognized input, try again!")

        self.__default_menu()

    def __search_point(self):
        coord: List[int] = []
        for n in range(self.dimensions):
            print("Set dimension", n + 1)
            while True:
                try:
                    tmp = int(get_input())
                    coord.append(tmp)
                    break
                except TypeError:
                    print("Try again")

        entry = self.tree.search_node(coord)
        if entry is None:
            print("No entry found on given coordinates")
        else:
            print(entry.data)

    def __add_entry(self):
        coord: List[int] = []
        for n in range(self.dimensions):
            print("Set dimension", n + 1)
            while True:
                try:
                    tmp = int(get_input())
                    coord.append(tmp)
                    break
                except TypeError:
                    print("Try again")

        print("Insert data for this entry:")
        data = input()

        entry = DatabaseEntry(coord, data)
        self.tree.insert_entry(entry)
        print(f"Entry '{entry.data}' saved, at position '{coord}'")


if __name__ == "__main__":
    CLI()
