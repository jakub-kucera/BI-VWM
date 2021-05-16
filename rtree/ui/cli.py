import os
from typing import List, Optional
from rtree.rtree import RTree
from rtree.default_config import *
from rtree.ui.cli_support import *
from rtree.ui.visualiser import visualize
from rtree.data.database_entry import DatabaseEntry


class CLI:
    def __init__(self):
        self.db_file: str = ""
        self.tree_file: str = ""
        self.dimensions: int = 2
        self.tree: Optional[RTree] = None

        self.__init_setup()

    def __print_entry(self, entry: DatabaseEntry):
        print(f"{entry.coordinates} {entry.data}")

    def __init_setup(self):
        print("+==========================================================+\n"
              "|                  R-Tree CLI  beta-0.4                    |\n"
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

            self.__create_new_tree()

        elif matches(db_choice, ["l", "2"]):

            self.__load_existing_tree()

        else:
            print("Unrecognized input, try again!")
            self.__setup_database_file()

    def __create_new_tree(self):
        print("\nSet name for new database file")
        file_name = ""
        while True:
            file_name = get_input()
            if is_valid_db_name(file_name):
                self.db_file = file_name + ".db.bin"
                self.tree_file = file_name + ".rtree.bin"
                for file in os.listdir(path=WORKING_DIRECTORY):
                    if file == self.db_file or file == self.tree_file:
                        print("Filename taken, try again")
                break
            else:
                print("Try again")

        print(f"\nPrepared database file '{WORKING_DIRECTORY}{self.db_file}'\n"
              f"Prepared r-tree file '{WORKING_DIRECTORY}{self.tree_file}'")

        print("\nHow many dimensions should r-tree use? [1 <= integer <= 16]")
        while True:
            try:
                self.dimensions = int(get_input())
                if 1 <= self.dimensions <= 16:
                    break
                else:
                    print("Try again")
            except ValueError:
                print("Try again")

        print(f"R-tree has {self.dimensions} 64-bit Int dimensions")

        self.tree = RTree(database_file=self.db_file,
                          tree_file=self.tree_file,
                          dimensions=self.dimensions,
                          parameters_size=8)

    def __load_existing_tree(self):
        print("\nSelect database file from saved_data/[name] or [id]")

        snapshot: List[str] = os.listdir(path=WORKING_DIRECTORY)
        iterator = 0
        for file in snapshot:
            print(f"{iterator}> {file}")
            iterator += 1

        while True:
            self.db_file = get_input()
            try:
                num = int(self.db_file)
                if 0 <= num < len(snapshot):
                    self.db_file = snapshot[num]
                    break
                else:
                    print("Try again")
            except ValueError:
                if is_valid_db_name(self.db_file):  # exists?
                    break
                else:
                    print("Try again")

        print("Select matching tree file")

        while True:
            self.tree_file = get_input()
            try:
                num = int(self.tree_file)
                if 0 <= num < len(snapshot):
                    self.tree_file = snapshot[num]
                    break
                else:
                    print("Try again")
            except ValueError:
                if is_valid_db_name(self.tree_file):  # exists?
                    break
                else:
                    print("Try again")

        self.tree = RTree(database_file=self.db_file, tree_file=self.tree_file)
        self.dimensions = self.tree.dimensions

        print(f"\nLoaded database file '{WORKING_DIRECTORY}{self.db_file}'\n"
              f"Loaded rtree file '{WORKING_DIRECTORY}{self.tree_file}'")

    def __default_menu(self):
        print("\n+==========================================================+\n"
              f"+ files: {self.db_file} & {self.tree_file}\n"
              f"+ dimensions: {self.dimensions}, entries: NaN\n"
              f"+ depth: NaN, node_size: NaN\n\n" +
              ("0> Print graph\n" if self.dimensions == 2 else "") +
              "1> Add to database\n"
              "TODO 2> Remove from database\n"
              "3> Search for Point\n"
              "4> Search for points in Range\n"
              "5> Search for Nearest neighbours\n"
              "TODO 8> Rebuild Tree\n"
              "TODO 9> Delete Tree and Database\n"
              "?> Help\n"
              "!> Exit")
        self.__default_loop()

    def __default_loop(self):
        action = get_input()
        print("")

        if matches(action, ["p", "0"]):

            if self.dimensions == 2:
                self.__graph()
            else:
                print("Graph is only available for 2 dimensions, sorry.")

        elif matches(action, ["a", "1"]):

            self.__add_entry()

        elif matches(action, ["r", "2"]):

            self.__remove_entry()

        elif matches(action, ["sp", "3"]):

            self.__search_point()

        elif matches(action, ["sr", "4"]):

            self.__search_range()

        elif matches(action, ["sn", "5"]):

            self.__search_knn()

        elif matches(action, ["rt", "8"]):

            print("Are you sure?  [1> Yes]")
            tmp = get_input()
            if matches(tmp, ["yes", "y", "1"]):
                self.__rebuild_tree()
            else:
                print("Action aborted")

        elif matches(action, ["dtd", "9"]):

            print("Are you sure?  [1> Yes]")
            tmp = get_input()
            if matches(tmp, ["yes", "y", "1"]):
                self.__delete_files()
            else:
                print("Action aborted")

        else:
            print("Unrecognized input, try again!")

        self.__default_menu()

    def __graph(self):
        print("Include entries?  [1> Yes]")
        tmp = not matches(get_input(), ["yes", "y", "1"])
        visualize(self.tree, show_mbbs_only=tmp, save_img=True, show_img=True)
        print("File generated in './testing_img.png'")

    def __add_entry(self):
        coord: List[int] = []
        for n in range(self.dimensions):
            print("Set dimension", n + 1)
            while True:
                try:
                    tmp = int(get_input())
                    coord.append(tmp)
                    break
                except ValueError:
                    print("Try again")

        print("Insert data for this entry:")
        data = input()

        entry = DatabaseEntry(coord, data)
        self.tree.insert_entry(entry)
        print(f"\nEntry '{entry.data}' saved, at position {coord}")

    def __remove_entry(self):
        pass

    def __search_point(self):
        coord: List[int] = []
        for n in range(self.dimensions):
            print("Set dimension", n + 1, " [integer]")
            while True:
                try:
                    tmp = int(get_input())
                    coord.append(tmp)
                    break
                except ValueError:
                    print("Try again")

        entry = self.tree.search_entry(coord)
        if entry is None:
            print("No entry found on given coordinates")
        else:
            self.__print_entry(entry)

    def __search_range(self):
        coord_min: List[int] = []
        coord_max: List[int] = []
        for n in range(self.dimensions):
            print("Set lower boundary for dimension", n + 1, " [integer]")
            while True:
                try:
                    tmp = int(get_input())
                    coord_min.append(tmp)
                    break
                except ValueError:
                    print("Try again")
            print("Set higher boundary for dimension", n + 1, " [integer]")
            while True:
                try:
                    tmp = int(get_input())
                    coord_max.append(tmp)
                    break
                except ValueError:
                    print("Try again")

        entries = self.tree.search_area(coordinates_min=coord_min, coordinates_max=coord_max)
        if entries is []:
            print("No entries found in given range")
        else:
            for entry in entries:
                self.__print_entry(entry)

    def __search_knn(self):
        k: int = 0
        print("How many entries to look for?  [integer >= 1]")
        while True:
            try:
                k = int(get_input())
                break
            except ValueError:
                print("Try again")

        coord: List[int] = []
        for n in range(self.dimensions):
            print("Set dimension", n + 1, " [integer]")
            while True:
                try:
                    tmp = int(get_input())
                    coord.append(tmp)
                    break
                except ValueError:
                    print("Try again")

        entries = self.tree.search_knn(k, coord)
        if entries is []:
            print("No entries found in given range")
        else:
            for entry in entries:
                self.__print_entry(entry)

    def __rebuild_tree(self):
        pass

    def __delete_files(self):
        pass


if __name__ == "__main__":
    CLI()
