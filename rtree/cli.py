# from rtree.rtree import RTree
# from rtree.default_config import *


class CLI:
    def __init__(self):
        self.file_name = ""
        # self.rtree = None
        self.dimensions = 2

        self.__init_setup()

    def __input(self):
        tmp = input(">> ")
        for x in ["exit", "e", "quit", "q"]:
            if x == tmp.lower():
                exit()
        for x in ["help", "h", "?"]:
            if x == tmp.lower():
                self.__print_help()
                return self.__input()
        return tmp

    def __matches(self, text, options) -> bool:
        for x in options:
            if x == text:
                return True
        return False

    def __print_help(self):
        print("+==========================================================+\n"
              "| CLI:\n"
              "| - For every choice, you can write the whole text, number\n"
              "| or just the capitals, for example '1> Exit' will accept\n"
              "| 'e' or '1' as an input\n"
              "| - To exit the application using 'exit', 'e', 'quit' or 'q'\n"
              "| - Get help using 'help', 'h' or '?'\n"
              "| R-Tree:\n"
              "| - Working binary files are stored in './saved_data' folder\n"
              "| - Filenames cannot contain spaces or start with a number,\n"
              "| other OS specific exceptions also apply\n"
              "+==========================================================+")
        return

    def __init_setup(self):
        print("+==========================================================+\n"
              "|                  R-Tree CLI  beta-0.1                    |\n"
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
        db_choice = self.__input()
        if self.__matches(db_choice, ["c", "1"]):

            print("Set name for new database")
            self.file_name = self.__input()
            print(f"Created database file 'saved_data/{self.file_name}.bin'\n"
                  f"Created r-tree file 'saved_data/{self.file_name}.rtree.bin'")

            self.__setup_dimensions()
            '''self.rtree = RTree(tree_file=self.file_name + ".rtree.bin",
                               database_file=self.file_name + ".bin",
                               dimensions=self.dimensions,
                               parameters_size=8)'''

        elif self.__matches(db_choice, ["l", "2"]):

            print("Insert database file from saved_data/[name]")
            self.file_name = self.__input()
            print(f"Loaded database file 'saved_data/{self.file_name}.bin'\n"
                  f"Loaded r-tree file 'saved_data/{self.file_name}.rtree.bin'")

            '''self.rtree = RTree(tree_file=self.file_name + ".rtree.bin",
                               database_file=self.file_name + ".bin")'''

        else:
            print("Unrecognized input, try again!")
            self.__setup_database_file()

    def __setup_dimensions(self):
        print("How many dimensions should r-tree use?")
        self.__setup_dimensions_loop()
        print(f"R-tree has {self.dimensions} 64-bit Int dimensions")

    def __setup_dimensions_loop(self):
        try:
            self.dimensions = int(self.__input())
        except ValueError:
            print("That's not an int!")
            self.__setup_dimensions_loop()
        return

    def __default_menu(self):
        print("============================================================\n"
              f"files: {self.file_name}, dimensions: {self.dimensions}\n"
              "1> Add to database\n"
              "2> Remove from database\n"
              "3> Search for Point\n"
              "4> Search for Range\n"
              "5> Search for Nearest neighbour\n"
              "6> Print graph")
        self.__default_loop()

    def __default_loop(self):
        action = self.__input()
        if self.__matches(action, ["a", "1"]):

            print("Set dimension 1, 2, 3, .., n - Set content")
            # self.__add_entry()

        elif self.__matches(action, ["r", "2"]):

            print("Set dimension 1, 2, 3, .., n - or node_id")
            # self.__remove_entry()

        elif self.__matches(action, ["sp", "3"]):

            print("Set dimension 1, 2, 3, .., n")
            # self.__search_point()

        elif self.__matches(action, ["sr", "4"]):

            print("Set dimension rage 1, 2, 3, .., n * 2")
            # self.__search_range()

        elif self.__matches(action, ["sn", "5"]):

            print("Set k - Set dimensions range 1, 2, 3, .., n * 2")
            # self.__search_knn()

        elif self.__matches(action, ["p", "6"]):

            print("Display visual")
            # self.__graph()

        else:
            print("Unrecognized input, try again!")

        self.__default_loop()


if __name__ == "__main__":
    CLI()
