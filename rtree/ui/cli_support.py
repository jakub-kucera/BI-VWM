
def get_input():
    tmp = input(">> ")
    for x in ["exit", "e", "!"]:
        if x == tmp.lower():
            exit()
    for x in ["help", "h", "?"]:
        if x == tmp.lower():
            print_help()
            return get_input()
    return tmp


def matches(text, options) -> bool:
    for x in options:
        if x == text:
            return True
    return False


def is_num(value) -> bool:
    try:
        value = int(value)
    except TypeError:
        return False
    return True


def is_valid_db_name(value) -> bool:
    if type(value[0]) == int or len(value) <= 3:
        return False
    return type(value) == str


def print_help():
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
