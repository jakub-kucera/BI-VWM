
class BinaryFileReader:  # delete, put into rtree, database will work differently
    def __init__(self):
        self.offset_size = 0
        # self.id_size = 8 # id not stored in files
        self.block_size = 8 + 4 + 4  # id, x y
        self.current_block = self.offset_size

    # moves the reading head to the beginning of a block with a given id
    def get_block(self, block_id: int):
        # todo bound check
        self.current_block = self.offset_size + block_id * self.block_size
        return self.current_block

    # todo maybe create iterator/generator

    # moves reading head by given number of blocks
    def move(self, move_by: int = 1):
        # todo bound check
        self.current_block += move_by * self.block_size
        return self.current_block
