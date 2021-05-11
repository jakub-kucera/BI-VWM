import os
import pathlib
from typing import List

from PIL import Image
from matplotlib import pyplot as plt, patches

from rtree.default_config import WORKING_DIRECTORY
from rtree.mbb import MBBDim, MBB
from rtree.node import Node
from rtree.rtree import RTree

from rtree import rtree, database_entry


# def visualize(r_tree: RTree, output_img: str = "testing_img.svg"):
def visualize(r_tree: RTree, output_img: str = "testing_img.png"):
    """Creates and image which visualizes the rtree nodes and database entries."""

    # check if r-tree can be visualized
    if r_tree.dimensions != 2:  # add support for more dimension == 1 and 3
        raise ValueError(f"Cannot make visualization for Rtree with dimension of {r_tree.dimensions}")

    # remove old file if exists
    if os.path.isfile(output_img):
        os.remove(output_img)

    # creates empty plot
    fig, ax = plt.subplots()
    ax.plot([0, 0], [0, 0])

    database_entries_addresses: List[int] = []
    # gets all the rtree nodes
    nodes = r_tree.get_all_nodes()

    for node in nodes:
        box = node.mbb.box

        if node.is_leaf:
            # gets ids of database entries
            database_entries_addresses.extend(node.entries)
            pass
        else:
            # Create a Rectangle and add it to plot
            node_rect = patches.Rectangle((box[0].low, box[1].low), box[0].high - box[0].low,
                                          box[1].high - box[1].low,
                                          linewidth=1, edgecolor='r', facecolor='None')
            ax.add_patch(node_rect)

    for db_entry_address in database_entries_addresses:
        entry = r_tree.database.search(db_entry_address)
        if entry is None:
            raise ValueError(f"Database entry not found. Entry address = {db_entry_address}")

        coordinates = entry.coordinates

        if len(coordinates) != r_tree.dimensions:
            raise ValueError(f"Database entry has dimension of {len(coordinates)}, \
            but rtree has dimension of {r_tree.dimensions}")

        plt.plot(105, 200, 'bo')

    plt.axis('off')
    plt.gca().set_position([0, 0, 1, 1])
    plt.savefig(output_img)

    # opens the image. (Doesnt support SVG)
    img = Image.open(output_img)
    img.show()


def delete_saved_rtree():
    file_dir = pathlib.Path(WORKING_DIRECTORY)
    for entry in file_dir.iterdir():
        if entry.is_file():
            # print(entry)
            os.remove(entry)


if __name__ == '__main__':
    print("hello, friend")
    # print(cpu_count())
    tree = RTree()
    print("tree.trunk_id")
    print(tree.trunk_id)
    print("tree.get_node(tree.trunk_id)")
    print(tree.get_node(tree.trunk_id))
    # visualize(r_tree=tree)
    # delete_saved_rtree()

"""
Non-leaf node:
(leaf_flag), N * [min, max], K * id
Leaf node:
(leaf_flag), N * [min, max], K * byte address

Database entry:
flag(marked as deleted), N * [min, max], (pickled) data
"""

"""
todo suggestions:
Database
RTREE main functionality (high level stuff)
Load from files RTree
Rebuilding
Cache

Demo CLI interface for user

test search speed with linear search
    a) directly go through database file. Slow. Have to call pickle all the time
    b) pre-generate a file with only coordinates/box and byte address to database (dont count into measured time)
        measure time going through this new file

MatPlotLib

(insert DatabaseEntry from JSON)
"""
