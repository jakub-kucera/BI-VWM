import os
from typing import List

from PIL import Image
from matplotlib import pyplot as plt, patches

from rtree.mbb import MBBDim, MBB
from rtree.node import Node
from rtree.rtree import RTree

from rtree import rtree, database_entry
from rtree.node import Node
from rtree.tree_file_handler import TreeFileHandler


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

    database_entries_ids: List[int] = []
    # gets all the rtree nodes
    nodes = r_tree.get_all_nodes()

    for node in nodes:
        box = node.mbb.box

        if node.is_leaf:
            # gets ids of database entries
            database_entries_ids.extend(node.entries)
            pass
        else:
            # Create a Rectangle and add it to plot
            node_rect = patches.Rectangle((box[0].low, box[1].low), box[0].high - box[0].low,
                                          box[1].high - box[1].low,
                                          linewidth=1, edgecolor='r', facecolor='None')
            ax.add_patch(node_rect)

    # todo change when database is implemented
    # for db_entry_id in database_entries_ids:
    #     box = r_tree.database.search(db_entry_id).box
    #     entry_rect = patches.Rectangle((box[0].low, box[1].low), box[0].high - box[0].low, box[1].high - box[1].low,
    #                                    linewidth=1, edgecolor='b', facecolor='None')
    #     ax.add_patch(entry_rect)

    plt.axis('off')
    plt.gca().set_position([0, 0, 1, 1])
    plt.savefig(output_img)

    # opens the image. (Doesnt support SVG)
    img = Image.open(output_img)
    img.show()


if __name__ == '__main__':
    print("hello, friend")
    # print(cpu_count())
    tree = RTree()
    visualize(r_tree=tree)

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
