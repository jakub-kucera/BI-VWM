import os
from typing import List

from PIL import Image
from matplotlib import pyplot as plt, patches

from rtree.rtree import RTree

VISUALIZER_MBB_COLOR = {0: 'r', 1: 'b', 2: 'g', 3: 'c', 4: 'm', 5: 'y'}


def visualize(r_tree: RTree, output_img: str = "testing_img.png", show_mbbs_only=True):
    """Creates and image which visualizes the rtree nodes and database entries."""

    # check if r-tree can be visualized
    if r_tree.dimensions != 2:  # add support for more dimension == 1 and 3
        raise ValueError(f"Cannot make visualization for Rtree with dimension of {r_tree.dimensions}")

    # remove old file if exists
    if os.path.isfile(output_img):
        os.remove(output_img)

    # creates empty plot
    fig, ax = plt.subplots()
    # ax.plot([0, 0], [0, 0])

    database_entries_addresses: List[int] = []
    # gets all the rtree nodes
    nodes = r_tree.get_all_nodes()
    print(f"Visualiser number of nodes: {len(nodes)}")

    for node, depth in nodes:
        box = node.mbb.box

        if node.is_leaf:
            # gets ids of database entries
            database_entries_addresses.extend(node.child_nodes)
        # else:
        # Create a Rectangle and add it to plot
        node_rect = patches.Rectangle((box[0].low, box[1].low), box[0].high - box[0].low,
                                      box[1].high - box[1].low,
                                      linewidth=1, edgecolor=VISUALIZER_MBB_COLOR[depth], facecolor='None')
        # linewidth=1, edgecolor='r', facecolor='None')
        plt.text(box[0].low, box[1].low, node.id)
        ax.add_patch(node_rect)

    if not show_mbbs_only:
        for db_entry_address in database_entries_addresses:
            entry = r_tree.database.search(db_entry_address)
            if entry is None:
                raise ValueError(f"Database entry not found. Entry address = {db_entry_address}")

            coordinates = entry.coordinates

            if len(coordinates) != r_tree.dimensions:
                raise ValueError(f"Database entry has dimension of {len(coordinates)}, \
                but rtree has dimension of {r_tree.dimensions}")

            plt.plot(coordinates[0], coordinates[1], 'bo')
    else:
        plt.plot()
    # plt.axis('off')
    # plt.gca().set_position([0, 0, 1, 1])
    # plt.savefig(output_img)
    plt.show()

    # opens the image. (Doesnt support SVG)
    # img = Image.open(output_img)
    # img.show()
