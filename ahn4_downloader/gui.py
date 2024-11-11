"""Interactive matplotlib-based tile selector (GUI mode)."""

import matplotlib.pyplot as plt
from matplotlib.widgets import PolygonSelector, Button

from .tiles import TileIndex, Tile


def gui_select(index: TileIndex) -> list[Tile]:
    """Open a matplotlib window for drawing a polygon selection.

    Returns the list of tiles intersecting the user-drawn polygon.
    """
    fig, ax = plt.subplots()

    # Draw the tile grid
    for tile in index.tiles:
        x, y = tile.polygon.exterior.xy
        ax.plot(x, y, color="black", linewidth=0.3)

    selected_tiles: list[Tile] = []

    def onselect(verts):
        nonlocal selected_tiles
        from shapely.geometry import Polygon as SPoly

        verts = [(float(x), float(y)) for x, y in verts]
        selection_poly = SPoly(verts)
        selected_tiles = index.find_intersecting(selection_poly)

        # Redraw highlights
        for tile in index.tiles:
            x, y = tile.polygon.exterior.xy
            if tile in selected_tiles:
                ax.fill(x, y, color="red", alpha=0.4)
            else:
                ax.fill(x, y, color="white", alpha=0.8)

        est_laz = index.estimate_size(selected_tiles)
        plt.title(
            f"Selected: {len(selected_tiles)} tiles\n"
            f"Est. LAZ: {est_laz:.2f} GB  |  Est. LAS: {est_laz * 5:.2f} GB"
        )
        plt.draw()

    def on_save(event):
        plt.close(fig)

    ax_save = fig.add_axes([0.2, 0.8, 0.1, 0.075])
    btn_save = Button(ax_save, "Save")
    btn_save.on_clicked(on_save)

    _selector = PolygonSelector(ax, onselect, useblit=True)
    plt.show()

    return selected_tiles
