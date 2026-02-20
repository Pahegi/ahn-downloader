"""Tile index loading, spatial queries, and contiguous area selection."""

import json
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from requests.exceptions import RequestException
from shapely.geometry import Polygon, Point, box
from shapely.strtree import STRtree
from tqdm import tqdm

INDEX_PATH = Path(__file__).parent / "index.json"
FALLBACK_TILE_SIZE_GB = 4.4      # full AHN4 tile, used when remote HEAD fails
FALLBACK_SUBTILE_SIZE_GB = 0.3   # one AHN5 colored sub-tile (~300 MiB)


class DataSource(Enum):
    """Available point-cloud data sources."""

    AHN4 = "ahn4"
    AHN5_COLORED = "ahn5-colored"


# Max parallel downloads per source (geotiles enforces a 10-connection limit)
SOURCE_MAX_THREADS = {
    DataSource.AHN4: 5,
    DataSource.AHN5_COLORED: 8,
}

# Sub-tile suffixes for geotiles colored products (5×5 grid → 25 sub-tiles)
SUBTILE_SUFFIXES = [f"{i:02d}" for i in range(1, 26)]


@dataclass
class Tile:
    """Represents a single AHN map tile."""

    name: str
    polygon: Polygon
    source: DataSource = field(default=DataSource.AHN4)
    subtile: str | None = field(default=None)
    centroid: Point = field(init=False)
    size_bytes: int | None = field(default=None, repr=False)

    def __post_init__(self):
        self.centroid = self.polygon.centroid

    @property
    def download_url(self) -> str:
        if self.source == DataSource.AHN5_COLORED:
            suffix = self.subtile or "01"
            return (
                f"https://geotiles.citg.tudelft.nl/AHN5_T/"
                f"{self.name}_{suffix}.LAZ"
            )
        return f"https://basisdata.nl/hwh-ahn/ahn4/01_LAZ/C_{self.name}.LAZ"

    @property
    def filename(self) -> str:
        if self.source == DataSource.AHN5_COLORED:
            suffix = self.subtile or "01"
            return f"{self.name}_{suffix}.laz"
        return f"C_{self.name}.laz"

    @property
    def size_gb(self) -> float:
        """Size in GB (uses fallback when not yet queried).

        For AHN5 colored main tiles (before sub-tile expansion), the queried
        size represents a single sub-tile.  Multiply by 25 so the BFS budget
        accounts for all sub-tiles that will be downloaded.
        """
        if self.size_bytes is not None:
            gb = self.size_bytes / (1024 ** 3)
            if self.source == DataSource.AHN5_COLORED and self.subtile is None:
                gb *= 25
            return gb
        if self.source == DataSource.AHN5_COLORED:
            if self.subtile is None:
                return FALLBACK_SUBTILE_SIZE_GB * 25
            return FALLBACK_SUBTILE_SIZE_GB
        return FALLBACK_TILE_SIZE_GB

    def expand_subtiles(self) -> list["Tile"]:
        """Expand this tile into 25 colored sub-tiles (AHN5 only)."""
        if self.source != DataSource.AHN5_COLORED:
            return [self]
        return [
            Tile(
                name=self.name,
                polygon=self.polygon,
                source=self.source,
                subtile=suffix,
            )
            for suffix in SUBTILE_SUFFIXES
        ]


class TileIndex:
    """Loads and queries the AHN tile index."""

    def __init__(self, index_path: Path = INDEX_PATH, source: DataSource = DataSource.AHN4):
        self.source = source
        self.tiles: list[Tile] = []
        self._load(index_path)
        # Build spatial index for fast intersection queries
        self._tree = STRtree([t.polygon for t in self.tiles])

    def _load(self, path: Path):
        with open(path) as f:
            data = json.load(f)
        for feature in data["features"]:
            name = feature["properties"]["kaartbladNr"]
            # Strip the "M_" or similar 2-char prefix used in the DTM index;
            # LAZ tiles use the same grid names.
            name = name[2:]
            coords = feature["geometry"]["coordinates"][0]
            polygon = Polygon(coords)
            if polygon.is_valid and not polygon.is_empty:
                self.tiles.append(Tile(name=name, polygon=polygon, source=self.source))

    # ------------------------------------------------------------------
    # Remote size queries
    # ------------------------------------------------------------------

    def fetch_remote_sizes(
        self,
        tiles: list[Tile],
        threads: int = 20,
    ) -> None:
        """Query Content-Length for each tile via HEAD requests (parallel).

        Results are stored on each `Tile.size_bytes`.
        """
        tiles_to_query = [t for t in tiles if t.size_bytes is None]
        if not tiles_to_query:
            return

        def _head(tile: Tile) -> tuple[Tile, int | None]:
            try:
                r = requests.head(tile.download_url, timeout=15,
                                  allow_redirects=True)
                r.raise_for_status()
                return tile, int(r.headers.get("content-length", 0)) or None
            except RequestException:
                return tile, None

        with ThreadPoolExecutor(max_workers=threads) as pool:
            futs = {pool.submit(_head, t): t for t in tiles_to_query}
            for fut in tqdm(as_completed(futs), total=len(futs), desc="Querying sizes", leave=False):
                tile, size = fut.result()
                tile.size_bytes = size

    # ------------------------------------------------------------------
    # Query helpers
    # ------------------------------------------------------------------

    def find_by_point(self, x: float, y: float) -> Tile | None:
        """Return the tile containing the given point, or None."""
        pt = Point(x, y)
        for idx in self._tree.query(pt):
            if self.tiles[idx].polygon.contains(pt):
                return self.tiles[idx]
        return None

    def find_intersecting(self, geometry) -> list[Tile]:
        """Return all tiles whose polygons intersect *geometry*."""
        hits = self._tree.query(geometry)
        return [self.tiles[i] for i in hits if self.tiles[i].polygon.intersects(geometry)]

    def find_by_bbox(self, xmin: float, ymin: float, xmax: float, ymax: float) -> list[Tile]:
        """Return tiles intersecting an axis-aligned bounding box."""
        return self.find_intersecting(box(xmin, ymin, xmax, ymax))

    # ------------------------------------------------------------------
    # Contiguous expansion with a memory budget
    # ------------------------------------------------------------------

    def select_contiguous(
        self,
        center_x: float,
        center_y: float,
        max_size_gb: float,
        query_sizes: bool = True,
    ) -> list[Tile]:
        """BFS-expand outward from *center* choosing contiguous tiles that
        fit within *max_size_gb* (using real remote file sizes).

        Algorithm
        ---------
        1. Find the seed tile that contains (center_x, center_y).
        2. Maintain a BFS queue ordered by distance from center.
        3. For each candidate, if adding it stays within budget, accept it
           and enqueue its spatial neighbours (tiles sharing an edge/overlap).
        4. Return the accepted set — guaranteed contiguous.
        """
        seed = self.find_by_point(center_x, center_y)
        if seed is None:
            # Fall back to nearest tile
            dists = [(t, t.centroid.distance(Point(center_x, center_y))) for t in self.tiles]
            dists.sort(key=lambda x: x[1])
            if not dists:
                return []
            seed = dists[0][0]

        center_pt = Point(center_x, center_y)

        selected_list: list[Tile] = []
        used_gb: float = 0
        visited: set[str] = set()

        queue: list[tuple[float, Tile]] = [(seed.centroid.distance(center_pt), seed)]
        visited.add(seed.name)

        while queue:
            queue.sort(key=lambda x: x[0])
            _, tile = queue.pop(0)

            # Query size on the fly if unknown
            if query_sizes and tile.size_bytes is None:
                self.fetch_remote_sizes([tile])

            tile_gb = tile.size_gb
            if used_gb + tile_gb > max_size_gb:
                continue  # skip but keep checking smaller neighbours

            used_gb += tile_gb
            selected_list.append(tile)

            # Find neighbours: tiles that touch or overlap
            buffered = tile.polygon.buffer(1)  # 1-metre buffer catches shared edges
            neighbours = self.find_intersecting(buffered)
            for nb in neighbours:
                if nb.name not in visited:
                    visited.add(nb.name)
                    dist = nb.centroid.distance(center_pt)
                    queue.append((dist, nb))

        return selected_list

    def select_contiguous_by_bbox(
        self,
        xmin: float,
        ymin: float,
        xmax: float,
        ymax: float,
        max_size_gb: float | None = None,
        query_sizes: bool = True,
    ) -> list[Tile]:
        """Select tiles inside a bbox, optionally trimming to *max_size_gb*
        by keeping tiles closest to the bbox centre (using real sizes)."""
        bbox_geom = box(xmin, ymin, xmax, ymax)
        tiles = self.find_intersecting(bbox_geom)

        if max_size_gb is not None:
            if query_sizes:
                self.fetch_remote_sizes(tiles)

            center = bbox_geom.centroid
            tiles.sort(key=lambda t: t.centroid.distance(center))
            kept: list[Tile] = []
            used = 0.0
            for t in tiles:
                t_gb = t.size_gb
                if used + t_gb <= max_size_gb:
                    used += t_gb
                    kept.append(t)
            tiles = kept

        return tiles

    # ------------------------------------------------------------------
    # Utility
    # ------------------------------------------------------------------

    def estimate_size(self, tiles: list[Tile]) -> float:
        """Total LAZ size in GB (uses real sizes where available)."""
        return sum(t.size_gb for t in tiles)

    def summary(self, tiles: list[Tile]) -> str:
        est_laz = self.estimate_size(tiles)
        has_real = all(t.size_bytes is not None for t in tiles)
        qualifier = "" if has_real else " (estimated)"
        est_las = est_laz * 5
        source_label = self.source.value
        lines = [
            f"Source         : {source_label}",
            f"Tiles selected : {len(tiles)}",
            f"LAZ size{qualifier:8s}: {est_laz:.2f} GB",
            f"LAS size{qualifier:8s}: {est_las:.2f} GB",
        ]
        if tiles:
            names = sorted({t.name for t in tiles})
            lines.append(f"Tile names     : {', '.join(names)}")
        return "\n".join(lines)
