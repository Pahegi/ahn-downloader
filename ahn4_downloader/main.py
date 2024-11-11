#!/usr/bin/env python3
"""AHN4 tile downloader — CLI & GUI interface.

Examples
--------
# GUI mode (original behaviour):
  python main.py gui --output ./data

# CLI: select tiles by bounding box:
  python main.py select --bbox 155000 463000 160000 468000

# CLI: contiguous area around a point, budget 20 GB:
  python main.py select --center 155000 463000 --max-size 20

# Download previously selected tiles:
  python main.py download --bbox 155000 463000 160000 468000 --output ./data

# Full pipeline: select → download → convert → merge:
  python main.py pipeline --center 155000 463000 --max-size 20 --output ./data

# Convert already-downloaded LAZ files to LAS:
  python main.py convert --input ./data --output ./data/las

# Merge files into chunks:
  python main.py merge --input ./data --output ./data/merged --chunk-size 4
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from .tiles import TileIndex


# ──────────────────────────────────────────────────────────────────────
# Argument parsing
# ──────────────────────────────────────────────────────────────────────

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ahn4",
        description="Download, convert & merge AHN4 point-cloud tiles.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # --- select -------------------------------------------------------
    p_sel = sub.add_parser("select", help="Show which tiles match a spatial query")
    _add_spatial_args(p_sel)
    p_sel.add_argument("--query-sizes", action="store_true", default=False,
                       help="Query actual remote file sizes via HEAD requests")

    # --- download -----------------------------------------------------
    p_dl = sub.add_parser("download", help="Download LAZ tiles")
    _add_spatial_args(p_dl)
    p_dl.add_argument("-o", "--output", type=Path, required=True, help="Download directory")
    p_dl.add_argument("-t", "--threads", type=int, default=5, help="Parallel downloads")
    p_dl.add_argument("--las", action="store_true", help="Convert to LAS after downloading")
    p_dl.add_argument("-w", "--workers", type=int, default=4, help="Parallel conversion workers (with --las)")

    # --- convert ------------------------------------------------------
    p_conv = sub.add_parser("convert", help="Convert LAZ → LAS (laspy)")
    p_conv.add_argument("-i", "--input", type=Path, required=True, help="Directory with .laz files")
    p_conv.add_argument("-o", "--output", type=Path, default=None, help="Output dir (default: same as input)")
    p_conv.add_argument("-w", "--workers", type=int, default=4, help="Parallel worker processes")

    # --- merge --------------------------------------------------------
    p_merge = sub.add_parser("merge", help="Merge LAZ/LAS into chunks (requires PDAL)")
    p_merge.add_argument("-i", "--input", type=Path, required=True, help="Directory with files to merge")
    p_merge.add_argument("-o", "--output", type=Path, required=True, help="Output directory")
    p_merge.add_argument("--chunk-size", type=int, default=2, help="Files per chunk")
    p_merge.add_argument("--ext", default=".laz", help="File extension to merge")

    # --- pipeline -----------------------------------------------------
    p_pipe = sub.add_parser("pipeline", help="Select → download → convert  (full run)")
    _add_spatial_args(p_pipe)
    p_pipe.add_argument("-o", "--output", type=Path, required=True, help="Base output directory")
    p_pipe.add_argument("-t", "--threads", type=int, default=5, help="Download threads")
    p_pipe.add_argument("-w", "--workers", type=int, default=4, help="Convert worker processes")
    p_pipe.add_argument("--no-convert", action="store_true", help="Skip LAZ → LAS conversion")

    # --- gui ----------------------------------------------------------
    p_gui = sub.add_parser("gui", help="Interactive map selector (matplotlib)")
    p_gui.add_argument("-o", "--output", type=Path, default=None,
                       help="Download directory (if omitted, only shows selection)")
    p_gui.add_argument("-t", "--threads", type=int, default=5)

    return parser


def _add_spatial_args(p: argparse.ArgumentParser):
    """Add mutually-exclusive --bbox / --center spatial selectors."""
    g = p.add_argument_group("spatial selection (pick one)")
    excl = g.add_mutually_exclusive_group(required=True)
    excl.add_argument("--bbox", nargs=4, type=float, metavar=("XMIN", "YMIN", "XMAX", "YMAX"),
                      help="Axis-aligned bounding box (EPSG:28992)")
    excl.add_argument("--center", nargs=2, type=float, metavar=("X", "Y"),
                      help="Center point for contiguous expansion")
    g.add_argument("--max-size", type=float, default=None,
                   help="Maximum total LAZ size in GB (required with --center unless --max-las-size is set)")
    g.add_argument("--max-las-size", type=float, default=None,
                   help="Maximum total LAS size in GB (converted to LAZ budget using ~5x ratio)")


# ──────────────────────────────────────────────────────────────────────
# Sub-command handlers
# ──────────────────────────────────────────────────────────────────────

def _get_max_size(args) -> float | None:
    """Resolve --max-size / --max-las-size into a LAZ budget in GB."""
    if args.max_size is not None and args.max_las_size is not None:
        print("ERROR: specify either --max-size or --max-las-size, not both", file=sys.stderr)
        sys.exit(1)
    if args.max_las_size is not None:
        return args.max_las_size / 5.0
    return args.max_size


def _resolve_tiles(args, index: TileIndex):
    """Return list of Tile objects from CLI spatial args."""
    max_size = _get_max_size(args)
    if args.bbox:
        tiles = index.select_contiguous_by_bbox(*args.bbox, max_size_gb=max_size,
                                                 query_sizes=True)
    elif args.center:
        if max_size is None:
            print("ERROR: --max-size or --max-las-size is required when using --center", file=sys.stderr)
            sys.exit(1)
        tiles = index.select_contiguous(*args.center, max_size_gb=max_size,
                                         query_sizes=True)
    else:
        tiles = []
    return tiles


def cmd_select(args):
    index = TileIndex()
    tiles = _resolve_tiles(args, index)
    if args.query_sizes:
        index.fetch_remote_sizes(tiles)
    print(index.summary(tiles))


def cmd_download(args):
    from .download import download_tiles

    index = TileIndex()
    tiles = _resolve_tiles(args, index)
    print(index.summary(tiles))
    if not tiles:
        return
    download_tiles(tiles, args.output, threads=args.threads)
    print("Download complete.")

    if args.las:
        from .convert import convert_laz_to_las
        las_dir = args.output / "las"
        convert_laz_to_las(args.output, las_dir, workers=args.workers)


def cmd_convert(args):
    from .convert import convert_laz_to_las
    convert_laz_to_las(args.input, args.output, workers=args.workers)


def cmd_merge(args):
    from .merge import merge_tiles
    merge_tiles(args.input, args.output, chunk_size=args.chunk_size, extension=args.ext)


def cmd_pipeline(args):
    from .download import download_tiles
    from .convert import convert_laz_to_las

    index = TileIndex()
    tiles = _resolve_tiles(args, index)
    index.fetch_remote_sizes(tiles)
    print(index.summary(tiles))
    if not tiles:
        return

    laz_dir = args.output / "laz"
    download_tiles(tiles, laz_dir, threads=args.threads)

    if not args.no_convert:
        las_dir = args.output / "las"
        convert_laz_to_las(laz_dir, las_dir, workers=args.workers)


def cmd_gui(args):
    from .gui import gui_select
    from .download import download_tiles

    index = TileIndex()
    tiles = gui_select(index)
    if not tiles:
        print("No tiles selected.")
        return
    index.fetch_remote_sizes(tiles)
    print(index.summary(tiles))
    if args.output:
        download_tiles(tiles, args.output, threads=args.threads)


# ──────────────────────────────────────────────────────────────────────
# Entry point
# ──────────────────────────────────────────────────────────────────────

COMMANDS = {
    "select": cmd_select,
    "download": cmd_download,
    "convert": cmd_convert,
    "merge": cmd_merge,
    "pipeline": cmd_pipeline,
    "gui": cmd_gui,
}


def main():
    parser = build_parser()
    args = parser.parse_args()
    COMMANDS[args.command](args)


if __name__ == "__main__":
    main()
