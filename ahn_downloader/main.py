#!/usr/bin/env python3
"""Download AHN point-cloud tiles (Dutch national height model).

By default downloads AHN5 colored tiles (with RGB) around Amsterdam.

Examples
--------
  ahn-downloader -o ./data                        # AHN5 colored, Amsterdam, 10 GB LAZ
  ahn-downloader -o ./data --las                  # same, converted to LAS
  ahn-downloader -o ./data --las --point-format 7 # convert to LAS format 7
  ahn-downloader -o ./data --validate             # validate files after download
  ahn-downloader -o ./data --validate --verbose   # validate with detailed output
  ahn-downloader --laz-output ./laz --las-output ./las --las  # separate LAZ and LAS dirs
  ahn-downloader -o ./data --max-size 20          # bigger area
  ahn-downloader -o ./data --source ahn4          # original AHN4 (no color)
  ahn-downloader -o ./data --bbox 119000 485000 123000 489000
  ahn-downloader --dry-run                        # preview tile selection

  ahn-downloader convert -i ./data -o ./data/las  # convert existing LAZ → LAS
  ahn-downloader convert -i ./data -o ./las --point-format 7  # convert to format 7
  ahn-downloader convert -i ./data -o ./las --remove-empty    # remove invalid LAZ files
  ahn-downloader merge   -i ./data -o ./merged    # merge tiles (requires PDAL)
  ahn-downloader gui     -o ./data                # interactive map selector
  ahn-downloader validate -i ./data               # validate file integrity
  ahn-downloader validate -i ./data --remove-invalid  # validate and delete bad files
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from .tiles import TileIndex, DataSource, SOURCE_MAX_THREADS

# Amsterdam Centraal in EPSG:28992
AMSTERDAM_RD = [121000.0, 487000.0]
DEFAULT_MAX_SIZE_GB = 10.0


# ──────────────────────────────────────────────────────────────────────
# Argument parsing
# ──────────────────────────────────────────────────────────────────────

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ahn-downloader",
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    # Download is the default action — these args live on the root parser
    parser.add_argument("-o", "--output", type=Path, default=None,
                        help="Download directory (required unless --dry-run). For compatibility; prefer --laz-output")
    parser.add_argument("--laz-output", type=Path, default=None,
                        help="LAZ download directory (defaults to --output)")
    parser.add_argument("--las-output", type=Path, default=None,
                        help="LAS output directory (defaults to --output/las or --laz-output/las)")
    parser.add_argument("--source", choices=[s.value for s in DataSource],
                        default=DataSource.AHN5_COLORED.value,
                        help="Data source (default: ahn5-colored)")
    parser.add_argument("--las", action="store_true",
                        help="Convert to LAS after downloading")
    parser.add_argument("--dry-run", action="store_true",
                        help="Only show tile selection, don't download")
    parser.add_argument("-t", "--threads", type=int, default=None,
                        help="Parallel downloads (auto-limited per source)")
    parser.add_argument("-w", "--workers", type=int, default=4,
                        help="Parallel LAZ→LAS conversion workers")
    parser.add_argument("--validate", action="store_true",
                        help="Validate file integrity after download/conversion")
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="Show detailed validation output (use with --validate)")
    parser.add_argument("--point-format", type=int, default=None,
                        metavar="FORMAT",
                        help="LAS point record format for conversion (e.g., 6, 7, 8). Use with --las")

    # Spatial selection
    area = parser.add_argument_group("area selection")
    excl = area.add_mutually_exclusive_group()
    excl.add_argument("--bbox", nargs=4, type=float,
                      metavar=("XMIN", "YMIN", "XMAX", "YMAX"),
                      help="Bounding box in EPSG:28992")
    excl.add_argument("--center", nargs=2, type=float, metavar=("X", "Y"),
                      default=AMSTERDAM_RD,
                      help="Center point (default: Amsterdam, 121000 487000)")
    area.add_argument("--max-size", type=float, default=None,
                      help=f"Max LAZ size in GB (default: {DEFAULT_MAX_SIZE_GB})")
    # Utility sub-commands
    sub = parser.add_subparsers(dest="command")

    p_conv = sub.add_parser("convert", help="Convert LAZ → LAS")
    p_conv.add_argument("-i", "--input", type=Path, required=True)
    p_conv.add_argument("-o", "--output", type=Path, default=None)
    p_conv.add_argument("-w", "--workers", type=int, default=4)
    p_conv.add_argument("--point-format", type=int, default=None,
                        metavar="FORMAT",
                        help="LAS point record format (e.g., 6, 7, 8)")
    p_conv.add_argument("--remove-empty", action="store_true",
                        help="Delete empty/invalid source LAZ files")

    p_merge = sub.add_parser("merge", help="Merge tiles into chunks (PDAL)")
    p_merge.add_argument("-i", "--input", type=Path, required=True)
    p_merge.add_argument("-o", "--output", type=Path, required=True)
    p_merge.add_argument("--chunk-size", type=int, default=2)
    p_merge.add_argument("--ext", default=".laz")

    p_gui = sub.add_parser("gui", help="Interactive map selector")
    p_gui.add_argument("-o", "--output", type=Path, default=None)
    p_gui.add_argument("--source", choices=[s.value for s in DataSource],
                       default=DataSource.AHN5_COLORED.value)
    p_gui.add_argument("-t", "--threads", type=int, default=None)

    p_validate = sub.add_parser("validate", help="Validate LAZ/LAS file integrity")
    p_validate.add_argument("-i", "--input", type=Path, required=True,
                            help="Directory containing LAZ/LAS files")
    p_validate.add_argument("-v", "--verbose", action="store_true",
                            help="Show validation status for each file")
    p_validate.add_argument("--remove-invalid", action="store_true",
                            help="Delete invalid files")

    return parser


# ──────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────

def _get_source(args) -> DataSource:
    return DataSource(args.source)


def _get_threads(args, source: DataSource) -> int:
    limit = SOURCE_MAX_THREADS.get(source, 5)
    if args.threads is not None:
        return min(args.threads, limit)
    return limit


def _resolve_tiles(args, index: TileIndex) -> list:
    max_size = args.max_size if args.max_size is not None else DEFAULT_MAX_SIZE_GB

    if args.bbox:
        tiles = index.select_contiguous_by_bbox(*args.bbox, max_size_gb=max_size,
                                                 query_sizes=True)
    else:
        tiles = index.select_contiguous(*args.center, max_size_gb=max_size,
                                         query_sizes=True)

    if index.source == DataSource.AHN5_COLORED:
        tiles = [st for t in tiles for st in t.expand_subtiles()]

    return tiles


# ──────────────────────────────────────────────────────────────────────
# Commands
# ──────────────────────────────────────────────────────────────────────

def cmd_download(args):
    """Default action: select tiles → download → optionally convert."""
    from .download import download_tiles

    source = _get_source(args)
    index = TileIndex(source=source)
    tiles = _resolve_tiles(args, index)
    print(index.summary(tiles))

    if args.dry_run or not tiles:
        return

    # Determine LAZ output directory
    laz_dir = args.laz_output or args.output
    if laz_dir is None:
        print("ERROR: -o/--output or --laz-output is required (or use --dry-run)", file=sys.stderr)
        sys.exit(1)

    threads = _get_threads(args, source)
    download_tiles(tiles, laz_dir, threads=threads)
    print("Download complete.")

    from .check import print_summary, validate_files
    print_summary(laz_dir)

    if args.validate:
        if not validate_files(laz_dir, verbose=args.verbose):
            print("⚠ WARNING: Some LAZ files failed validation", file=sys.stderr)

    if args.las:
        from .convert import convert_laz_to_las
        # Determine LAS output directory
        las_dir = args.las_output or (laz_dir / "las")
        convert_laz_to_las(laz_dir, las_dir, workers=args.workers, point_format=args.point_format)
        print_summary(las_dir)

        if args.validate:
            if not validate_files(las_dir, verbose=args.verbose):
                print("⚠ WARNING: Some LAS files failed validation", file=sys.stderr)


def cmd_convert(args):
    from .convert import convert_laz_to_las
    out = args.output if args.output is not None else args.input
    convert_laz_to_las(args.input, args.output, workers=args.workers, 
                       point_format=args.point_format, remove_empty=args.remove_empty)

    from .check import print_summary
    print_summary(out)


def cmd_merge(args):
    from .merge import merge_tiles
    merge_tiles(args.input, args.output, chunk_size=args.chunk_size, extension=args.ext)


def cmd_gui(args):
    from .gui import gui_select
    from .download import download_tiles

    source = _get_source(args)
    index = TileIndex(source=source)
    tiles = gui_select(index)
    if not tiles:
        print("No tiles selected.")
        return

    if source == DataSource.AHN5_COLORED:
        tiles = [st for t in tiles for st in t.expand_subtiles()]

    index.fetch_remote_sizes(tiles)
    print(index.summary(tiles))
    if args.output:
        threads = _get_threads(args, source)
        download_tiles(tiles, args.output, threads=threads)

        from .check import print_summary
        print_summary(args.output)


def cmd_validate(args):
    from .check import validate_files
    valid = validate_files(args.input, verbose=args.verbose, remove_invalid=args.remove_invalid)
    sys.exit(0 if valid else 1)


# ──────────────────────────────────────────────────────────────────────
# Entry point
# ──────────────────────────────────────────────────────────────────────

COMMANDS = {
    "convert": cmd_convert,
    "merge": cmd_merge,
    "gui": cmd_gui,
    "validate": cmd_validate,
}


def main():
    parser = build_parser()
    args = parser.parse_args()
    if args.command in COMMANDS:
        COMMANDS[args.command](args)
    else:
        cmd_download(args)


if __name__ == "__main__":
    main()
