"""Summarise and verify downloaded point-cloud files."""

from __future__ import annotations

from collections import Counter
from pathlib import Path

import laspy


def _fmt_size(n_bytes: int | float) -> str:
    """Human-readable file size."""
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(n_bytes) < 1024:
            return f"{n_bytes:.1f} {unit}"
        n_bytes /= 1024
    return f"{n_bytes:.1f} PB"


def print_summary(directory: Path) -> bool:
    """Print a summary of all LAZ/LAS files in *directory*.

    Reports file counts / sizes, point totals, point record formats,
    and point attributes.  Returns False when a format mismatch is detected.
    """
    files = sorted([*directory.glob("*.laz"), *directory.glob("*.las")])
    if not files:
        return True

    laz_files: list[Path] = []
    las_files: list[Path] = []
    total_points = 0
    format_counts: Counter[int] = Counter()
    version_counts: Counter[str] = Counter()
    dimension_names: set[str] | None = None
    errors: list[str] = []

    for path in files:
        if path.suffix.lower() == ".laz":
            laz_files.append(path)
        else:
            las_files.append(path)

        try:
            with laspy.open(str(path)) as reader:
                hdr = reader.header
                total_points += hdr.point_count
                fmt_id = hdr.point_format.id
                format_counts[fmt_id] += 1
                version_counts[f"{hdr.version.major}.{hdr.version.minor}"] += 1

                dims = set(dim.name for dim in hdr.point_format.dimensions)
                if dimension_names is None:
                    dimension_names = dims
                else:
                    dimension_names &= dims
        except Exception as exc:
            errors.append(f"{path.name}: {exc}")

    # ── print ──
    print()
    print("── Summary ─────────────────────────────────────────")
    print(f"  Directory : {directory.resolve()}")

    if laz_files:
        laz_size = sum(f.stat().st_size for f in laz_files)
        print(f"  LAZ files : {len(laz_files):>6}   ({_fmt_size(laz_size)})")
    if las_files:
        las_size = sum(f.stat().st_size for f in las_files)
        print(f"  LAS files : {len(las_files):>6}   ({_fmt_size(las_size)})")

    print(f"  Points    : {total_points:,}")

    # Point record format(s)
    fmt_parts = [f"{fmt} (×{cnt})" for fmt, cnt in sorted(format_counts.items())]
    print(f"  Format    : {', '.join(fmt_parts)}")

    # LAS version(s)
    ver_parts = [f"{v} (×{cnt})" for v, cnt in sorted(version_counts.items())]
    print(f"  Version   : {', '.join(ver_parts)}")

    # Attributes shared across all files
    if dimension_names:
        print(f"  Attributes: {', '.join(sorted(dimension_names))}")

    # Format consistency check
    ok = True
    if len(format_counts) > 1:
        print("  ⚠ WARNING : mixed point record formats detected!")
        ok = False

    if errors:
        for msg in errors:
            print(f"  ⚠ WARNING : could not read {msg}")
        ok = False

    print("────────────────────────────────────────────────────")
    return ok
