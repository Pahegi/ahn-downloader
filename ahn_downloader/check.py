"""Summarise and verify downloaded point-cloud files."""

from __future__ import annotations

from collections import Counter
from pathlib import Path

import laspy
from tqdm import tqdm

from .validation import is_valid_laz_file, get_bbox_info


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
    zero_bbox_files: list[str] = []

    # Track overall bounding box
    overall_x_min = overall_y_min = overall_z_min = float('inf')
    overall_x_max = overall_y_max = overall_z_max = float('-inf')

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

                # Check for zero bounding box using utility
                bbox = get_bbox_info(path)
                if bbox and (bbox[0] == 0 or bbox[1] == 0 or bbox[2] == 0):
                    zero_bbox_files.append(f"{path.name} (X={bbox[0]:.3f}, Y={bbox[1]:.3f}, Z={bbox[2]:.3f})")

                # Update overall bounding box
                overall_x_min = min(overall_x_min, hdr.x_min)
                overall_x_max = max(overall_x_max, hdr.x_max)
                overall_y_min = min(overall_y_min, hdr.y_min)
                overall_y_max = max(overall_y_max, hdr.y_max)
                overall_z_min = min(overall_z_min, hdr.z_min)
                overall_z_max = max(overall_z_max, hdr.z_max)

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

    # Bounding box info
    if overall_x_min != float('inf'):
        print(f"  Bbox X    : {overall_x_min:.2f} to {overall_x_max:.2f} (range: {overall_x_max - overall_x_min:.2f})")
        print(f"  Bbox Y    : {overall_y_min:.2f} to {overall_y_max:.2f} (range: {overall_y_max - overall_y_min:.2f})")
        print(f"  Bbox Z    : {overall_z_min:.2f} to {overall_z_max:.2f} (range: {overall_z_max - overall_z_min:.2f})")

    # Format consistency check
    ok = True
    if len(format_counts) > 1:
        print("  ⚠ WARNING : mixed point record formats detected!")
        ok = False

    if zero_bbox_files:
        print(f"  ⚠ WARNING : {len(zero_bbox_files)} file(s) with zero bounding box:")
        for msg in zero_bbox_files:
            print(f"              {msg}")
        ok = False

    if errors:
        for msg in errors:
            print(f"  ⚠ WARNING : could not read {msg}")
        ok = False

    print("────────────────────────────────────────────────────")
    return ok


def validate_files(directory: Path, verbose: bool = False, remove_invalid: bool = False) -> bool:
    """Thoroughly validate all LAZ/LAS files in *directory*.

    Checks file integrity by:
    - Verifying files can be opened
    - Reading all points to ensure data is not corrupted
    - Checking header consistency
    - Validating point counts match headers
    - Checking for zero-size bounding boxes

    Args:
        directory: Directory containing LAZ/LAS files
        verbose: Show validation status for each file
        remove_invalid: Automatically delete invalid files

    Returns True if all files are valid, False otherwise.
    """
    files = sorted([*directory.glob("*.laz"), *directory.glob("*.las")])
    if not files:
        print(f"No LAZ/LAS files found in {directory}")
        return True

    print()
    print("── Validating files ────────────────────────────────")
    print(f"  Directory : {directory.resolve()}")
    print(f"  Files     : {len(files)}")
    print()

    errors: list[tuple[Path, str]] = []
    valid_count = 0

    for path in tqdm(files, desc="Validating", unit="file"):
        try:
            # Quick validation check
            is_valid, error_msg = is_valid_laz_file(path)
            if not is_valid:
                errors.append((path, error_msg or "Unknown error"))
                if remove_invalid:
                    path.unlink()
                    tqdm.write(f"  Removed invalid: {path.name}")
                continue
            
            # Deep validation: read all points
            with laspy.open(str(path)) as reader:
                hdr = reader.header
                expected_points = hdr.point_count
                
                actual_points = 0
                for chunk in reader.chunk_iterator(10_000_000):
                    actual_points += len(chunk)
                
                # Verify point count matches header
                if actual_points != expected_points:
                    errors.append((
                        path,
                        f"Point count mismatch: header={expected_points:,}, actual={actual_points:,}"
                    ))
                    if remove_invalid:
                        path.unlink()
                        tqdm.write(f"  Removed invalid: {path.name}")
                elif verbose:
                    bbox = get_bbox_info(path)
                    if bbox:
                        bbox_info = f"bbox: X={bbox[0]:.1f}, Y={bbox[1]:.1f}, Z={bbox[2]:.1f}"
                        tqdm.write(f"  ✓ {path.name} ({expected_points:,} points, {bbox_info})")
                    else:
                        tqdm.write(f"  ✓ {path.name} ({expected_points:,} points)")
                    
                valid_count += 1

        except Exception as exc:
            errors.append((path, str(exc)))

    # Print results
    print()
    print(f"  Valid     : {valid_count}/{len(files)}")

    if errors:
        print(f"  Errors    : {len(errors)}")
        if remove_invalid:
            print(f"  Removed   : {len([e for e in errors if not Path(e[0]).exists()])}")
        print()
        print("── Errors ──────────────────────────────────────────")
        for path, msg in errors:
            status = "✗ (removed)" if remove_invalid and not path.exists() else "✗"
            print(f"  {status} {path.name}")
            print(f"    {msg}")
        print("────────────────────────────────────────────────────")
        return False
    else:
        print("  Status    : All files valid ✓")
        print("────────────────────────────────────────────────────")
        return True
