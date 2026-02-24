"""Convert LAZ files to LAS using laspy (chunked streaming to avoid OOM)."""

from pathlib import Path

import laspy
from tqdm import tqdm

from .validation import is_valid_laz_file, get_bbox_info

# Number of points to read/write at a time. 10M points ≈ 300-500 MB RAM.
CHUNK_SIZE = 10_000_000


def convert_laz_to_las(
    input_dir: Path,
    output_dir: Path | None = None,
    workers: int = 4,  # kept for API compat, unused (sequential is safer)
    point_format: int | None = None,
    remove_empty: bool = False,
) -> list[Path]:
    """Decompress every .laz in *input_dir* → .las via laspy.

    Uses chunked streaming so memory usage stays bounded even for
    multi-GB files. Files are processed sequentially to avoid OOM.

    If *output_dir* is None the .las files are written next to the originals.
    If *point_format* is specified, converts to that point record format (e.g., 7).
    If *remove_empty* is True, deletes source LAZ files that are empty or invalid.
    Returns a list of successfully created .las paths.
    """
    if output_dir is None:
        output_dir = input_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    laz_files = sorted(input_dir.glob("*.laz"))
    if not laz_files:
        print(f"No .laz files found in {input_dir}")
        return []

    if point_format is not None:
        print(f"Converting {len(laz_files)} LAZ file(s) → LAS (format {point_format}) …")
    else:
        print(f"Converting {len(laz_files)} LAZ file(s) → LAS …")
    results: list[Path] = []
    removed_count = 0

    for laz_path in laz_files:
        path, should_remove = _convert_one_chunked(laz_path, output_dir, point_format=point_format)
        if path is not None:
            results.append(path)
        elif should_remove and remove_empty:
            laz_path.unlink()
            removed_count += 1
            tqdm.write(f"  Removed empty/invalid: {laz_path.name}")

    print(f"Done — {len(results)}/{len(laz_files)} converted successfully.")
    if removed_count > 0:
        print(f"Removed {removed_count} empty/invalid LAZ file(s).")
    return results


def _convert_one_chunked(laz_path: Path, output_dir: Path, point_format: int | None = None) -> tuple[Path | None, bool]:
    """Stream-convert a single LAZ → LAS in fixed-size chunks.
    
    Args:
        laz_path: Source LAZ file
        output_dir: Output directory for LAS file
        point_format: If specified, convert to this point record format
        
    Returns:
        Tuple of (output_path or None, should_remove_source)
        - output_path is None if conversion failed
        - should_remove_source is True if source file is empty/invalid
    """
    las_path = output_dir / laz_path.with_suffix(".las").name
    if las_path.exists() and las_path.stat().st_size > 0:
        tqdm.write(f"  {las_path.name} already exists — skipped")
        return las_path, False

    try:
        # Validate source file first
        is_valid, error_msg = is_valid_laz_file(laz_path)
        if not is_valid:
            tqdm.write(f"  {laz_path.name} is invalid ({error_msg}) — skipped")
            return None, True
        
        with laspy.open(str(laz_path)) as reader:
            header = reader.header
            total_points = header.point_count

            # Convert point format if requested
            if point_format is not None and header.point_format.id != point_format:
                # Need to read all data at once for format conversion
                las_data = reader.read()
                
                # Convert to target format
                converted_las = laspy.convert(las_data, point_format_id=point_format)
                
                with tqdm(total=1, unit=" file", desc=f"  {laz_path.name} (converting to format {point_format})") as bar:
                    with laspy.open(str(las_path), mode="w", header=converted_las.header) as writer:
                        writer.write_points(converted_las.points)
                    bar.update(1)
                    
            else:
                # Standard chunked conversion without format change
                with laspy.open(str(las_path), mode="w", header=header) as writer:
                    with tqdm(total=total_points, unit=" pts", unit_scale=True,
                              desc=f"  {laz_path.name}") as bar:
                        for chunk in reader.chunk_iterator(CHUNK_SIZE):
                            writer.write_points(chunk)
                            bar.update(len(chunk))

        return las_path, False
    except Exception as exc:
        tqdm.write(f"Failed: {laz_path.name} — {exc}")
        # Clean up partial file
        if las_path.exists():
            las_path.unlink()
        return None, False
