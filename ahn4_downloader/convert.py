"""Convert LAZ files to LAS using laspy (chunked streaming to avoid OOM)."""

from pathlib import Path

import laspy
from tqdm import tqdm

# Number of points to read/write at a time. 10M points ≈ 300-500 MB RAM.
CHUNK_SIZE = 10_000_000


def convert_laz_to_las(
    input_dir: Path,
    output_dir: Path | None = None,
    workers: int = 4,  # kept for API compat, unused (sequential is safer)
) -> list[Path]:
    """Decompress every .laz in *input_dir* → .las via laspy.

    Uses chunked streaming so memory usage stays bounded even for
    multi-GB files. Files are processed sequentially to avoid OOM.

    If *output_dir* is None the .las files are written next to the originals.
    Returns a list of successfully created .las paths.
    """
    if output_dir is None:
        output_dir = input_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    laz_files = sorted(input_dir.glob("*.laz"))
    if not laz_files:
        print(f"No .laz files found in {input_dir}")
        return []

    print(f"Converting {len(laz_files)} LAZ file(s) → LAS …")
    results: list[Path] = []

    for laz_path in laz_files:
        path = _convert_one_chunked(laz_path, output_dir)
        if path is not None:
            results.append(path)

    print(f"Done — {len(results)}/{len(laz_files)} converted successfully.")
    return results


def _convert_one_chunked(laz_path: Path, output_dir: Path) -> Path | None:
    """Stream-convert a single LAZ → LAS in fixed-size chunks."""
    las_path = output_dir / laz_path.with_suffix(".las").name
    if las_path.exists() and las_path.stat().st_size > 0:
        tqdm.write(f"  {las_path.name} already exists — skipped")
        return las_path

    try:
        with laspy.open(str(laz_path)) as reader:
            header = reader.header
            total_points = header.point_count

            with laspy.open(str(las_path), mode="w", header=header) as writer:
                with tqdm(total=total_points, unit=" pts", unit_scale=True,
                          desc=f"  {laz_path.name}") as bar:
                    for chunk in reader.chunk_iterator(CHUNK_SIZE):
                        writer.write_points(chunk)
                        bar.update(len(chunk))

        return las_path
    except Exception as exc:
        tqdm.write(f"Failed: {laz_path.name} — {exc}")
        # Clean up partial file
        if las_path.exists():
            las_path.unlink()
        return None
