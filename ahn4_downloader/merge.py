"""Merge multiple LAZ/LAS files into larger chunks using PDAL."""

import subprocess
from pathlib import Path

from tqdm import tqdm


def merge_tiles(
    input_dir: Path,
    output_dir: Path,
    chunk_size: int = 2,
    extension: str = ".laz",
) -> list[Path]:
    """Merge point-cloud files in *input_dir* into chunks of *chunk_size*.

    Each chunk is written as ``merged_<index>.laz`` in *output_dir*.
    Returns the list of successfully created output files.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    files = sorted(input_dir.glob(f"*{extension}"))

    if not files:
        print(f"No {extension} files found in {input_dir}")
        return []

    results: list[Path] = []
    chunks = [files[i : i + chunk_size] for i in range(0, len(files), chunk_size)]

    print(f"Merging {len(files)} file(s) in {len(chunks)} chunk(s) …")

    for idx, chunk in enumerate(tqdm(chunks, desc="Merging")):
        out_path = output_dir / f"merged_{idx:04d}{extension}"

        if len(chunk) == 1:
            # Nothing to merge — just copy / link
            _copy_or_link(chunk[0], out_path)
            results.append(out_path)
            continue

        cmd = ["pdal", "merge"] + [str(f) for f in chunk] + [str(out_path)]
        try:
            subprocess.run(cmd, check=True, capture_output=True, text=True)
            results.append(out_path)
        except subprocess.CalledProcessError as exc:
            tqdm.write(f"Merge chunk {idx} failed: {exc.stderr.strip()}")
        except FileNotFoundError:
            tqdm.write("pdal not found — install PDAL (https://pdal.io)")
            break

    print(f"Done — {len(results)}/{len(chunks)} chunk(s) created.")
    return results


def _copy_or_link(src: Path, dst: Path):
    """Hard-link if possible, else copy."""
    try:
        dst.hardlink_to(src)
    except OSError:
        import shutil
        shutil.copy2(src, dst)
