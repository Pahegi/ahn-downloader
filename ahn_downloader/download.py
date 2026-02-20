"""Download AHN4 LAZ tiles with parallel workers and resume support."""

from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import requests
from tqdm import tqdm

from .tiles import Tile

DEFAULT_THREADS = 5
BLOCK_SIZE = 8192  # 8 KiB


def download_tiles(
    tiles: list[Tile],
    output_dir: Path,
    threads: int = DEFAULT_THREADS,
) -> list[Path]:
    """Download *tiles* into *output_dir* using *threads* workers.

    Returns a list of local file paths that were successfully written.
    Existing files whose size matches the remote content-length are skipped.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    results: list[Path] = []

    with ThreadPoolExecutor(max_workers=threads) as pool:
        futures = {
            pool.submit(_download_one, idx, tile, output_dir): tile
            for idx, tile in enumerate(tiles)
        }
        for future in as_completed(futures):
            path = future.result()
            if path is not None:
                results.append(path)

    return results


def _download_one(
    index: int,
    tile: Tile,
    output_dir: Path,
) -> Path | None:
    url = tile.download_url
    dest = output_dir / tile.filename

    # --- check if already downloaded ---
    try:
        head = requests.head(url, timeout=30, allow_redirects=True)
        head.raise_for_status()
        remote_size = int(head.headers.get("content-length", 0))
        if dest.exists() and dest.stat().st_size == remote_size:
            tqdm.write(f"[{index}] {dest.name} already complete — skipped")
            return dest
    except requests.RequestException as exc:
        tqdm.write(f"[{index}] HEAD failed for {url}: {exc}")
        return None

    # --- stream download with progress ---
    try:
        resp = requests.get(url, stream=True, timeout=60)
        resp.raise_for_status()
        total = int(resp.headers.get("content-length", 0))

        with open(dest, "wb") as fh, tqdm(
            total=total,
            unit="iB",
            unit_scale=True,
            desc=f"[{index}] {dest.name}",
            leave=False,
        ) as bar:
            for chunk in resp.iter_content(chunk_size=BLOCK_SIZE):
                fh.write(chunk)
                bar.update(len(chunk))

        return dest
    except requests.RequestException as exc:
        tqdm.write(f"[{index}] download failed for {url}: {exc}")
        return None
