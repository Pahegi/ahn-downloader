# AHN4 Downloader

Download, convert & merge [AHN4](https://www.ahn.nl/) point-cloud tiles (Dutch national height model).

## Installation

```bash
poetry install
```

## Usage

```bash
# Activate the Poetry virtualenv
poetry shell

# Interactive GUI selector
ahn4 gui --output ./data

# CLI: select tiles by bounding box
ahn4 select --bbox 155000 463000 160000 468000

# CLI: contiguous area around a point, budget 20 GB
ahn4 select --center 155000 463000 --max-size 20

# Download tiles and convert to LAS
ahn4 download --center 121000 487000 --max-size 10 --output ./data --las

# Download tiles (LAZ only)
ahn4 download --bbox 155000 463000 160000 468000 --output ./data

# Full pipeline: select → download → convert
ahn4 pipeline --center 155000 463000 --max-size 20 --output ./data

# Convert LAZ → LAS
ahn4 convert --input ./data/laz --output ./data/las

# Merge files into chunks (requires PDAL)
ahn4 merge --input ./data --output ./data/merged --chunk-size 4
```

Coordinates are in **EPSG:28992** (Amersfoort / RD New).

## Tile index

The file `ahn4_downloader/index.json` contains the AHN4 tile grid (kaartbladindex)
sourced from the [PDOK ATOM download service](https://service.pdok.nl/rws/ahn/atom/index.xml).
It maps tile names to their polygon footprints in EPSG:28992. The actual LAZ files
are served by [basisdata.nl](https://basisdata.nl/hwh-ahn/ahn4/01_LAZ/).

## License

MIT
