# AHN Downloader

Download [AHN](https://www.ahn.nl/) point-cloud tiles (Dutch national height model).
By default downloads **AHN5 colored** tiles (with RGB from aerial photographs)
around Amsterdam via [GeoTiles.nl](https://geotiles.citg.tudelft.nl/).

## Installation

```bash
poetry install
```

## Usage

```bash
poetry shell

# Download AHN5 colored tiles around Amsterdam (10 GB LAZ)
ahn-downloader -o ./data

# Convert to LAS after downloading
ahn-downloader -o ./data --las

# Convert to LAS with specific point format (e.g., format 7)
ahn-downloader -o ./data --las --point-format 7

# Specify separate directories for LAZ and LAS files
ahn-downloader --laz-output ./laz --las-output ./las --las

# Validate files after download
ahn-downloader -o ./data --validate

# Validate with verbose output (shows each file)
ahn-downloader -o ./data --validate --verbose

# Validate during conversion too
ahn-downloader -o ./data --las --validate

# Bigger area
ahn-downloader -o ./data --max-size 20

# Custom center point
ahn-downloader -o ./data --center 155000 463000 --max-size 10

# Bounding box selection
ahn-downloader -o ./data --bbox 119000 485000 123000 489000

# Preview what would be downloaded (no download)
ahn-downloader --dry-run
ahn-downloader --dry-run --max-size 20

# Download original AHN4 (no color)
ahn-downloader -o ./data --source ahn4
```

### Utility commands

```bash
# Convert existing LAZ → LAS
ahn-downloader convert -i ./data -o ./data/las

# Convert with specific point format
ahn-downloader convert -i ./data -o ./data/las --point-format 7

# Remove empty/invalid LAZ files during conversion (so you can redownload)
ahn-downloader convert -i ./data -o ./data/las --remove-empty

# Merge tiles into chunks (requires PDAL)
ahn-downloader merge -i ./data -o ./data/merged --chunk-size 4

# Validate file integrity
ahn-downloader validate -i ./data           # validate LAZ files
ahn-downloader validate -i ./data/las -v    # validate with verbose output
ahn-downloader validate -i ./data --remove-invalid  # validate and delete invalid files

# Interactive map selector
ahn-downloader gui -o ./data
```

### Point Record Formats

LAS files support different point record formats with varying attributes:
- **Format 6**: Basic XYZ + intensity + classification (LAS 1.4)
- **Format 7**: Format 6 + RGB colors (LAS 1.4)
- **Format 8**: Format 7 + NIR (near-infrared) (LAS 1.4)

AHN5 colored tiles use format 7 (with RGB). Use `--point-format` to convert to a different format compatible with your tools.

## Data Details

Coordinates are in **EPSG:28992** (Amersfoort / RD New).
Default center is Amsterdam (121000, 487000). Default budget is 10 GB LAZ.

> **Note:** Not all AHN5 tiles are available yet. GeoTiles.nl limits
> parallel connections; the downloader caps threads automatically.
> 
> **Empty tiles:** AHN5 colored tiles are split into 25 subtiles. Not all subtiles 
> contain data. The downloader automatically validates and removes empty or invalid 
> files (zero points or zero bounding box) after download.

## Tile index

The file `ahn_downloader/index.json` contains the AHN4 tile grid (kaartbladindex)
sourced from the [PDOK ATOM download service](https://service.pdok.nl/rws/ahn/atom/index.xml).
It maps tile names to their polygon footprints in EPSG:28992. The actual LAZ files
are served by [basisdata.nl](https://basisdata.nl/hwh-ahn/ahn4/01_LAZ/).

## License

MIT
