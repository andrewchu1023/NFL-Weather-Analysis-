# Raw Data Setup

This guide documents how raw data was obtained/prepared for this project and how that relates to the official v1 pipeline.

- Official v1 pipeline (`run_pipeline_v1.sh --mode default`) starts from prepared stable silver inputs.
- Raw landing and NOAA refresh are separate operational workflows.
- Raw data files are not committed to this GitHub repository.

## NFL raw data acquisition

The project used nflverse GitHub Releases (tag `pbp`) and retained both:
- parquet in `bronze/pbp/` for Spark processing
- csv in `bronze/pbp_csv/` for exploration/completeness

```bash
mkdir -p pbp_parquet pbp_csv_stage/files

TAG="pbp"

# Download yearly parquet files (2000-2025)
for y in $(seq 2000 2025); do
  curl -fL --retry 3 --retry-delay 2 \
    -o "pbp_parquet/play_by_play_${y}.parquet" \
    "https://github.com/nflverse/nflverse-data/releases/download/${TAG}/play_by_play_${y}.parquet"
done

# Upload parquet to bronze processing path
gsutil -m cp pbp_parquet/play_by_play_*.parquet \
  gs://msba405-nfl-weather-raw/bronze/pbp/

# Download yearly CSV files (2000-2025)
for y in $(seq 2000 2025); do
  curl -fL --retry 3 --retry-delay 2 \
    -o "pbp_csv_stage/files/play_by_play_${y}.csv" \
    "https://github.com/nflverse/nflverse-data/releases/download/pbp/play_by_play_${y}.csv"
done

# Upload CSV to separate bronze exploration path
gsutil -m cp pbp_csv_stage/files/play_by_play_*.csv \
  gs://msba405-nfl-weather-raw/bronze/pbp_csv/
```

Notes:
- The parquet path is the primary Spark-processing raw input.
- The CSV path is retained separately for exploration/completeness.
- These raw data files are not committed to GitHub.

## NOAA reference setup

NOAA reference setup for this project includes:
- `isd-history.csv`
- `noaa_token.txt`

```bash
mkdir -p noaa_ref

# Download NOAA ISD station-history reference
curl -fL \
  -o noaa_ref/isd-history.csv \
  "https://www.ncei.noaa.gov/pub/data/noaa/isd-history.csv"

# Save NOAA token locally (token obtained separately from NOAA)
printf '%s\n' "$NOAA_TOKEN" > noaa_ref/noaa_token.txt

# Upload reference inputs to bronze reference path
gsutil -m cp noaa_ref/isd-history.csv noaa_ref/noaa_token.txt \
  gs://msba405-nfl-weather-raw/bronze/reference/noaa/
```

Notes:
- `noaa_token.txt` is private and must never be committed to GitHub.
- `env.example` must never contain a real NOAA token.
- NOAA reference setup is separate from the default v1 runner.

## NOAA raw weather ingestion for this project

The project’s final production NOAA raw weather path was not a simple one-off CDO JSON download.

Instead, the project used a manifest-driven ISD / ISD-Lite station-year ingestion workflow into bronze:

```bash
# Build station-year fallback resolution
python3 build_station_year_resolution_v1.py

# Build production NOAA station-year manifest
python3 build_noaa_station_year_manifest_v1.py

# Ingest raw NOAA station-year archives into bronze
python3 ingest_noaa_bronze_isd_v1.py
```

Raw NOAA bronze outputs land conceptually under:
- `gs://msba405-nfl-weather-raw/bronze/noaa/isd/source=.../year=YYYY/station_key=USAF-WBAN/*.gz`

Notes:
- This NOAA refresh chain is a separate operational workflow.
- It is not included in the default v1 entrypoint.
- Current GitHub v1 focuses on the official reproducible path from prepared stable silver inputs onward.
