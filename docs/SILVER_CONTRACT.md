# Silver Contract (v1)

Effective date: 2026-03-06

## Purpose
Define the only datasets that are allowed in `gs://msba405-nfl-weather-raw/silver/` for downstream gold-layer use.

## Allowed Silver Prefixes
- `silver/pbp_play_level_core/`
- `silver/pbp_game_level/`
- `silver/dim_venue/`
- `silver/dim_station_hourly/`
- `silver/venue_station_map_hourly/`
- `silver/noaa_hourly_observations/` (required deliverable; not yet built)

## Not Allowed In Silver
These must live under `qc/` or `scratch/` instead of `silver/`:
- precheck samples
- precheck missing-case tables
- QC summaries/diagnostics
- temporary experiments

## Governance Rules
- Run inventory before and after cleanup.
- Use move operations when reorganizing prefixes.
- Keep `silver/` free of duplicate or historical diagnostic outputs.
- Validate key uniqueness and null-key checks on production silver tables after cleanup.
