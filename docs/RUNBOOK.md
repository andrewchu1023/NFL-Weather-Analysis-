# Runbook

## Official Runner

Use `run_pipeline_v1.sh` from repo root.

## Modes

### Default (official v1)

Starts from prepared stable silver prefixes and runs gold + serving build/validation.

```bash
./run_pipeline_v1.sh --mode default
```

### Extended Rebuild

Rebuilds selected silver foundations, then executes the default flow.

```bash
./run_pipeline_v1.sh --mode extended-rebuild
```

## Dry Run

Always recommended before first execution:

```bash
./run_pipeline_v1.sh --mode default --dry-run
```

## Optional Snowflake Publish

Run after GCS serving outputs are validated:

```bash
./run_pipeline_v1.sh --mode default --with-snowflake
```

Include phase1 object setup only when needed:

```bash
./run_pipeline_v1.sh --mode default --with-snowflake --with-snowflake-phase1
```

Snowflake order is fixed in runner:
1. phase1 setup (only if requested)
2. team load
3. play load
4. indoor load
5. C1 metadata load
6. reconciliation audit
7. final readiness

## Default Mode Preflight Checks

Runner verifies these stable silver paths exist in GCS:
- `silver/pbp_play_level_core/`
- `silver/pbp_game_level/`
- `silver/dim_venue/`
- `silver/dim_station_hourly/`
- `silver/venue_station_map_hourly/`
- `silver/noaa_hourly_observations/`
- `silver/noaa_hourly_features_extended/v1/`

## Out of Scope for Default v1

Not executed by default runner:
- raw data landing
- NOAA raw refresh ingestion
- historical cleanup operations

Those are separate operational workflows.

## Raw Data Preparation (Separate Workflow)

Raw data acquisition/preparation is not part of `./run_pipeline_v1.sh --mode default`.

- Use the default runner for the official v1 path from prepared stable silver onward.
- Use a separate operational workflow for initial raw-data landing and NOAA refresh.
- Full project-specific setup commands are documented in [docs/RAW_DATA_SETUP.md](RAW_DATA_SETUP.md).
