# NFL Weather Pipeline (MSBA405)

This repository contains the publishable code, SQL, bash orchestration, and documentation for the NFL weather analytics pipeline.

## Official Entrypoint

Use:

```bash
./run_pipeline_v1.sh --mode default
```

`run_pipeline_v1.sh` is the official v1 orchestrator.

## Official v1 Scope

The official v1 pipeline is **gold/serving orchestration from prepared silver inputs**.

- `--mode default` starts from already-prepared stable silver datasets in GCS and runs:
  1. silver extended validation
  2. gold fact build + validation
  3. gold marts build + validation
  4. tableau serving build + validation
- Optional Snowflake publish is available with:
  - `--with-snowflake`
  - `--with-snowflake-phase1` (first-time Snowflake object setup only)
- Raw landing and NOAA raw refresh are **not** part of the default v1 entrypoint.

## Pipeline Modes

- `default`: starts from stable silver state; produces/validates gold + serving outputs in GCS.
- `extended-rebuild`: rebuilds selected silver foundations first, then runs the default flow.

## Raw Data Setup

Raw data acquisition and NOAA refresh are separate from the default v1 runner.

- Default v1 pipeline starts from prepared stable silver inputs.
- Raw landing and NOAA refresh are operational prep workflows outside `./run_pipeline_v1.sh --mode default`.
- See [docs/RAW_DATA_SETUP.md](docs/RAW_DATA_SETUP.md) for the project-specific raw data setup steps and commands.

## Quick Start

1. Create a Python environment and install dependencies.
2. Configure credentials and environment variables (see `env.example`).
3. Authenticate to GCP (`gcloud auth application-default login` or service account key).
4. Run dry-run first:

```bash
./run_pipeline_v1.sh --mode default --dry-run
```

5. Run full default pipeline:

```bash
./run_pipeline_v1.sh --mode default
```

6. Optional Snowflake publish:

```bash
./run_pipeline_v1.sh --mode default --with-snowflake
```

## Repo Contents

- Root Python scripts: official build/validation scripts used by `run_pipeline_v1.sh`
- `snowflake/batch_c2/`: Snowflake setup/load/reconciliation/readiness scripts
- `docs/`: project contracts, dataset context, runbook, and setup guides
- `legacy/`: placeholder for non-official or superseded scripts (not part of v1 pipeline)

## Notes

- This repo intentionally excludes raw data, processed outputs, runtime logs, caches, and credentials.
- GCS paths and contracts currently target `gs://msba405-nfl-weather-raw/`.
- For portability, Snowflake readiness script supports overriding summary output path via `--summary-path`.
