# Snowflake Setup and Publish Guide

This project loads Tableau-serving outputs from GCS into Snowflake.

## Required Snowflake Context

- Role: `ACCOUNTADMIN` (or role with equivalent privileges)
- Warehouse: `COMPUTE_WH` (or equivalent)
- Database: `NFL_WEATHER`
- Storage integration: `GCS_NFL_WEATHER_INT`
- External stage path: `gcs://msba405-nfl-weather-raw/gold/tableau_serving/v1/`

## Environment Variables

Set values from `env.example`:
- `SNOWFLAKE_ACCOUNT` (fallback optional)
- `SNOWFLAKE_USER`
- `SNOWFLAKE_PASSWORD`
- `SNOWFLAKE_ROLE`
- `SNOWFLAKE_WAREHOUSE`
- `SNOWFLAKE_DATABASE`
- `SNOWFLAKE_STORAGE_INTEGRATION` (required for phase1)

## Phase1 (only if setup not already present)

Creates/validates:
- schemas: `STG`, `SERVING`, `AUDIT`
- file format: `STG.FF_PARQUET_V1`
- stage: `STG.GCS_TABLEAU_SERVING_V1`
- empty target seed table

Via runner:

```bash
./run_pipeline_v1.sh --mode default --with-snowflake --with-snowflake-phase1
```

## Phase2 Load Pattern

Loads in this order:
1. `SERV_TEAM_SEASON_WEATHER_V1`
2. `SERV_PLAY_CALLING_WEATHER_V1`
3. `SERV_INDOOR_OUTDOOR_COMPARE_V1`
4. C1 metadata/audit tables
5. reconciliation audit
6. final readiness

Each load performs copy/merge validations including row counts and key uniqueness.

## Expected Row Counts (current contract)

- team table: `21858`
- play table: `16792`
- indoor table: `145`

If row counts change due to an intentional contract update, update both the source contract docs and Snowflake validation arguments.
