# Batch C2 Phase 1 (Snowflake object setup)

This phase creates the baseline Snowflake objects for loading Batch C1 serving outputs from:

`gcs://msba405-nfl-weather-raw/gold/tableau_serving/v1/`

## Objects created

- Schemas: `STG`, `SERVING`, `AUDIT`
- File format: `STG.FF_PARQUET_V1`
- Stage: `STG.GCS_TABLEAU_SERVING_V1`
- Empty target table: `SERVING.SERV_TEAM_SEASON_WEATHER_V1`

## Prerequisites

- Snowflake account + role + warehouse with DDL permissions
- Existing Snowflake storage integration for GCS (example: `GCS_MSBA405_INT`)
- Python package: `snowflake-connector-python`

Install connector:

```bash
pip install snowflake-connector-python
```

## Dry run (local render check)

```bash
python3 snowflake/batch_c2/phase1/run_phase1_setup.py \
  --sf-storage-integration GCS_MSBA405_INT \
  --dry-run
```

## Execute against Snowflake

```bash
export SNOWFLAKE_ACCOUNT='<account_locator_or_account_url>'
export SNOWFLAKE_USER='<user>'
export SNOWFLAKE_PASSWORD='<password>'
export SNOWFLAKE_ROLE='SYSADMIN'
export SNOWFLAKE_WAREHOUSE='COMPUTE_WH'
export SNOWFLAKE_DATABASE='NFL_WEATHER'
export SNOWFLAKE_STORAGE_INTEGRATION='GCS_MSBA405_INT'

python3 snowflake/batch_c2/phase1/run_phase1_setup.py
```

## Success criteria

The script prints `PHASE1_VALIDATION_PASSED` only when all checks pass:

- all 3 schemas exist
- file format exists
- stage exists and matches expected GCS URL + storage integration
- target table exists and is empty
- target table column contract matches expected v1 schema
