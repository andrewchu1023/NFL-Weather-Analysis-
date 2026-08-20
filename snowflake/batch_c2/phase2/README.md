# Batch C2 Phase 2 (incremental serving loads)

This phase loads serving tables end-to-end in Snowflake using the same validated pattern.

Loaders:
- `run_phase2_team_load.py`
- `run_phase2_play_load.py`
- `run_phase2_indoor_load.py`
- `run_phase2_c1_metadata_loads.py`

## What the loader does

1. Connects using primary account id, with fallback account id if needed.
2. Verifies stage visibility and source parquet files.
3. Ensures landing and target tables exist (`CREATE TABLE IF NOT EXISTS`).
4. `TRUNCATE` landing table and `COPY INTO` from stage path.
5. Validates landing row count, schema alignment, and key uniqueness.
6. Loads target safely through `SWAP` with a temporary fully-loaded table.
7. Validates final target row count and key uniqueness.

## Run in order

```bash
python3 snowflake/batch_c2/phase2/run_phase2_team_load.py \
  --account ekbpfux-vob93769 \
  --account-fallback hbb86970 \
  --expected-rows 21858

python3 snowflake/batch_c2/phase2/run_phase2_play_load.py \
  --account ekbpfux-vob93769 \
  --account-fallback hbb86970 \
  --expected-rows 16792

python3 snowflake/batch_c2/phase2/run_phase2_indoor_load.py \
  --account ekbpfux-vob93769 \
  --account-fallback hbb86970 \
  --expected-rows 145
```

All three loaders derive `season` and `weather_sample` from `METADATA$FILENAME` to preserve partition-path fields.

## Reconciliation and audit

After all three table loads pass:

```bash
python3 snowflake/batch_c2/phase2/run_phase2_reconciliation_audit.py \
  --account ekbpfux-vob93769 \
  --account-fallback hbb86970
```

Audit tables written:
- `AUDIT.BATCH_C2_RUN_AUDIT_V1`
- `AUDIT.BATCH_C2_TABLE_RECON_V1`

## C1 metadata/audit sync

Load remaining C1 tables into Snowflake `AUDIT` schema:

```bash
python3 snowflake/batch_c2/phase2/run_phase2_c1_metadata_loads.py \
  --account ekbpfux-vob93769 \
  --account-fallback hbb86970
```

Targets:
- `AUDIT.RUN_AUDIT_V1`
- `AUDIT.TABLE_AUDIT_V1`
- `AUDIT.DQ_AUDIT_V1`
- `AUDIT.DATA_DICTIONARY_V1`

## Final readiness checklist

```bash
python3 snowflake/batch_c2/phase2/run_batch_c2_final_readiness.py \
  --account ekbpfux-vob93769 \
  --account-fallback hbb86970
```

Summary output file:
- `/home/jayyu1/snowflake/batch_c2/FINAL_READINESS_SUMMARY.md`

The script uses these env vars by default:

- `SNOWFLAKE_USER`
- `SNOWFLAKE_PASSWORD`
- `SNOWFLAKE_ROLE`
- `SNOWFLAKE_WAREHOUSE`
- `SNOWFLAKE_DATABASE`
