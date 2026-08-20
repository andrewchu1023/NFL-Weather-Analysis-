# Batch C2 Status

## Phase 1 (Snowflake object setup)
Status: COMPLETE (live)

Confirmed:
- `NFL_WEATHER` database in use
- stage `STG.GCS_TABLEAU_SERVING_V1` exists and lists files
- storage integration `GCS_NFL_WEATHER_INT` is active

## Phase 2 (minimal one-table load)
Status: COMPLETE for all 3 serving tables + reconciliation audit

Run result:
- source path: `@STG.GCS_TABLEAU_SERVING_V1/serv_team_season_weather_v1/`
- parquet files listed: `50`
- rows copied to landing: `21858`
- final target rows: `21858`
- duplicate rows at grain key: `0`
- season coverage: `2000-2025`
- weather_sample values: `ALL_PLAYS`, `OBSERVED_ONLY`

Additional Phase 2 table loads:
- `SERVING.SERV_PLAY_CALLING_WEATHER_V1`: `16792` rows, duplicate key rows `0`, null partition keys `0`
- `SERVING.SERV_INDOOR_OUTDOOR_COMPARE_V1`: `145` rows, duplicate key rows `0`, null partition keys `0`

Batch C2 reconciliation/audit:
- run id: `c2_recon_20260309T070434Z`
- run status: `PASS`
- error count: `0`
- table-level recon:
  - `SERVING.SERV_TEAM_SEASON_WEATHER_V1` pass
  - `SERVING.SERV_PLAY_CALLING_WEATHER_V1` pass
  - `SERVING.SERV_INDOOR_OUTDOOR_COMPARE_V1` pass
- Snowflake audit tables updated:
  - `AUDIT.BATCH_C2_RUN_AUDIT_V1`
  - `AUDIT.BATCH_C2_TABLE_RECON_V1`

C1 metadata/audit sync to Snowflake:
- status: `PASS`
- tables loaded:
  - `AUDIT.RUN_AUDIT_V1` rows `1`
  - `AUDIT.TABLE_AUDIT_V1` rows `3`
  - `AUDIT.DQ_AUDIT_V1` rows `27`
  - `AUDIT.DATA_DICTIONARY_V1` rows `72`
- key uniqueness checks: all pass (`0` duplicate rows)

Final release readiness:
- validation timestamp: `2026-03-09 07:32:27Z`
- checklist: all `PASS`
- known blockers: `None`
- summary file: `/home/jayyu1/snowflake/batch_c2/FINAL_READINESS_SUMMARY.md`

Notes:
- Spark partition columns (`season`, `weather_sample`) are path-derived in GCS.
- Loader extracts them from `METADATA$FILENAME` during `COPY INTO`.
