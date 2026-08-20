# NFL Weather Project: Current-State Handoff (2026-03-09)

## 1. Current data assets now available

This project now has a stable serving layer for visualization, plus synced audit/metadata tables.

### GCS final serving outputs (published)
- `gs://msba405-nfl-weather-raw/gold/tableau_serving/v1/serv_team_season_weather_v1/`
- `gs://msba405-nfl-weather-raw/gold/tableau_serving/v1/serv_play_calling_weather_v1/`
- `gs://msba405-nfl-weather-raw/gold/tableau_serving/v1/serv_indoor_outdoor_compare_v1/`
- `gs://msba405-nfl-weather-raw/gold/tableau_serving/v1/run_audit_v1/`
- `gs://msba405-nfl-weather-raw/gold/tableau_serving/v1/table_audit_v1/`
- `gs://msba405-nfl-weather-raw/gold/tableau_serving/v1/dq_audit_v1/`
- `gs://msba405-nfl-weather-raw/gold/tableau_serving/v1/data_dictionary_v1/`

### Snowflake final Tableau-facing tables (official BI contract)
- `NFL_WEATHER.SERVING.SERV_TEAM_SEASON_WEATHER_V1`
- `NFL_WEATHER.SERVING.SERV_PLAY_CALLING_WEATHER_V1`
- `NFL_WEATHER.SERVING.SERV_INDOOR_OUTDOOR_COMPARE_V1`

### Snowflake audit/metadata tables
- `NFL_WEATHER.AUDIT.RUN_AUDIT_V1`
- `NFL_WEATHER.AUDIT.TABLE_AUDIT_V1`
- `NFL_WEATHER.AUDIT.DQ_AUDIT_V1`
- `NFL_WEATHER.AUDIT.DATA_DICTIONARY_V1`
- `NFL_WEATHER.AUDIT.BATCH_C2_RUN_AUDIT_V1`
- `NFL_WEATHER.AUDIT.BATCH_C2_TABLE_RECON_V1`

### Total final usable tables
- Tableau-facing analysis tables: `3`
- Audit/metadata tables in Snowflake: `6`
- Total final tables in Snowflake: `9`
- Published final outputs in GCS serving path: `7`

## 2. Table-by-table documentation

| Table / Path | Purpose | Grain | Primary business key | Latest validated row count | Tableau-facing? | How to use |
|---|---|---|---|---:|---|---|
| `gs://msba405-nfl-weather-raw/gold/tableau_serving/v1/serv_team_season_weather_v1/` and `NFL_WEATHER.SERVING.SERV_TEAM_SEASON_WEATHER_V1` | Team-season performance by weather bins | `season, team, temp_bin, wind_bin, precip_bin, roof_env_bin, weather_sample` | Same as grain | 21858 | Yes | Use for team-season weather performance dashboards (pass rate, completion %, EPA/play, points/game). |
| `gs://msba405-nfl-weather-raw/gold/tableau_serving/v1/serv_play_calling_weather_v1/` and `NFL_WEATHER.SERVING.SERV_PLAY_CALLING_WEATHER_V1` | Play-calling behavior by weather bins | `season, team, temp_bin, wind_bin, precip_bin, weather_sample` | Same as grain | 16792 | Yes | Use for pass/rush tendency shifts vs baseline. |
| `gs://msba405-nfl-weather-raw/gold/tableau_serving/v1/serv_indoor_outdoor_compare_v1/` and `NFL_WEATHER.SERVING.SERV_INDOOR_OUTDOOR_COMPARE_V1` | Indoor vs outdoor comparison | `season, roof_env_bin, weather_sample` | Same as grain | 145 | Yes | Use for environment-level comparison views. |
| `gs://msba405-nfl-weather-raw/gold/tableau_serving/v1/run_audit_v1/` and `NFL_WEATHER.AUDIT.RUN_AUDIT_V1` | Published run-level audit from serving build | `serving_run_id` | `serving_run_id` | 1 | Audit-only | Validate which serving run produced current outputs. |
| `gs://msba405-nfl-weather-raw/gold/tableau_serving/v1/table_audit_v1/` and `NFL_WEATHER.AUDIT.TABLE_AUDIT_V1` | Per-table output summary for published serving tables | `serving_run_id, table_name` | `serving_run_id, table_name` | 3 | Audit-only | Check row counts, season ranges, duplicate-grain counts. |
| `gs://msba405-nfl-weather-raw/gold/tableau_serving/v1/dq_audit_v1/` and `NFL_WEATHER.AUDIT.DQ_AUDIT_V1` | Data-quality checks from serving publish | `serving_run_id, table_name, check_name` | `serving_run_id, table_name, check_name` | 27 | Audit-only | Inspect pass/fail status and expected vs actual values. |
| `gs://msba405-nfl-weather-raw/gold/tableau_serving/v1/data_dictionary_v1/` and `NFL_WEATHER.AUDIT.DATA_DICTIONARY_V1` | Published data dictionary | `serving_run_id, table_name, column_name` | `serving_run_id, table_name, column_name` | 72 | Audit-only | Reference definitions and data types. |
| `NFL_WEATHER.AUDIT.BATCH_C2_RUN_AUDIT_V1` | Snowflake load/reconciliation run summary | `run_id` | `run_id` | 1 | Audit-only | Confirm Snowflake-side load reconciliation status. |
| `NFL_WEATHER.AUDIT.BATCH_C2_TABLE_RECON_V1` | Snowflake load/reconciliation per table | `run_id, table_name` | `run_id, table_name` | 3 | Audit-only | Confirm expected rows, duplicate keys, partition null checks in Snowflake targets. |

## 3. Columns

### 3.1 `NFL_WEATHER.SERVING.SERV_TEAM_SEASON_WEATHER_V1`

| Column | Type | Business meaning | Class | Caveat |
|---|---|---|---|---|
| `SEASON` | NUMBER(10,0) | NFL season year | Dimension | Range is 2000-2025 in current release |
| `TEAM` | TEXT | Offense team code | Dimension | Team abbreviations are standardized source values |
| `TEMP_BIN` | TEXT | Temperature bucket | Dimension | Includes `UNKNOWN` when weather detail unavailable |
| `WIND_BIN` | TEXT | Wind speed bucket | Dimension | Includes `UNKNOWN` |
| `PRECIP_BIN` | TEXT | Precipitation bucket | Dimension | Currently all rows are `UNKNOWN` |
| `ROOF_ENV_BIN` | TEXT | Stadium roof environment category | Dimension | Values include `INDOOR`, `OUTDOOR`, `RETRACTABLE` |
| `WEATHER_SAMPLE` | TEXT | Weather availability scope | Dimension | `ALL_PLAYS` vs `OBSERVED_ONLY` must not be mixed blindly |
| `N_PLAYS` | NUMBER(38,0) | Offensive plays count | Metric | Use for weighting |
| `N_PASS_ATTEMPTS` | NUMBER(38,0) | Pass attempts count | Metric | Numerator for pass rate |
| `N_RUSH_ATTEMPTS` | NUMBER(38,0) | Rush attempts count | Metric | Numerator for rush rate |
| `N_COMPLETIONS` | NUMBER(38,0) | Completed passes count | Metric | Numerator for completion % |
| `N_EPA_PLAYS` | NUMBER(38,0) | Plays with valid EPA | Metric | Denominator for EPA/play |
| `N_GAMES` | NUMBER(38,0) | Distinct games count | Metric | Not always same as team-games for points |
| `N_GAMES_POINTS` | NUMBER(38,0) | Games contributing to points calc | QC/Metric support | Use with `N_TEAM_GAMES_POINTS` context |
| `N_TEAM_GAMES_POINTS` | NUMBER(38,0) | Team-games contributing to points calc | QC/Metric support | Used by precomputed `POINTS_PER_GAME` |
| `PASS_RATE` | FLOAT | Pass attempts / offensive attempts | Metric | Already precomputed |
| `COMPLETION_PCT` | FLOAT | Completions / pass attempts | Metric | Already precomputed |
| `EPA_PER_PLAY` | FLOAT | EPA sum / EPA-valid plays | Metric | Already precomputed |
| `POINTS_PER_GAME` | FLOAT | Average final points per team-game | Metric | Already precomputed |
| `BATCH_A_BUILD_RUN_TS` | TEXT | Upstream build timestamp | Metadata | Traceability field |
| `BATCH_B_BUILD_RUN_TS` | TEXT | Upstream mart build timestamp | Metadata | Traceability field |
| `SOURCE_BATCH_A_BUILD_RUN_TS` | TEXT | Source batch-A timestamp carried forward | Metadata | Traceability field |
| `SOURCE_BATCH_B_BUILD_RUN_TS` | TEXT | Source batch-B timestamp carried forward | Metadata | Traceability field |
| `SERVING_BUILD_RUN_TS` | TEXT | Serving publish timestamp | Metadata | Useful for freshness checks |
| `SERVING_VERSION` | TEXT | Serving contract version | Metadata | Current version is `v1` |
| `SERVING_RUN_ID` | TEXT | Serving run identifier | Metadata/QC | Join key into audit tables |

### 3.2 `NFL_WEATHER.SERVING.SERV_PLAY_CALLING_WEATHER_V1`

| Column | Type | Business meaning | Class | Caveat |
|---|---|---|---|---|
| `SEASON` | NUMBER(10,0) | NFL season year | Dimension | Range is 2000-2025 |
| `TEAM` | TEXT | Offense team code | Dimension | Standard team abbreviations |
| `TEMP_BIN` | TEXT | Temperature bucket | Dimension | Includes `UNKNOWN` |
| `WIND_BIN` | TEXT | Wind bucket | Dimension | Includes `UNKNOWN` |
| `PRECIP_BIN` | TEXT | Precipitation bucket | Dimension | Currently all rows are `UNKNOWN` |
| `WEATHER_SAMPLE` | TEXT | Weather availability scope | Dimension | Compare `ALL_PLAYS` vs `OBSERVED_ONLY` separately |
| `N_PLAYS` | NUMBER(38,0) | Offensive plays count | Metric | Use as weight |
| `N_PASS_ATTEMPTS` | NUMBER(38,0) | Pass attempts | Metric | Numerator support |
| `N_RUSH_ATTEMPTS` | NUMBER(38,0) | Rush attempts | Metric | Numerator support |
| `N_GAMES` | NUMBER(38,0) | Distinct games count | Metric | Context count |
| `PASS_RATE` | FLOAT | Pass attempt rate | Metric | Already precomputed |
| `RUSH_RATE` | FLOAT | Rush attempt rate | Metric | Already precomputed |
| `PASS_RATE_BASELINE` | FLOAT | Team-season baseline pass rate | Metric | Baseline reference for shift fields |
| `RUSH_RATE_BASELINE` | FLOAT | Team-season baseline rush rate | Metric | Baseline reference for shift fields |
| `PASS_RATE_SHIFT_VS_TEAM_SEASON` | FLOAT | Difference vs baseline pass rate | Metric | Positive means pass-heavier than baseline |
| `RUSH_RATE_SHIFT_VS_TEAM_SEASON` | FLOAT | Difference vs baseline rush rate | Metric | Positive means rush-heavier than baseline |
| `BATCH_A_BUILD_RUN_TS` | TEXT | Upstream build timestamp | Metadata | Traceability field |
| `BATCH_B_BUILD_RUN_TS` | TEXT | Upstream mart build timestamp | Metadata | Traceability field |
| `SOURCE_BATCH_A_BUILD_RUN_TS` | TEXT | Source batch-A timestamp carried forward | Metadata | Traceability field |
| `SOURCE_BATCH_B_BUILD_RUN_TS` | TEXT | Source batch-B timestamp carried forward | Metadata | Traceability field |
| `SERVING_BUILD_RUN_TS` | TEXT | Serving publish timestamp | Metadata | Freshness check |
| `SERVING_VERSION` | TEXT | Serving contract version | Metadata | Current version is `v1` |
| `SERVING_RUN_ID` | TEXT | Serving run identifier | Metadata/QC | Join key into audit tables |

### 3.3 `NFL_WEATHER.SERVING.SERV_INDOOR_OUTDOOR_COMPARE_V1`

| Column | Type | Business meaning | Class | Caveat |
|---|---|---|---|---|
| `SEASON` | NUMBER(10,0) | NFL season year | Dimension | Range is 2000-2025 |
| `ROOF_ENV_BIN` | TEXT | Roof environment category | Dimension | `RETRACTABLE` is stadium type bucket |
| `WEATHER_SAMPLE` | TEXT | Weather availability scope | Dimension | Keep sample types separate in analysis |
| `N_PLAYS` | NUMBER(38,0) | Offensive plays count | Metric | Use for weighting |
| `N_PASS_ATTEMPTS` | NUMBER(38,0) | Pass attempts count | Metric | Numerator support |
| `N_RUSH_ATTEMPTS` | NUMBER(38,0) | Rush attempts count | Metric | Numerator support |
| `N_COMPLETIONS` | NUMBER(38,0) | Completions count | Metric | Numerator support |
| `N_EPA_PLAYS` | NUMBER(38,0) | EPA-valid plays | Metric | Denominator support |
| `N_GAMES` | NUMBER(38,0) | Distinct games count | Metric | Context count |
| `N_GAMES_POINTS` | NUMBER(38,0) | Games in points calculation | QC/Metric support | Support field |
| `N_TEAM_GAMES_POINTS` | NUMBER(38,0) | Team-games in points calculation | QC/Metric support | Support field |
| `N_TEAMS` | NUMBER(38,0) | Distinct teams count | Metric | Coverage context |
| `PASS_RATE` | FLOAT | Pass attempt rate | Metric | Already precomputed |
| `COMPLETION_PCT` | FLOAT | Completion rate | Metric | Already precomputed |
| `EPA_PER_PLAY` | FLOAT | EPA per play | Metric | Already precomputed |
| `POINTS_PER_GAME` | FLOAT | Average final points per team-game | Metric | Already precomputed |
| `BATCH_A_BUILD_RUN_TS` | TEXT | Upstream build timestamp | Metadata | Traceability field |
| `BATCH_B_BUILD_RUN_TS` | TEXT | Upstream mart build timestamp | Metadata | Traceability field |
| `SOURCE_BATCH_A_BUILD_RUN_TS` | TEXT | Source batch-A timestamp carried forward | Metadata | Traceability field |
| `SOURCE_BATCH_B_BUILD_RUN_TS` | TEXT | Source batch-B timestamp carried forward | Metadata | Traceability field |
| `SERVING_BUILD_RUN_TS` | TEXT | Serving publish timestamp | Metadata | Freshness check |
| `SERVING_VERSION` | TEXT | Serving contract version | Metadata | Current version is `v1` |
| `SERVING_RUN_ID` | TEXT | Serving run identifier | Metadata/QC | Join key into audit tables |

### 3.4 Audit tables (lighter reference)

| Table | Key columns | What it contains |
|---|---|---|
| `NFL_WEATHER.AUDIT.RUN_AUDIT_V1` | `SERVING_RUN_ID` | High-level published run status, source row counts, serving row counts |
| `NFL_WEATHER.AUDIT.TABLE_AUDIT_V1` | `SERVING_RUN_ID, TABLE_NAME` | Per-serving-table counts, min/max season, duplicate-grain count |
| `NFL_WEATHER.AUDIT.DQ_AUDIT_V1` | `SERVING_RUN_ID, TABLE_NAME, CHECK_NAME` | DQ checks with expected/actual/status |
| `NFL_WEATHER.AUDIT.DATA_DICTIONARY_V1` | `SERVING_RUN_ID, TABLE_NAME, COLUMN_NAME` | Column dictionary and grain-key indicator |
| `NFL_WEATHER.AUDIT.BATCH_C2_RUN_AUDIT_V1` | `RUN_ID` | Snowflake-side load/reconciliation run outcome |
| `NFL_WEATHER.AUDIT.BATCH_C2_TABLE_RECON_V1` | `RUN_ID, TABLE_NAME` | Snowflake-side per-table reconciliation metrics |

## 4. How tables relate / how to join

### Safe relationship guidance
- The 3 Tableau-facing `SERVING` tables are separate pre-aggregated marts at different grains.
- Recommended approach: use each table independently for its own dashboard purpose.
- Do not join the 3 serving tables together in Tableau for primary analytics.

### Why not join the 3 serving tables directly
- Grains differ.
- Joining can create many-to-many duplication and incorrect rates.
- Example: `SERV_TEAM_SEASON_WEATHER_V1` includes `ROOF_ENV_BIN`, but `SERV_PLAY_CALLING_WEATHER_V1` does not.

### What can be joined safely
- Audit joins for QA only.
- `RUN_AUDIT_V1`, `TABLE_AUDIT_V1`, `DQ_AUDIT_V1`, `DATA_DICTIONARY_V1` can be joined using `SERVING_RUN_ID` (and `TABLE_NAME` where relevant).
- `BATCH_C2_RUN_AUDIT_V1` and `BATCH_C2_TABLE_RECON_V1` join on `RUN_ID`.

## 5. Tableau usage guide for the visualization team

### Connection target
- Connect Tableau to Snowflake database: `NFL_WEATHER`.
- Use schema: `SERVING` for dashboards.

### Official Tableau contract tables
- `SERVING.SERV_TEAM_SEASON_WEATHER_V1`
- `SERVING.SERV_PLAY_CALLING_WEATHER_V1`
- `SERVING.SERV_INDOOR_OUTDOOR_COMPARE_V1`

### Tables to ignore for normal dashboard building
- Ignore `STG` schema tables.
- Ignore temporary/load tables (`*_LND`, `*_TMP`, `*__LOAD_TMP`).
- Use `AUDIT` schema only for validation/reference, not primary charting.

### Recommended starting worksheets/dashboards
- Team-season weather performance: use `SERV_TEAM_SEASON_WEATHER_V1`.
- Play-calling weather shifts: use `SERV_PLAY_CALLING_WEATHER_V1`.
- Indoor vs outdoor comparison: use `SERV_INDOOR_OUTDOOR_COMPARE_V1`.

### How to use core dimensions
- `TEMP_BIN`, `WIND_BIN`, `PRECIP_BIN`: categorical weather buckets for grouping/filtering.
- `ROOF_ENV_BIN`: environment grouping (`INDOOR`, `OUTDOOR`, `RETRACTABLE`).
- `WEATHER_SAMPLE`: `ALL_PLAYS` means all mapped plays are included; `OBSERVED_ONLY` means only rows with observed weather availability are included.

### Practical do/don’t
- Do filter or facet by `WEATHER_SAMPLE` rather than mixing both into one measure without context.
- Do use count columns (`N_PLAYS`, `N_PASS_ATTEMPTS`, etc.) when building weighted custom calcs.
- Do not average already-aggregated rates across rows without weighting.
- Do not connect to staging/intermediate tables for production dashboards.

## 6. Data quality / readiness status

Current readiness is `PASS`.

Validated and passed:
- All 3 Tableau-facing serving tables loaded and reconciled in Snowflake.
- Expected row counts match actual row counts: `SERV_TEAM_SEASON_WEATHER_V1=21858`, `SERV_PLAY_CALLING_WEATHER_V1=16792`, `SERV_INDOOR_OUTDOOR_COMPARE_V1=145`.
- Duplicate rows at business key grain: `0` for all 3 serving tables.
- Required metadata/audit tables loaded in Snowflake: `RUN_AUDIT_V1 (1)`, `TABLE_AUDIT_V1 (3)`, `DQ_AUDIT_V1 (27)`, `DATA_DICTIONARY_V1 (72)`.
- Latest Snowflake reconciliation run: `c2_recon_20260309T070434Z`, status `PASS`, error_count `0`.
- Final readiness check timestamp: `2026-03-09 07:32:27Z`, all checklist items `PASS`.

Known blockers: none.

Tableau can proceed safely.

## 7. Known limitations / caveats

- `PRECIP_BIN` is currently `UNKNOWN` for all rows in both weather-bin serving marts.
- `UNKNOWN` bucket values can appear for weather bins and should be handled explicitly in visuals.
- Tables are aggregated outputs, not play-level facts.
- Precomputed rate fields should be interpreted as finalized metrics at table grain.
- If you roll up further in Tableau, prefer weighted calculations using count columns.
- Season coverage in current release is `2000` to `2025`.

## 8. Recommended team workflow

- Use only `NFL_WEATHER.SERVING` tables for production Tableau dashboards.
- Use `NFL_WEATHER.AUDIT` tables only for validation, freshness checks, and troubleshooting.
- Do not use `STG` tables or any intermediate/load tables in BI workbooks.
- Ask the data team when you need a new metric or different grain, need a combined table across dashboard topics, detect count mismatches or unexpected `UNKNOWN` behavior, or need changes to weather bin definitions.
