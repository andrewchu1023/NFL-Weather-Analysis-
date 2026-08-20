# NFL Weather Project Work History (Complete)

## Document purpose
This is a full replacement work-history document from initial project setup through current production-ready state.

It explicitly distinguishes what is:
- **Confirmed by artifact**
- **Reconstructed from project evidence**
- **Inferred but plausible**

## Evidence model used

### Evidence tags
- **Confirmed by artifact**: directly verifiable from files, scripts, object listings, timestamps, or status artifacts in this workspace / bucket.
- **Reconstructed from project evidence**: not explicitly stated in one artifact, but strongly supported by multiple artifacts together.
- **Inferred but plausible**: likely based on context patterns, but no direct artifact proving exact execution detail.

### Primary sources inspected
- Local docs:
  - `/home/jayyu1/docs/PROJECT_CONTEXT.md`
  - `/home/jayyu1/docs/DATASETS.md`
  - `/home/jayyu1/docs/NEXT_STEPS.md`
  - `/home/jayyu1/docs/SILVER_CONTRACT.md`
  - `/home/jayyu1/docs/NOAA_HOURLY_FEATURES_EXTENDED_V1_CONTRACT.md`
  - `/home/jayyu1/docs/BATCH_B_CONTRACT_FREEZE_20260308.md`
  - `/home/jayyu1/docs/CURRENT_STATE_HANDOFF_20260309.md`
- Build and validation scripts in `/home/jayyu1/`
- Snowflake load/readiness artifacts in `/home/jayyu1/snowflake/batch_c2/`
- GCS inventory and object listings for `gs://msba405-nfl-weather-raw/`
- Historical inventory snapshots in `/home/jayyu1/inventory/`
- Early local staging artifacts:
  - `/home/jayyu1/pbp_parquet/`
  - `/home/jayyu1/bronze_reference/`
  - `/home/jayyu1/noaa_ref/`

---

## Project objective

- **Confirmed by artifact**: Project goal is to enrich NFL play/game data with NOAA hourly weather and produce curated datasets for downstream analytics (`docs/PROJECT_CONTEXT.md`).
- **Confirmed by artifact**: Final intended consumption includes stable serving tables and dashboard use (`docs/CURRENT_STATE_HANDOFF_20260309.md`).
- **Reconstructed from project evidence**: End-to-end objective is: raw acquisition -> bronze landing -> silver curation -> gold marts -> serving publication -> Snowflake contract -> Tableau use.

---

## Project bootstrap and raw data acquisition

This section addresses the earliest phase **before bronze was fully populated**.

### 1) Environment and platform bootstrap

1. **Bucket was created before data landing**.
- Claim: `gs://msba405-nfl-weather-raw` existed before bronze uploads.
- Evidence tag: **Confirmed by artifact**.
- Basis: bucket metadata shows creation time `Tue, 03 Mar 2026 03:30:34 GMT` (`gsutil ls -L -b`).

2. **Project stack was GCP + Spark + Snowflake + Tableau**.
- Claim: storage/compute/serving/BI stack includes GCS, Spark processing (Dataproc-style workflow), Snowflake, Tableau.
- Evidence tag: **Reconstructed from project evidence**.
- Basis:
  - Spark/PySpark build scripts throughout repository.
  - GCS paths used as canonical storage in every script.
  - Snowflake load/reconciliation scripts and final readiness docs.
  - Tableau-facing serving contracts explicitly documented.
- Note: Dataproc cluster creation commands are not preserved in scripts, so exact cluster bootstrap command history is not directly recoverable from artifacts.

3. **The project ultimately used a layered storage model (`bronze` / `silver` / `qc` / `scratch` / `archive`).**
- Claim: the project used a layered storage design, clearly visible in the final bucket organization and governance artifacts.
- Evidence tag: **Reconstructed from project evidence** for the earliest bootstrap stage; **Confirmed by artifact** for the later enforced project structure.
- Basis:
  - `docs/PROJECT_CONTEXT.md`
  - current bucket root structure
  - cleanup / organization scripts that explicitly enforce layer separation

### 2) Initial bucket/prefix setup (before full bronze population)

1. **Bronze prefix placeholders existed before the observed bronze pbp parquet objects were landed.**
- Claim: bronze root/prefix placeholder objects existed before the bronze pbp parquet objects visible in the inventory snapshot.
- Evidence tag: **Confirmed by artifact**.
- Basis: inventory file `/home/jayyu1/inventory/20260305T065233Z_bronze_objects.txt` shows:
  - `gs://.../bronze/` 0-byte object at `2026-03-04T07:39:46Z`
  - `gs://.../bronze/pbp/` 0-byte object at `2026-03-04T07:40:55Z`

2. **Initial bucket layout started minimal and expanded during buildout**.
- Claim: early prefixes focused on bronze/silver; qc/scratch/archive were formalized later.
- Evidence tag: **Reconstructed from project evidence**.
- Basis:
  - earliest inventories show bronze/silver activity first.
  - organization scripts later create `.keep` markers for `qc/`, `scratch/`, `archive/` and move artifacts out of silver.

3. **Exact command used to create bucket/prefixes is not directly retained**.
- Claim: likely `gsutil mb` + `gsutil cp`/writes were used.
- Evidence tag: **Inferred but plausible**.
- Basis: bucket metadata + object timestamps + standard GCS workflow; no dedicated bootstrap shell script found.

### 3) Raw source acquisition before/into bronze

### 3.1 NFL play-by-play raw source acquisition

1. **Project now maintains two separate raw PBP bronze paths (format-separated by design)**.
- Claim: raw PBP is intentionally separated into:
  - `gs://msba405-nfl-weather-raw/bronze/pbp/` (parquet, Spark/Dataproc processing path)
  - `gs://msba405-nfl-weather-raw/bronze/pbp_csv/` (csv, raw exploration/completeness path)
- Evidence tag: **Confirmed by artifact**.
- Basis: both prefixes are present with yearly objects; post-upload verification confirms retained CSV raw landing.

2. **Confirmed operational parquet landing pattern for bronze PBP**.
- Claim: raw yearly PBP parquet files (`2000-2025`) were pulled from `nflverse/nflverse-data` GitHub Releases tag `pbp` in Cloud Shell, then uploaded to `gs://msba405-nfl-weather-raw/bronze/pbp/`.
- Evidence tag: **Confirmed by artifact**.
- Basis: confirmed project pattern plus retained parquet objects in bronze.

```bash
TAG="pbp"
for y in $(seq 2000 2025); do
  curl -fL --retry 3 --retry-delay 2 \
    -o "play_by_play_${y}.parquet" \
    "https://github.com/nflverse/nflverse-data/releases/download/${TAG}/play_by_play_${y}.parquet"
done

gsutil -m cp play_by_play_*.parquet gs://msba405-nfl-weather-raw/bronze/pbp/
```

3. **Confirmed operational CSV landing pattern for separate bronze raw path**.
- Claim: raw yearly CSV files (`2000-2025`) were downloaded from nflverse releases and uploaded to `gs://msba405-nfl-weather-raw/bronze/pbp_csv/`.
- Evidence tag: **Confirmed by artifact**.
- Basis: current observed `bronze/pbp_csv/` objects and verification summary.

```bash
for y in $(seq 2000 2025); do
  curl -fL --retry 3 --retry-delay 2 \
    -o "play_by_play_${y}.csv" \
    "https://github.com/nflverse/nflverse-data/releases/download/pbp/play_by_play_${y}.csv"
done

gsutil -m cp play_by_play_*.csv gs://msba405-nfl-weather-raw/bronze/pbp_csv/
```

- Confirmed post-upload verification:
  - sample objects include:
    - `gs://msba405-nfl-weather-raw/bronze/pbp_csv/play_by_play_2000.csv`
    - `gs://msba405-nfl-weather-raw/bronze/pbp_csv/play_by_play_2025.csv`
  - exact summary: `TOTAL: 26 objects, 2333020659 bytes (2.17 GiB)`

4. **Raw yearly pbp files also existed locally in Cloud Shell workspace**.
- Claim: annual parquet files (`2000-2025`) were present in `/home/jayyu1/pbp_parquet/`.
- Evidence tag: **Confirmed by artifact**.
- Basis: local files with consistent naming and timestamps (e.g., `play_by_play_2000.parquet` ... `play_by_play_2025.parquet`, mtime `2026-03-04 07:47`).
- The interpretation that `/home/jayyu1/pbp_parquet/` was the practical pre-upload staging area is **Reconstructed from project evidence**.

5. **Source family is nflfastR/nflverse-style pbp datasets (parquet and csv release assets)**.
- Claim: pbp raw source pattern is nflverse release assets, with parquet retained for processing and csv retained for exploration/completeness.
- Evidence tag: **Confirmed by artifact**.
- Basis: `docs/DATASETS.md` explicitly labels `bronze/pbp/play_by_play_YYYY.parquet` source as nflfastR pbp parquet.

### 3.2 Stadium/venue reference acquisition

1. **Stadium metadata was staged locally as CSV**.
- Claim: `stadiums.csv` existed locally under `/home/jayyu1/bronze_reference/stadiums.csv` before silver dim build.
- Evidence tag: **Confirmed by artifact**.
- Basis: local file exists with mtime `2026-03-04 23:25` and builder script references corresponding bronze path.

2. **Stadium CSV was landed to bronze reference path**.
- Claim: stadium reference landed to `gs://msba405-nfl-weather-raw/bronze/reference/stadiums/stadiums.csv`.
- Evidence tag: **Confirmed by artifact**.
- Basis: object present in current bucket and historical inventory with timestamp `2026-03-04T23:25:45Z`.

3. **Likely origin of stadium metadata**.
- Claim: dataset appears consistent with Greer NFL stadium dataset distribution.
- Evidence tag: **Reconstructed from project evidence**.
- Basis: CSV rows include `img_sat_url` entries under `raw.githubusercontent.com/greerrenfl/Stadiums/...`.

4. **Exact acquisition mode for stadium CSV**.
- Claim: likely manual download/export then upload.
- Evidence tag: **Inferred but plausible**.
- Basis: no explicit stadium download script found; local file + bronze landing are present.

### 3.3 NOAA reference acquisition (before NOAA hourly ingestion)

1. **Early NOAA reference/token setup in bronze reference**.
- Claim: NOAA reference inputs were staged under `gs://msba405-nfl-weather-raw/bronze/reference/noaa/`, including `isd-history.csv` and `noaa_token.txt`.
- Evidence tag: **Confirmed by artifact**.
- Basis:
  - local `isd-history.csv` exists at `/home/jayyu1/noaa_ref/isd-history.csv` (mtime `2026-03-05 03:02`).
  - bucket objects exist at:
    - `gs://msba405-nfl-weather-raw/bronze/reference/noaa/isd-history.csv`
    - `gs://msba405-nfl-weather-raw/bronze/reference/noaa/noaa_token.txt`
  - early scripts read token from this bronze reference path.

2. **Early NOAA API usage was exploratory/reference-oriented, not the final raw hourly ingestion method**.
- Claim: token-based NOAA NCEI CDO API calls were used for station lookup/exploration/early checks.
- Evidence tag: **Reconstructed from project evidence**.
- Basis: early token-reading scripts and project context around prechecks and station resolution.
- Example (early exploration pattern only, not final bronze hourly ingestion path):

```bash
curl -sG "https://www.ncei.noaa.gov/cdo-web/api/v2/data" \
  -H "token: $NOAA_TOKEN" \
  --data-urlencode "datasetid=GHCND" \
  --data-urlencode "stationid=GHCND:USW00023174" \
  --data-urlencode "startdate=2025-01-05" \
  --data-urlencode "enddate=2025-01-05" \
  --data-urlencode "datatypeid=TAVG" \
  --data-urlencode "datatypeid=AWND" \
  --data-urlencode "datatypeid=PRCP" \
  --data-urlencode "limit=1000"
```

3. **Final production raw hourly NOAA weather path was manifest-driven ISD/ISD-Lite station-year ingestion**.
- Claim: final raw hourly weather ingestion was not based on bulk CDO JSON storage in bronze.
- Evidence tag: **Confirmed by artifact**.
- Basis:
  - station/year resolution + manifest builders are present (`build_station_year_resolution_v1.py`, `build_noaa_station_year_manifest_v1.py`).
  - bronze ingestion script is present (`ingest_noaa_bronze_isd_v1.py`).
  - current observed bronze NOAA layout is `gs://msba405-nfl-weather-raw/bronze/noaa/isd/source=<...>/year=YYYY/station_key=USAF-WBAN/*.gz`.

4. **Token creation path is not scripted in repo**.
- Claim: token creation/upload likely manual secure handling.
- Evidence tag: **Inferred but plausible**.
- Basis: no script generates token file; scripts only read it.

### 3.4 Initial bronze creation sequence

1. **Bronze pbp loaded first, then bronze/reference components**.
- Claim: bronze object timestamps show pbp objects landing on `2026-03-04`, with stadium/noaa reference objects appearing later (`2026-03-04` to `2026-03-05`).
- Evidence tag: **Confirmed by artifact** for bucket object timestamp order; **Reconstructed from project evidence** for likely workflow order.
- Basis: timestamp ordering from `/home/jayyu1/inventory/20260305T065233Z_bronze_objects.txt`.

2. **Initial bronze layout became the stable working raw layout for the project**.
- Claim: bronze settled into:
  - `bronze/pbp/play_by_play_YYYY.parquet`
  - `bronze/pbp_csv/play_by_play_YYYY.csv`
  - `bronze/reference/noaa/isd-history.csv`
  - `bronze/reference/noaa/noaa_token.txt`
  - `bronze/reference/stadiums/stadiums.csv`
- Evidence tag: **Confirmed by artifact**.
- Basis: current bucket and docs.

3. **Later bronze evolution added NOAA hourly raw station-year archives**.
- Claim: bronze extended to `bronze/noaa/isd/source=.../year=.../station_key=.../*.gz` after prechecks/resolution.
- Evidence tag: **Confirmed by artifact**.
- Basis: objects present and ingestion script/output contracts.

4. **Later bronze evolution also added a separate raw CSV PBP landing path**.
- Claim: `bronze/pbp_csv/play_by_play_YYYY.csv` was added and retained to keep raw CSV exploration/completeness separate from parquet processing input.
- Evidence tag: **Confirmed by artifact**.
- Basis: observed `bronze/pbp_csv/` object set with full 2000-2025 yearly files; detailed source pattern and verification totals are documented in section `3.1`.

### 3.5 Reconstructed early operational workflow (plain language)

- This early workflow is **Reconstructed from project evidence** (not command-by-command proven): the team appears to have identified source datasets, staged raw/reference files in local Cloud Shell folders, uploaded them into `bronze/` and `bronze/reference/` prefixes, and then started Spark-based curation from those landed raw inputs.
- A brief uncertainty note for this phase: no preserved command-level bootstrap script (and no preserved shell-history entries that fully capture the earliest acquisition sequence) was found, so exact first download/upload commands cannot be reconstructed with full certainty.
- Practical summary of that reconstructed workflow:
  - NFL PBP parquet files were downloaded from nflverse GitHub Releases and uploaded to `bronze/pbp/` for Spark processing.
  - NFL PBP CSV files were also downloaded from nflverse GitHub Releases and uploaded to `bronze/pbp_csv/` for exploration/completeness.
  - NOAA reference inputs were staged into `bronze/reference/noaa/`.
  - Final raw hourly NOAA weather data was later ingested into bronze through the manifest-driven ISD / ISD-Lite station-year file process.

---

## Chronological build history (successful implementations)

### 2026-03-04 (first Spark outputs from bronze)

1. Built initial game-level silver table from bronze pbp.
- Script: `/home/jayyu1/pbp_parquet/pbp_to_game_level.py`
- Input: `gs://msba405-nfl-weather-raw/bronze/pbp/`
- Output: `gs://msba405-nfl-weather-raw/silver/pbp_game_level/`
- Evidence tag: **Confirmed by artifact**.
- Outcome: succeeded; became foundational game-level table.

2. Created and validated venue dimension pipeline from stadiums reference.
- Script: `/home/jayyu1/bronze_reference/dim_venue_build.py`
- Input: `gs://.../bronze/reference/stadiums/stadiums.csv`
- Outputs:
  - `silver/dim_venue/`
  - `silver/dim_venue_bad_rows/`
  - `silver/dim_venue_qc/`
- Evidence tag: **Confirmed by artifact**.
- Outcome: succeeded; `silver/dim_venue/` remained active production table.

### 2026-03-05 (play-level core, station dimension, mapping, prechecks)

3. Developed play-level core builder iterations and stabilized season-partition workflow.
- Scripts:
  - `/home/jayyu1/build_pbp_play_level_core_by_year_v2.py`
  - `/home/jayyu1/build_pbp_play_level_core_by_year_v3.py`
  - `/home/jayyu1/build_pbp_play_level_core_by_year_v4.py`
- Output: `gs://msba405-nfl-weather-raw/silver/pbp_play_level_core/`
- Evidence tag: **Confirmed by artifact**.
- Outcome: succeeded; v4 style became the durable write pattern.

4. Audited pbp schemas and added inspection helpers.
- Scripts:
  - `audit_pbp_columns.py`
  - `audit_pbp_columns_safe.py`
  - `audit_pbp_columns_safe_v2.py`
  - `audit_pbp_columns_by_year.py`
- Evidence tag: **Confirmed by artifact**.
- Outcome: informed robust core schema selection.

5. Implemented station dimension for hourly NOAA mapping.
- Script: `/home/jayyu1/build_dim_station_hourly.py` (finalized root-level version)
- Input: `bronze/reference/noaa/isd-history.csv`
- Output: `silver/dim_station_hourly/`
- Evidence tag: **Confirmed by artifact**.
- Outcome: succeeded; active production dimension.

6. Implemented venue-to-station hourly mapping with multiple refinement versions.
- Script chain:
  - `build_venue_station_map_hourly.py`
  - `..._v2.py` through `..._v7.py`
- Final output: `silver/venue_station_map_hourly/`
- Evidence tag: **Confirmed by artifact**.
- Outcome: succeeded with v7 adjustments (coordinate/radius robustness).

7. Built early NOAA kickoff-hour availability prechecks.
- Script chain:
  - `precheck_isd_kickoff_hour_coverage.py`
  - `precheck_isd_lite_kickoff_hour_coverage_v2.py`
  - `precheck_isd_lite_kickoff_hour_coverage_v3.py`
  - `precheck_isd_lite_kickoff_hour_coverage_v3_1.py`
- Initial outputs were under silver (later moved): sample/QC/missing-case prefixes.
- Evidence tag: **Confirmed by artifact**.
- Outcome: succeeded as diagnostics; output placement later corrected by silver contract cleanup.

8. Added controlled bucket organization scripts.
- Scripts:
  - `organize_bucket_dryrun.sh`
  - `organize_bucket_apply.sh`
  - `silver_find_empty_prefixes.sh`
  - `silver_cleanup_placeholders_apply.sh`
  - `delete_empty_placeholders_dryrun.sh`
  - `delete_empty_placeholders_apply.sh`
- Evidence tag: **Confirmed by artifact**.
- Outcome: enabled safe migration of non-production artifacts from silver to qc/scratch.

### 2026-03-06 (NOAA resolution + manifest + bronze ingest + silver observations)

9. Built station-year fallback resolution for missing NOAA station-years.
- Script: `/home/jayyu1/build_station_year_resolution_v1.py`
- Output base: `gs://msba405-nfl-weather-raw/qc/noaa_station_year_resolution_v1/`
- Evidence tag: **Confirmed by artifact**.
- Outcome: succeeded; used as input to production manifest.

10. Built production station-year ingestion manifest.
- Script: `/home/jayyu1/build_noaa_station_year_manifest_v1.py`
- Outputs:
  - `qc/noaa/production_station_year_manifest_v1/manifest/`
  - `qc/noaa/production_station_year_manifest_v1/qc_summary/`
- Evidence tag: **Confirmed by artifact**.
- Outcome: succeeded.

11. Ingested NOAA raw station-year archives into bronze.
- Script: `/home/jayyu1/ingest_noaa_bronze_isd_v1.py`
- Raw output: `bronze/noaa/isd/source=.../year=.../station_key=.../*.gz`
- Manifest status outputs:
  - `.../ingestion_status/`
  - `.../ingestion_summary/`
  - `.../manifest_resolved/`
- Evidence tag: **Confirmed by artifact**.
- Outcome: succeeded; current observed bronze NOAA layout includes `source=isd` with 802 station-year files, spanning 2000-2025.

12. Built silver NOAA hourly observations table.
- Script: `/home/jayyu1/build_silver_noaa_hourly_observations_v1.py`
- Output: `silver/noaa_hourly_observations/`
- QC output: `qc/noaa/noaa_hourly_build_v1/...`
- Evidence tag: **Confirmed by artifact**.
- Outcome: succeeded; became stable NOAA hourly source table for joins.

13. Locked silver governance contract.
- Contract doc: `/home/jayyu1/docs/SILVER_CONTRACT.md`
- Bucket copy: `gs://msba405-nfl-weather-raw/qc/contracts/silver_contract_v1_20260306T013141Z.md`
- Evidence tag: **Confirmed by artifact**.
- Outcome: only production-required datasets retained in silver.

### 2026-03-07 (extended silver weather features)

14. Designed and implemented separate extended weather feature layer in silver.
- Contract: `/home/jayyu1/docs/NOAA_HOURLY_FEATURES_EXTENDED_V1_CONTRACT.md`
- Builder: `/home/jayyu1/build_silver_noaa_hourly_features_extended_v1.py`
- Validator: `/home/jayyu1/validate_silver_noaa_hourly_features_extended_v1.py`
- Output: `silver/noaa_hourly_features_extended/v1/`
- Evidence tag: **Confirmed by artifact**.
- Outcome: succeeded and validated; join key contract preserved (`station_key`,`obs_hour_utc`).

### 2026-03-08 (gold fact + marts + serving publication)

15. Built play-level weather fact table and weather-bin dimension.
- Builder: `/home/jayyu1/build_gold_batch_a_v1.py`
- Validator: `/home/jayyu1/validate_gold_batch_a_v1.py`
- Outputs:
  - `gold/fact_play_weather_hourly_v1/`
  - `gold/dim_weather_bins_v1/`
- QC: `qc/gold/fact_play_weather_hourly_v1/...`
- Evidence tag: **Confirmed by artifact**.
- Outcome: succeeded and validated.

16. Built three analytical marts from the fact table.
- Builder: `/home/jayyu1/build_gold_batch_b_v1.py`
- Validator: `/home/jayyu1/validate_gold_batch_b_v1.py`
- Outputs:
  - `gold/mart_team_season_weather_v1/`
  - `gold/mart_play_calling_weather_v1/`
  - `gold/mart_indoor_outdoor_compare_v1/`
- Contract freeze: `/home/jayyu1/docs/BATCH_B_CONTRACT_FREEZE_20260308.md`
- Evidence tag: **Confirmed by artifact**.
- Outcome: succeeded; schema/grain/metric/bin contracts frozen for downstream serving.

17. Published stable serving layer in GCS for BI consumption.
- Builder: `/home/jayyu1/build_gold_batch_c1_tableau_serving_v1.py`
- Validator: `/home/jayyu1/validate_gold_batch_c1_tableau_serving_v1.py`
- Outputs under `gold/tableau_serving/v1/`:
  - `serv_team_season_weather_v1/`
  - `serv_play_calling_weather_v1/`
  - `serv_indoor_outdoor_compare_v1/`
  - `run_audit_v1/`, `table_audit_v1/`, `dq_audit_v1/`, `data_dictionary_v1/`
- Evidence tag: **Confirmed by artifact**.
- Outcome: succeeded and validated.

### 2026-03-08 to 2026-03-09 (Snowflake load and release readiness)

18. Snowflake object setup scripts prepared and validated.
- Files:
  - `/home/jayyu1/snowflake/batch_c2/phase1/phase1_setup.sql`
  - `/home/jayyu1/snowflake/batch_c2/phase1/run_phase1_setup.py`
- Objects:
  - schemas `STG`, `SERVING`, `AUDIT`
  - format `STG.FF_PARQUET_V1`
  - stage `STG.GCS_TABLEAU_SERVING_V1`
- Evidence tag: **Confirmed by artifact**.
- Outcome: complete.

19. Loaded serving tables to Snowflake incrementally using validated pattern.
- Loaders:
  - `run_phase2_team_load.py`
  - `run_phase2_play_load.py`
  - `run_phase2_indoor_load.py`
- Targets:
  - `NFL_WEATHER.SERVING.SERV_TEAM_SEASON_WEATHER_V1`
  - `NFL_WEATHER.SERVING.SERV_PLAY_CALLING_WEATHER_V1`
  - `NFL_WEATHER.SERVING.SERV_INDOOR_OUTDOOR_COMPARE_V1`
- Standardized fix: parse `season` and `weather_sample` from `METADATA$FILENAME` in `COPY INTO`.
- Evidence tag: **Confirmed by artifact**.
- Outcome: complete and reconciled.

20. Synced metadata/audit serving tables to Snowflake.
- Loader: `run_phase2_c1_metadata_loads.py`
- Targets:
  - `AUDIT.RUN_AUDIT_V1`
  - `AUDIT.TABLE_AUDIT_V1`
  - `AUDIT.DQ_AUDIT_V1`
  - `AUDIT.DATA_DICTIONARY_V1`
- Evidence tag: **Confirmed by artifact**.
- Outcome: complete.

21. Wrote reconciliation audits and final readiness summary.
- Scripts:
  - `run_phase2_reconciliation_audit.py`
  - `run_batch_c2_final_readiness.py`
- Status artifacts:
  - `/home/jayyu1/snowflake/batch_c2/STATUS.md`
  - `/home/jayyu1/snowflake/batch_c2/FINAL_READINESS_SUMMARY.md`
- Evidence tag: **Confirmed by artifact**.
- Outcome: final readiness PASS, no known completeness blockers for Tableau.

22. Added separate raw CSV NFL PBP landing path in bronze.
- Source pattern: `https://github.com/nflverse/nflverse-data/releases/download/pbp/play_by_play_YYYY.csv`
- Output: `gs://msba405-nfl-weather-raw/bronze/pbp_csv/`
- Verification summary: `TOTAL: 26 objects, 2333020659 bytes (2.17 GiB)`; sample files include years `2000` and `2025`.
- Evidence tag: **Confirmed by artifact**.
- Outcome: raw-format separation enforced (`bronze/pbp/` parquet for Spark; `bronze/pbp_csv/` for raw exploration/completeness).

---

## Chronological failed/replaced/debugging history (separate)

### 1) Early play-level core write strategy rework

- Problem: schema/partition robustness across years required multiple iterations.
- Scripts replaced: `build_pbp_play_level_core_by_year_v2.py` -> `v3.py` -> `v4.py`.
- Additional corrective script: `/home/jayyu1/fix_duplicate_season_pbp_core.py`.
- Evidence tag: **Confirmed by artifact**.
- Final status: stabilized on per-season write model with duplicate-season safeguards.

### 2) Venue-station mapping rework (v1-v7)

- Problem: unmapped venues and coordinate-quality issues.
- Root causes (reconstructed from script diffs):
  - longitude sign anomalies for US venues
  - radius threshold too restrictive for some venues
  - schema normalization and debug visibility gaps
- Fixes:
  - coordinate correction logic
  - max distance increase to 200 km
  - nearest-any debug outputs for unmapped diagnostics
- Evidence tag: **Confirmed by artifact** for script evolution; **Reconstructed from project evidence** for root-cause narrative.
- Final status: v7 output active and stable.

### 3) NOAA precheck and parsing rework

- Problem: early precheck approach was brittle for true source availability checks.
- Root causes:
  - early endpoint/identifier assumptions
  - station_key split assumptions
  - kickoff-time parsing edge cases
- Fixes:
  - shifted to station-year file existence checks on NOAA ISD-Lite paths
  - robust station_key parsing in v3.1
  - improved kickoff parse logic and season-year sanity filter
- Evidence tag: **Confirmed by artifact** for script progression; **Reconstructed from project evidence** for root-cause chain.
- Final status: precheck role limited to diagnostics feeding fallback/manifest process.

### 4) Silver contamination cleanup rework

- Problem: precheck/QC/scratch artifacts initially written under silver.
- Fixes:
  - documented silver contract
  - inventory-driven move scripts
  - placeholder cleanup scripts
- Evidence tag: **Confirmed by artifact**.
- Final status: current silver contains only production-required datasets.

### 5) Snowflake partition-field load issue

- Problem: partition-derived columns (`season`, `weather_sample`) can load as null if treated as parquet payload columns.
- Fix:
  - extract partition values from `METADATA$FILENAME` during `COPY INTO`.
- Evidence tag: **Confirmed by artifact** (loader SQL and status docs).
- Final status: adopted as standard loader pattern for all serving tables.

---

## Data pipeline evolution by layer

### Bronze

- Initial bronze landing (confirmed):
  - `bronze/pbp/play_by_play_2000.parquet` ... `play_by_play_2025.parquet`
  - `bronze/pbp_csv/play_by_play_2000.csv` ... `play_by_play_2025.csv`
  - `bronze/reference/stadiums/stadiums.csv`
  - `bronze/reference/noaa/isd-history.csv`
  - `bronze/reference/noaa/noaa_token.txt`
- Current observed retained raw CSV landing path:
  - `gs://msba405-nfl-weather-raw/bronze/pbp_csv/play_by_play_YYYY.csv`
  - footprint snapshot: `26` objects, `2333020659` bytes (`2.17 GiB`)
  - purpose/status/role: active upstream raw (play-level CSV) retained for exploration/completeness; parquet remains the primary Spark-processing input
  - Evidence tag: **Confirmed by artifact**.
- Later bronze expansion (confirmed):
  - `bronze/noaa/isd/source=isd/year=YYYY/station_key=USAF-WBAN/*.gz`
- Current NOAA bronze footprint snapshot:
  - total station-year `.gz` objects: `802`
  - year coverage: `2000` to `2025`
  - unique station keys represented: `47`

### Silver

Current active silver production prefixes:
- `silver/pbp_play_level_core/`
- `silver/pbp_game_level/`
- `silver/dim_venue/`
- `silver/dim_station_hourly/`
- `silver/venue_station_map_hourly/`
- `silver/noaa_hourly_observations/`
- `silver/noaa_hourly_features_extended/v1/`

Deprecated/replaced silver outputs (historical):
- early venue mapping prototypes under `silver/venue_station_map*` families (non-hourly variants)
- temporary `silver/pbp_play_level_core_v2/`

QC/scratch outputs intentionally moved out of silver:
- now under `qc/` and `scratch/` prefixes.

### Gold

Active gold analytical outputs:
- `gold/fact_play_weather_hourly_v1/`
- `gold/dim_weather_bins_v1/`
- `gold/mart_team_season_weather_v1/`
- `gold/mart_play_calling_weather_v1/`
- `gold/mart_indoor_outdoor_compare_v1/`

Serving publication in GCS:
- `gold/tableau_serving/v1/serv_team_season_weather_v1/`
- `gold/tableau_serving/v1/serv_play_calling_weather_v1/`
- `gold/tableau_serving/v1/serv_indoor_outdoor_compare_v1/`
- plus `run_audit_v1`, `table_audit_v1`, `dq_audit_v1`, `data_dictionary_v1`

### Snowflake and Tableau contract

Final Tableau-facing contract tables (active):
- `NFL_WEATHER.SERVING.SERV_TEAM_SEASON_WEATHER_V1`
- `NFL_WEATHER.SERVING.SERV_PLAY_CALLING_WEATHER_V1`
- `NFL_WEATHER.SERVING.SERV_INDOOR_OUTDOOR_COMPARE_V1`

Audit metadata tables (active, non-visualization contract):
- `NFL_WEATHER.AUDIT.RUN_AUDIT_V1`
- `NFL_WEATHER.AUDIT.TABLE_AUDIT_V1`
- `NFL_WEATHER.AUDIT.DQ_AUDIT_V1`
- `NFL_WEATHER.AUDIT.DATA_DICTIONARY_V1`
- `NFL_WEATHER.AUDIT.BATCH_C2_RUN_AUDIT_V1`
- `NFL_WEATHER.AUDIT.BATCH_C2_TABLE_RECON_V1`

Contract rule:
- `SERVING` schema is the only Tableau-facing contract.
- `STG` and transient load tables are not dashboard sources.

---

## Validation and readiness history

### Silver extended features validation
- Script: `validate_silver_noaa_hourly_features_extended_v1.py`
- Key checks: row/key parity, duplicate keys, missing/extra keys, domain/range checks, run metadata, year range.
- Status: **Confirmed by artifact** -> `VALIDATION_PASSED`.

### Gold fact validation
- Script: `validate_gold_batch_a_v1.py`
- Key checks: required schema, row parity with pbp, key uniqueness, bin completeness, match-rate thresholds, QC consistency.
- Status: **Confirmed by artifact** -> `VALIDATION_PASSED`.

### Gold marts validation + contract freeze
- Script: `validate_gold_batch_b_v1.py`
- Freeze doc: `docs/BATCH_B_CONTRACT_FREEZE_20260308.md`
- Frozen row counts:
  - team mart `21858`
  - play-calling mart `16792`
  - indoor/outdoor mart `145`
- Duplicate grain rows: `0` across all three.
- Cross-mart total reconciliation: pass.
- Status: **Confirmed by artifact**.

### Serving validation (GCS)
- Script: `validate_gold_batch_c1_tableau_serving_v1.py`
- Checks: source/serving row parity, grain uniqueness, metric ranges, metadata fields, `_SUCCESS` markers, DQ audit status.
- Status: **Confirmed by artifact** -> `VALIDATION_PASSED`.

### Snowflake load/reconciliation/final readiness
- Status doc: `/home/jayyu1/snowflake/batch_c2/STATUS.md`
- Final summary: `/home/jayyu1/snowflake/batch_c2/FINAL_READINESS_SUMMARY.md`
- Final validated serving row counts:
  - `SERV_TEAM_SEASON_WEATHER_V1 = 21858`
  - `SERV_PLAY_CALLING_WEATHER_V1 = 16792`
  - `SERV_INDOOR_OUTDOOR_COMPARE_V1 = 145`
- Partition null check: `0` for all serving tables.
- Grain duplicate check: `0` for all serving tables.
- Final readiness timestamp: `2026-03-09 07:32:27Z`.
- Release status: PASS.

Evidence tag for this section: **Confirmed by artifact**.

---

## Current final state (production-ready)

1. **GCS has stable final serving outputs and audit metadata under** `gold/tableau_serving/v1/`.
- Evidence tag: **Confirmed by artifact**.

2. **Snowflake SERVING has the three official Tableau contract tables** with reconciled counts and no duplicate grain rows.
- Evidence tag: **Confirmed by artifact**.

3. **Snowflake AUDIT has both serving-build metadata and Snowflake-load reconciliation records**.
- Evidence tag: **Confirmed by artifact**.

4. **Tableau can proceed without known completeness blockers in current release**.
- Evidence tag: **Confirmed by artifact** (final readiness summary PASS).

---

## Known limitations and caveats (current)

1. `precip_bin` is currently `UNKNOWN` across serving marts.
- Evidence tag: **Confirmed by artifact**.

2. `UNKNOWN` values in weather/roof bins are valid and expected in edge cases.
- Evidence tag: **Confirmed by artifact**.

3. Serving tables are pre-aggregated marts, not play-level facts; custom re-aggregation should be weighted.
- Evidence tag: **Reconstructed from project evidence**.

4. Season scope in current release is `2000-2025`.
- Evidence tag: **Confirmed by artifact**.

---

## Gaps and uncertainty notes

1. Exact earliest shell commands for download and upload (e.g., specific `wget` / `gsutil cp` invocations) are not preserved in dedicated bootstrap scripts.
- Evidence tag: **Confirmed by artifact** (no dedicated bootstrap ingestion script and no preserved shell-history entries that fully capture the earliest raw-acquisition command sequence).
- Therefore, the exact first download/upload command order cannot be reconstructed with full certainty.

2. Early raw acquisition method is reconstructed primarily from local staged files, their timestamps, and matching bronze object timestamps.
- Evidence tag: **Reconstructed from project evidence**.

3. Where command-level history is missing, manual/out-of-band acquisition is inferred as plausible workflow.
- Evidence tag: **Inferred but plausible**.
