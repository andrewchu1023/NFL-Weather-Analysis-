# Batch C2 Final Readiness Summary

- validated_at_utc: `2026-03-09 07:32:27Z`
- snowflake_account: `ekbpfux-vob93769`
- database: `NFL_WEATHER`

## Checklist
- PASS: 1. All 3 serving tables are complete and reconciled
- PASS: 2. All required metadata/audit tables are loaded
- PASS: 3. Snowflake SERVING is the only Tableau-facing contract
- PASS: 4. Tableau can start safely with no known completeness blockers

## Detail
- SERVING.SERV_TEAM_SEASON_WEATHER_V1: rows=21858 expected=21858 dup=0 null_partition=0
- SERVING.SERV_PLAY_CALLING_WEATHER_V1: rows=16792 expected=16792 dup=0 null_partition=0
- SERVING.SERV_INDOOR_OUTDOOR_COMPARE_V1: rows=145 expected=145 dup=0 null_partition=0
- latest_recon_run=c2_recon_20260309T070434Z status=PASS error_count=0
- latest_recon_tables pass=3 total=3
- AUDIT.RUN_AUDIT_V1: rows=1 expected=1 dup=0
- AUDIT.TABLE_AUDIT_V1: rows=3 expected=3 dup=0
- AUDIT.DQ_AUDIT_V1: rows=27 expected=27 dup=0
- AUDIT.DATA_DICTIONARY_V1: rows=72 expected=72 dup=0
- SERVING contract table count=3
- non_SERVING contract-like tables=0

## Known Blockers
- None
