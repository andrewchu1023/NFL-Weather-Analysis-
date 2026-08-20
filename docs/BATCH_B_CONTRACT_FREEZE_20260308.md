# Batch B Contract Freeze (Pre-Batch C)

Freeze timestamp (UTC): 2026-03-08

Scope:
- `gs://msba405-nfl-weather-raw/gold/mart_team_season_weather_v1/`
- `gs://msba405-nfl-weather-raw/gold/mart_play_calling_weather_v1/`
- `gs://msba405-nfl-weather-raw/gold/mart_indoor_outdoor_compare_v1/`

Validated source run stamps:
- `batch_a_build_run_ts = 20260308T012622Z`
- `batch_b_build_run_ts = 20260308T074409Z`

## 1) Schema stability (frozen)

### mart_team_season_weather_v1
| column | type |
|---|---|
| team | string |
| temp_bin | string |
| wind_bin | string |
| precip_bin | string |
| roof_env_bin | string |
| n_plays | bigint |
| n_pass_attempts | bigint |
| n_rush_attempts | bigint |
| n_completions | bigint |
| n_epa_plays | bigint |
| n_games | bigint |
| n_games_points | bigint |
| n_team_games_points | bigint |
| pass_rate | double |
| completion_pct | double |
| epa_per_play | double |
| points_per_game | double |
| batch_a_build_run_ts | string |
| batch_b_build_run_ts | string |
| season | int |
| weather_sample | string |

### mart_play_calling_weather_v1
| column | type |
|---|---|
| team | string |
| temp_bin | string |
| wind_bin | string |
| precip_bin | string |
| n_plays | bigint |
| n_pass_attempts | bigint |
| n_rush_attempts | bigint |
| n_games | bigint |
| pass_rate | double |
| rush_rate | double |
| pass_rate_baseline | double |
| rush_rate_baseline | double |
| pass_rate_shift_vs_team_season | double |
| rush_rate_shift_vs_team_season | double |
| batch_a_build_run_ts | string |
| batch_b_build_run_ts | string |
| season | int |
| weather_sample | string |

### mart_indoor_outdoor_compare_v1
| column | type |
|---|---|
| roof_env_bin | string |
| n_plays | bigint |
| n_pass_attempts | bigint |
| n_rush_attempts | bigint |
| n_completions | bigint |
| n_epa_plays | bigint |
| n_games | bigint |
| n_games_points | bigint |
| n_team_games_points | bigint |
| n_teams | bigint |
| pass_rate | double |
| completion_pct | double |
| epa_per_play | double |
| points_per_game | double |
| batch_a_build_run_ts | string |
| batch_b_build_run_ts | string |
| season | int |
| weather_sample | string |

Schema change status:
- No pending column additions/removals detected in current build/validate scripts.
- Builder uses explicit final `select(...)` lists for all 3 marts.

## 2) Grain contract validation (frozen)

Frozen grains:
- `mart_team_season_weather_v1`: `season, team, temp_bin, wind_bin, precip_bin, roof_env_bin, weather_sample`
- `mart_play_calling_weather_v1`: `season, team, temp_bin, wind_bin, precip_bin, weather_sample`
- `mart_indoor_outdoor_compare_v1`: `season, roof_env_bin, weather_sample`

Duplicate checks at grain:
- team mart duplicates: `0`
- play-calling mart duplicates: `0`
- indoor/outdoor mart duplicates: `0`

## 3) Metric definition freeze (frozen)

Definitions used by Batch B builder:
- `pass_rate = n_pass_attempts / (n_pass_attempts + n_rush_attempts)`
- `rush_rate = n_rush_attempts / (n_pass_attempts + n_rush_attempts)`
- `completion_pct = n_completions / n_pass_attempts`
- `epa_per_play = epa_sum / n_epa_plays`
- `points_per_game = avg(team_points_final)` over distinct `(team, game_id)` within mart grain
- `pass_rate_shift_vs_team_season = pass_rate - pass_rate_baseline`
- `rush_rate_shift_vs_team_season = rush_rate - rush_rate_baseline`

Metric logic change status:
- No TODO/FIXME/PENDING markers in Batch B build/validate scripts.
- Official validator checks formula consistency for shift metrics and metric ranges.

## 4) Weather bin contract freeze (frozen)

Source dimension: `gs://msba405-nfl-weather-raw/gold/dim_weather_bins_v1/`

`temp_bin`:
- `UNKNOWN`
- `FREEZING_LT_0C` (`< 0C`)
- `COLD_0_10C` (`[0,10)`)
- `COOL_10_20C` (`[10,20)`)
- `MILD_20_30C` (`[20,30)`)
- `HOT_GE_30C` (`>= 30C`)

`wind_bin`:
- `UNKNOWN`
- `CALM_LT_3_MPS` (`< 3`)
- `BREEZY_3_6_MPS` (`[3,6)`)
- `WINDY_6_10_MPS` (`[6,10)`)
- `STRONG_GE_10_MPS` (`>= 10`)

`precip_bin`:
- `UNKNOWN` (current v1 state)

`roof_env_bin`:
- `UNKNOWN`, `INDOOR`, `OUTDOOR`, `RETRACTABLE`

Dimension checks:
- `dim_weather_bins_v1` row count: `16`
- UNKNOWN rows in dim: `4`
- bin dimension run stamp: `batch_a_build_run_ts = 20260308T012622Z`

## 5) Cross-mart reconciliation

Official validator result: `VALIDATION_PASSED`

Expected offensive play totals (from fact):
- `ALL_PLAYS = 877137`
- `OBSERVED_ONLY = 734248`

Reconciled totals by mart (`sum(n_plays)` by `weather_sample`):
- team mart: `ALL_PLAYS=877137`, `OBSERVED_ONLY=734248`
- play-calling mart: `ALL_PLAYS=877137`, `OBSERVED_ONLY=734248`
- indoor/outdoor mart: `ALL_PLAYS=877137`, `OBSERVED_ONLY=734248`

Row counts:
- team mart: `21858`
- play-calling mart: `16792`
- indoor/outdoor mart: `145`

## 6) Output stability (complete)

`_SUCCESS` present:
- all 3 gold mart output roots
- all 3 QC summary roots

QC summary run stamp alignment:
- all 3 summaries report `batch_b_build_run_ts = 20260308T074409Z`

## Batch C readiness

Pre-Batch C checks pass.
Batch B mart contract is frozen for downstream Tableau serving and Snowflake staging/merge.
