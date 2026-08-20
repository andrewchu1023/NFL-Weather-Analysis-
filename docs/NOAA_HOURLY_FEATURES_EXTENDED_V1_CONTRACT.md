# NOAA Hourly Features Extended v1 Contract

Effective date: 2026-03-07

## Purpose
Define a stable, reproducible, and gold-ready derived weather feature table in silver, without changing `silver/noaa_hourly_observations/`.

## Layering and Paths
- Source (read-only): `gs://msba405-nfl-weather-raw/silver/noaa_hourly_observations/`
- New output: `gs://msba405-nfl-weather-raw/silver/noaa_hourly_features_extended/v1/`
- QC output base: `gs://msba405-nfl-weather-raw/qc/noaa/weather_hourly_features_extended_v1/`

## Non-negotiable Rules
- Do not modify `silver/noaa_hourly_observations/` schema or values.
- Derived features must use only stable fields already in `silver/noaa_hourly_observations/`.
- Join key must remain `station_key + obs_hour_utc`.
- Output must be unique on `station_key + obs_hour_utc`.

## Input Schema (required fields)
- `station_key` string
- `obs_hour_utc` timestamp
- `obs_year` int
- `obs_month` int
- `source_dataset` string
- `source_file_uri` string
- `air_temp_c` double
- `dew_point_c` double
- `sea_level_pressure_hpa` double
- `wind_speed_mps` double
- `wind_dir_deg` double
- `obs_present_flag` int
- `build_run_ts` string

## Output Schema v1
- `station_key` string
- `obs_hour_utc` timestamp
- `obs_year` int (partition column)
- `obs_month` int
- `source_dataset` string
- `obs_present_flag` int
- `relative_humidity_pct` double
- `wet_bulb_temp_c` double
- `apparent_temp_c` double
- `wind_u_mps` double
- `wind_v_mps` double
- `pressure_delta_1h_hpa` double
- `pressure_delta_3h_hpa` double
- `pressure_delta_6h_hpa` double
- `pressure_tendency_3h` int
- `rh_valid_flag` int
- `wet_bulb_valid_flag` int
- `apparent_temp_valid_flag` int
- `wind_components_valid_flag` int
- `pressure_delta_valid_1h_flag` int
- `pressure_delta_valid_3h_flag` int
- `pressure_delta_valid_6h_flag` int
- `source_build_run_ts` string
- `feature_build_run_ts` string
- `feature_version` string (constant `v1`)

## Feature Definitions v1

### 1) Relative Humidity
Formula (Magnus):
- `es(T) = exp((a*T)/(b+T))`, where `a=17.625`, `b=243.04`
- `RH = 100 * es(Td) / es(T)`
- Input domain guard: `T in [-90, 70]`, `Td in [-100, 70]`
- Clamp output to `[0, 100]`

### 2) Wet-bulb Proxy
Formula (Stull approximation):
- `Tw = T*atan(0.151977*sqrt(RH+8.313659)) + atan(T+RH) - atan(RH-1.676331) + 0.00391838*RH^(3/2)*atan(0.023101*RH) - 4.686035`
- Domain guard: `T in [-80, 60]` and `RH in [1, 100]`
- Plausibility guard: `Tw in [-100, 70]` and `Tw <= T + 2`

### 3) Apparent Temperature
Branch logic:
- Wind chill branch if `T <= 10` and `wind_speed_kph > 4.8`
- Humidex branch if `T >= 20` and `Td in [-40, 50]`
- Else fallback to `T`

Wind chill formula:
- `AT_wc = 13.12 + 0.6215*T - 11.37*V^0.16 + 0.3965*T*V^0.16`
- `V` in km/h (`wind_speed_mps * 3.6`)

Humidex formula:
- `e = 6.11 * exp(5417.7530 * ((1/273.16) - (1/(Td+273.15))))`
- `AT_hx = T + 0.5555 * (e - 10)`

### 4) Wind Components
- Normalize direction `360 -> 0`
- Domain guard: `wind_speed_mps in [0, 150]`, `wind_dir_deg in [0, 360]`
- `u = -wind_speed_mps * sin(radians(dir))`
- `v = -wind_speed_mps * cos(radians(dir))`

### 5) Pressure Change Features
For each station ordered by `obs_hour_utc`:
- `delta_1h = slp(t) - slp(t-1h)`
- `delta_3h = slp(t) - slp(t-3h)`
- `delta_6h = slp(t) - slp(t-6h)`

Plausibility guards:
- `abs(delta_1h) <= 20`
- `abs(delta_3h) <= 30`
- `abs(delta_6h) <= 40`

Pressure tendency:
- `pressure_tendency_3h = 1` if `delta_3h > 0.5`
- `pressure_tendency_3h = -1` if `delta_3h < -0.5`
- `pressure_tendency_3h = 0` otherwise

## QC Contract (hard-fail checks)
- Row count equals source silver row count.
- Key uniqueness in output (`station_key + obs_hour_utc`).
- Key coverage parity with source silver (no missing keys, no extra keys).
- `relative_humidity_pct` in `[0, 100]` when non-null.
- `pressure_tendency_3h` in `{-1, 0, 1}` when non-null.
- `feature_version` must be exactly `v1`.

## QC Contract (monitoring checks)
- Null rates by feature overall and by `obs_year`.
- Station-year feature coverage summaries.
- Wind-component reconstruction check:
  - `sqrt(u^2 + v^2)` close to `wind_speed_mps`.
- Pressure delta outlier counts (pre-guard and post-guard if tracked).

## Join-back Contract
- Join key to source NOAA hourly table: `station_key`, `obs_hour_utc`
- For play-level gold tables:
  - Use join keys table containing `station_key` and weather hour in UTC.
  - Left-join to preserve play rows.
  - Assert no row fan-out.
