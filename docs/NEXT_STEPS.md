# Next steps

## NOAA Step 2 (hourly)
Objective: ingest hourly weather for the mapped stations, at least for kickoff-hour coverage across early->recent seasons.

### Data source preference
1) NOAA ISD (preferred for hourly station observations)
2) ISD-Lite as fallback if needed, but verify availability per year/station

### Prechecks required
- Validate station_key -> (usaf,wban) parse for all mapped stations
- For a stratified sample of seasons (early, mid, recent), check existence of station-year files (HTTP 200 vs 404)
- Define fallback mapping strategy for missing station-year (nearest alternative station within max_km)

### Deliverables
- bronze/noaa/isd/... raw hourly files (or an indexed manifest)
- silver/noaa_hourly_observations (long table) with normalized schema
- QC: coverage by year/station and missing cases
