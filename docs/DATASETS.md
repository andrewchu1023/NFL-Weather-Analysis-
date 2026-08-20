# Datasets catalog

## bronze/
- bronze/pbp/play_by_play_YYYY.parquet
  - Source: nflfastR pbp parquet (raw)
  - Grain: play
  - Use: upstream raw

- bronze/reference/noaa/noaa_token.txt
  - Token for NOAA API (keep private)

- bronze/reference/stadiums/...
  - Stadium/venue reference data

## silver/
- silver/pbp_play_level_core/season=YYYY/
  - Grain: play
  - Partition: season
  - Key: game_id + play_id
  - Status: production

- silver/pbp_game_level/
  - Grain: game
  - Key: game_id
  - Status: production

- silver/dim_venue/
  - Grain: stadium_id
  - Fields: stadium_id, stadium_name, lat, lon, tz, city, state, country, roof_type, first_game_date, last_game_date
  - Status: production

- silver/dim_station_hourly/
  - Grain: station_key (USAF-WBAN)
  - Fields: station_key, station_name, station_lat, station_lon, state, begin_year, end_year, country
  - Status: production (US only)

- silver/venue_station_map_hourly/
  - Grain: stadium_id
  - Fields: stadium_id, station_key, distance_km, selection_reason (+ venue/station coords)
  - Status: production (US venues)

## qc/ and scratch/
- qc/: moved QC tables, unmatched lists, coverage checks (keep for auditing)
- scratch/: samples, precheck outputs (safe to purge later)
