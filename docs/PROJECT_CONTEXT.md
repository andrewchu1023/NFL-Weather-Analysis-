# Project context (NFL + Weather)

## Goal
Build an NFL play/game-level dataset and enrich it with NOAA weather (hourly at kickoff hour).

## Storage conventions
- bronze/: raw ingested source files (minimal transforms)
- silver/: curated tables used downstream (stable schemas, partitioning)
- qc/: quality-check outputs, diagnostics, unmatched lists (not required for downstream joins)
- scratch/: experiments, samples, prechecks (temporary)
- archive/: point-in-time copies for rollback/reference

## Non-negotiable rules
- Do not delete real data objects unless explicitly approved.
- Always run a dry-run/inventory step before moving or deleting.
- Prefer mv over rm when cleaning.
- Keep only "production needed" datasets in silver/.

## Current state (high level)
- pbp_play_level_core written for seasons 2000-2025.
- pbp_game_level exists.
- dim_venue exists (US-focused for mapping).
- dim_station_hourly exists (US only, station_key looks like USAF-WBAN).
- venue_station_map_hourly exists (US venues mapped to station_key).
- NOAA precheck used ISD-Lite URLs (ncei.noaa.gov) and showed partial 404s for a few station-years.
