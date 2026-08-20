#!/usr/bin/env bash
set -Eeuo pipefail

# Official v1 orchestration entrypoint for NFL Weather pipeline.
# This script orchestrates existing Python scripts only; no business logic is reimplemented here.

readonly ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly BUCKET="msba405-nfl-weather-raw"
readonly RUN_TS="$(date -u +%Y%m%dT%H%M%SZ)"
readonly LOG_DIR="${ROOT_DIR}/logs/pipeline_v1/${RUN_TS}"

MODE="default"                   # default | extended-rebuild
WITH_SNOWFLAKE=0                 # 0 | 1
WITH_SNOWFLAKE_PHASE1=0          # 0 | 1 (only used if WITH_SNOWFLAKE=1)
DRY_RUN=0                        # 0 | 1

CURRENT_STEP=""

usage() {
  cat <<'EOF'
Usage:
  ./run_pipeline_v1.sh [options]

Options:
  --mode <default|extended-rebuild>   Pipeline mode (default: default)
  --with-snowflake                    Run Snowflake publish/readiness steps after GCS serving validation
  --with-snowflake-phase1             Also run Snowflake phase1 setup (first-time/bootstrap only)
  --dry-run                           Print planned actions without executing
  -h, --help                          Show help

Modes:
  default:
    Start from stable silver state and run validated gold + serving pipeline.

  extended-rebuild:
    Rebuild foundation/silver tables first, then run default gold + serving pipeline.

Snowflake order (when enabled):
  1) phase1 setup only if requested
  2) team load
  3) play load
  4) indoor load
  5) C1 metadata load
  6) reconciliation audit
  7) final readiness
EOF
}

log() {
  printf '[%s] %s\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "$*"
}

die() {
  log "ERROR: $*"
  exit 1
}

on_error() {
  local exit_code=$?
  local line_no="${1:-unknown}"
  log "FAILED: step='${CURRENT_STEP:-unknown}' line=${line_no} exit_code=${exit_code}"
  log "Logs: ${LOG_DIR}"
  exit "${exit_code}"
}
trap 'on_error $LINENO' ERR

run_cmd() {
  local step="$1"
  shift
  CURRENT_STEP="${step}"
  local log_file="${LOG_DIR}/$(echo "${step}" | tr ' /:' '___').log"

  log "START: ${step}"
  if [[ "${DRY_RUN}" -eq 1 ]]; then
    log "DRY_RUN CMD: $*"
    log "END: ${step}"
    return 0
  fi

  (
    cd "${ROOT_DIR}"
    "$@"
  ) 2>&1 | tee "${log_file}"

  log "END: ${step}"
}

run_python() {
  local step="$1"
  local script_rel="$2"
  shift 2
  run_cmd "${step}" python3 "${ROOT_DIR}/${script_rel}" "$@"
}

require_cmd() {
  local cmd="$1"
  command -v "${cmd}" >/dev/null 2>&1 || die "Required command not found: ${cmd}"
}

require_python_module() {
  local module="$1"
  python3 - <<PY >/dev/null 2>&1
import importlib
importlib.import_module("${module}")
PY
}

require_file() {
  local rel_path="$1"
  [[ -f "${ROOT_DIR}/${rel_path}" ]] || die "Required file missing: ${rel_path}"
}

check_gcs_prefix() {
  local uri="$1"
  if [[ "${DRY_RUN}" -eq 1 ]]; then
    log "DRY_RUN GCS CHECK: ${uri}"
    return 0
  fi
  gsutil ls -d "${uri}" >/dev/null 2>&1 || die "Required GCS prefix/object missing: ${uri}"
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --mode)
        [[ $# -ge 2 ]] || die "--mode requires a value"
        MODE="$2"
        shift 2
        ;;
      --with-snowflake)
        WITH_SNOWFLAKE=1
        shift
        ;;
      --with-snowflake-phase1)
        WITH_SNOWFLAKE_PHASE1=1
        WITH_SNOWFLAKE=1
        shift
        ;;
      --dry-run)
        DRY_RUN=1
        shift
        ;;
      -h|--help)
        usage
        exit 0
        ;;
      *)
        die "Unknown argument: $1"
        ;;
    esac
  done

  case "${MODE}" in
    default|extended-rebuild) ;;
    *) die "Unsupported --mode '${MODE}'. Use default or extended-rebuild." ;;
  esac
}

common_preflight() {
  mkdir -p "${LOG_DIR}"
  log "Run timestamp: ${RUN_TS}"
  log "Mode: ${MODE}"
  log "Snowflake: ${WITH_SNOWFLAKE} (phase1=${WITH_SNOWFLAKE_PHASE1})"
  log "Dry run: ${DRY_RUN}"

  require_cmd python3
  require_cmd gsutil
  require_python_module pyspark || die "Missing Python module: pyspark"

  if [[ "${WITH_SNOWFLAKE}" -eq 1 ]]; then
    require_python_module snowflake.connector || die "Missing Python module: snowflake-connector-python"
    [[ -n "${SNOWFLAKE_USER:-}" ]] || die "SNOWFLAKE_USER is required when --with-snowflake is set"
    [[ -n "${SNOWFLAKE_PASSWORD:-}" ]] || die "SNOWFLAKE_PASSWORD is required when --with-snowflake is set"
    if [[ "${WITH_SNOWFLAKE_PHASE1}" -eq 1 ]]; then
      [[ -n "${SNOWFLAKE_STORAGE_INTEGRATION:-}" ]] || die "SNOWFLAKE_STORAGE_INTEGRATION is required for --with-snowflake-phase1"
    fi
  fi
}

default_mode_preflight() {
  # Explicit stable silver-state checks for default v1 scope.
  local required_silver=(
    "gs://${BUCKET}/silver/pbp_play_level_core/"
    "gs://${BUCKET}/silver/pbp_game_level/"
    "gs://${BUCKET}/silver/dim_venue/"
    "gs://${BUCKET}/silver/dim_station_hourly/"
    "gs://${BUCKET}/silver/venue_station_map_hourly/"
    "gs://${BUCKET}/silver/noaa_hourly_observations/"
    "gs://${BUCKET}/silver/noaa_hourly_features_extended/v1/"
  )
  local uri
  for uri in "${required_silver[@]}"; do
    check_gcs_prefix "${uri}"
  done
}

extended_mode_preflight() {
  local required_inputs=(
    "gs://${BUCKET}/bronze/pbp/"
    "gs://${BUCKET}/bronze/reference/stadiums/stadiums.csv"
    "gs://${BUCKET}/bronze/reference/noaa/isd-history.csv"
    "gs://${BUCKET}/bronze/noaa/isd/"
    "gs://${BUCKET}/qc/noaa/production_station_year_manifest_v1/manifest_resolved/"
  )
  local uri
  for uri in "${required_inputs[@]}"; do
    check_gcs_prefix "${uri}"
  done
}

require_official_scripts() {
  local required=(
    "validate_silver_noaa_hourly_features_extended_v1.py"
    "build_gold_batch_a_v1.py"
    "validate_gold_batch_a_v1.py"
    "build_gold_batch_b_v1.py"
    "validate_gold_batch_b_v1.py"
    "build_gold_batch_c1_tableau_serving_v1.py"
    "validate_gold_batch_c1_tableau_serving_v1.py"
  )

  if [[ "${MODE}" == "extended-rebuild" ]]; then
    required+=(
      "bronze_reference/dim_venue_build.py"
      "pbp_parquet/pbp_to_game_level.py"
      "build_pbp_play_level_core_by_year_v4.py"
      "build_dim_station_hourly.py"
      "build_venue_station_map_hourly_v7.py"
      "build_silver_noaa_hourly_observations_v1.py"
      "build_silver_noaa_hourly_features_extended_v1.py"
    )
  fi

  if [[ "${WITH_SNOWFLAKE}" -eq 1 ]]; then
    required+=(
      "snowflake/batch_c2/phase2/run_phase2_team_load.py"
      "snowflake/batch_c2/phase2/run_phase2_play_load.py"
      "snowflake/batch_c2/phase2/run_phase2_indoor_load.py"
      "snowflake/batch_c2/phase2/run_phase2_c1_metadata_loads.py"
      "snowflake/batch_c2/phase2/run_phase2_reconciliation_audit.py"
      "snowflake/batch_c2/phase2/run_batch_c2_final_readiness.py"
    )
    if [[ "${WITH_SNOWFLAKE_PHASE1}" -eq 1 ]]; then
      required+=("snowflake/batch_c2/phase1/run_phase1_setup.py")
    fi
  fi

  local rel
  for rel in "${required[@]}"; do
    require_file "${rel}"
  done
}

run_gold_serving_gcs_pipeline() {
  run_python "validate silver NOAA extended v1" "validate_silver_noaa_hourly_features_extended_v1.py"

  run_python "build gold batch A v1" "build_gold_batch_a_v1.py"
  run_python "validate gold batch A v1" "validate_gold_batch_a_v1.py"

  run_python "build gold batch B v1" "build_gold_batch_b_v1.py"
  run_python "validate gold batch B v1" "validate_gold_batch_b_v1.py"

  run_python "build gold batch C1 tableau serving v1" "build_gold_batch_c1_tableau_serving_v1.py"
  run_python "validate gold batch C1 tableau serving v1" "validate_gold_batch_c1_tableau_serving_v1.py"
}

run_extended_rebuild_prefix() {
  run_python "build silver dim_venue" "bronze_reference/dim_venue_build.py"
  run_python "build silver pbp_game_level" "pbp_parquet/pbp_to_game_level.py"
  run_python "build silver pbp_play_level_core v4" "build_pbp_play_level_core_by_year_v4.py"
  run_python "build silver dim_station_hourly" "build_dim_station_hourly.py"
  run_python "build silver venue_station_map_hourly v7" "build_venue_station_map_hourly_v7.py"
  run_python "build silver noaa_hourly_observations v1" "build_silver_noaa_hourly_observations_v1.py"
  run_python "build silver noaa_hourly_features_extended v1" "build_silver_noaa_hourly_features_extended_v1.py"
}

build_snowflake_args() {
  SNOWFLAKE_ARGS=()
  [[ -n "${SNOWFLAKE_ACCOUNT:-}" ]] && SNOWFLAKE_ARGS+=(--account "${SNOWFLAKE_ACCOUNT}")
  [[ -n "${SNOWFLAKE_ACCOUNT_FALLBACK:-}" ]] && SNOWFLAKE_ARGS+=(--account-fallback "${SNOWFLAKE_ACCOUNT_FALLBACK}")
  [[ -n "${SNOWFLAKE_ROLE:-}" ]] && SNOWFLAKE_ARGS+=(--sf-role "${SNOWFLAKE_ROLE}")
  [[ -n "${SNOWFLAKE_WAREHOUSE:-}" ]] && SNOWFLAKE_ARGS+=(--sf-warehouse "${SNOWFLAKE_WAREHOUSE}")
  [[ -n "${SNOWFLAKE_DATABASE:-}" ]] && SNOWFLAKE_ARGS+=(--sf-database "${SNOWFLAKE_DATABASE}")
  return 0
}

run_snowflake_publish() {
  build_snowflake_args

  if [[ "${WITH_SNOWFLAKE_PHASE1}" -eq 1 ]]; then
    local phase1_args=("${SNOWFLAKE_ARGS[@]}" --sf-storage-integration "${SNOWFLAKE_STORAGE_INTEGRATION}")
    [[ -n "${GCS_SERVING_URL:-}" ]] && phase1_args+=(--gcs-serving-url "${GCS_SERVING_URL}")
    run_python "snowflake phase1 setup" "snowflake/batch_c2/phase1/run_phase1_setup.py" "${phase1_args[@]}"
  fi

  run_python "snowflake phase2 team load" "snowflake/batch_c2/phase2/run_phase2_team_load.py" \
    "${SNOWFLAKE_ARGS[@]}" --expected-rows 21858
  run_python "snowflake phase2 play load" "snowflake/batch_c2/phase2/run_phase2_play_load.py" \
    "${SNOWFLAKE_ARGS[@]}" --expected-rows 16792
  run_python "snowflake phase2 indoor load" "snowflake/batch_c2/phase2/run_phase2_indoor_load.py" \
    "${SNOWFLAKE_ARGS[@]}" --expected-rows 145

  # Requested ordering:
  # metadata load -> reconciliation audit -> final readiness
  run_python "snowflake phase2 C1 metadata load" "snowflake/batch_c2/phase2/run_phase2_c1_metadata_loads.py" \
    "${SNOWFLAKE_ARGS[@]}"
  run_python "snowflake phase2 reconciliation audit" "snowflake/batch_c2/phase2/run_phase2_reconciliation_audit.py" \
    "${SNOWFLAKE_ARGS[@]}" --expected-team-rows 21858 --expected-play-rows 16792 --expected-indoor-rows 145
  run_python "snowflake phase2 final readiness" "snowflake/batch_c2/phase2/run_batch_c2_final_readiness.py" \
    "${SNOWFLAKE_ARGS[@]}"
}

main() {
  parse_args "$@"
  common_preflight
  require_official_scripts

  if [[ "${MODE}" == "default" ]]; then
    default_mode_preflight
    run_gold_serving_gcs_pipeline
  else
    extended_mode_preflight
    run_extended_rebuild_prefix
    run_gold_serving_gcs_pipeline
  fi

  if [[ "${WITH_SNOWFLAKE}" -eq 1 ]]; then
    run_snowflake_publish
  fi

  log "SUCCESS: pipeline completed"
  log "Run logs: ${LOG_DIR}"
}

main "$@"
