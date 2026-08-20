#!/usr/bin/env python3
"""Batch C2 Phase 2 (minimal): load one serving table end-to-end.

Scope:
- Source: @STG.GCS_TABLEAU_SERVING_V1/serv_indoor_outdoor_compare_v1/
- Landing table: STG.SERV_INDOOR_OUTDOOR_COMPARE_V1_LND
- Target table:  SERVING.SERV_INDOOR_OUTDOOR_COMPARE_V1

Validation:
- stage path contains parquet files
- COPY loads successfully
- row count == expected
- schema aligned (landing vs target and expected column order)
- key uniqueness at grain
"""

from __future__ import annotations

import argparse
import os
import sys
from typing import List, Sequence, Tuple


EXPECTED_COLUMNS: List[str] = [
    "SEASON",
    "ROOF_ENV_BIN",
    "WEATHER_SAMPLE",
    "N_PLAYS",
    "N_PASS_ATTEMPTS",
    "N_RUSH_ATTEMPTS",
    "N_COMPLETIONS",
    "N_EPA_PLAYS",
    "N_GAMES",
    "N_GAMES_POINTS",
    "N_TEAM_GAMES_POINTS",
    "N_TEAMS",
    "PASS_RATE",
    "COMPLETION_PCT",
    "EPA_PER_PLAY",
    "POINTS_PER_GAME",
    "BATCH_A_BUILD_RUN_TS",
    "BATCH_B_BUILD_RUN_TS",
    "SOURCE_BATCH_A_BUILD_RUN_TS",
    "SOURCE_BATCH_B_BUILD_RUN_TS",
    "SERVING_BUILD_RUN_TS",
    "SERVING_VERSION",
    "SERVING_RUN_ID",
]

KEY_COLUMNS: List[str] = [
    "SEASON",
    "ROOF_ENV_BIN",
    "WEATHER_SAMPLE",
]

CREATE_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS {table_fqn} (
  SEASON NUMBER(10,0),
  ROOF_ENV_BIN VARCHAR,
  WEATHER_SAMPLE VARCHAR,
  N_PLAYS NUMBER(38,0),
  N_PASS_ATTEMPTS NUMBER(38,0),
  N_RUSH_ATTEMPTS NUMBER(38,0),
  N_COMPLETIONS NUMBER(38,0),
  N_EPA_PLAYS NUMBER(38,0),
  N_GAMES NUMBER(38,0),
  N_GAMES_POINTS NUMBER(38,0),
  N_TEAM_GAMES_POINTS NUMBER(38,0),
  N_TEAMS NUMBER(38,0),
  PASS_RATE FLOAT,
  COMPLETION_PCT FLOAT,
  EPA_PER_PLAY FLOAT,
  POINTS_PER_GAME FLOAT,
  BATCH_A_BUILD_RUN_TS VARCHAR,
  BATCH_B_BUILD_RUN_TS VARCHAR,
  SOURCE_BATCH_A_BUILD_RUN_TS VARCHAR,
  SOURCE_BATCH_B_BUILD_RUN_TS VARCHAR,
  SERVING_BUILD_RUN_TS VARCHAR,
  SERVING_VERSION VARCHAR,
  SERVING_RUN_ID VARCHAR
)
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Batch C2 Phase 2: load indoor/outdoor serving table")
    parser.add_argument("--account", default=os.getenv("SNOWFLAKE_ACCOUNT", "ekbpfux-vob93769"))
    parser.add_argument("--account-fallback", default="hbb86970")
    parser.add_argument("--user", default=os.getenv("SNOWFLAKE_USER"))
    parser.add_argument("--password", default=os.getenv("SNOWFLAKE_PASSWORD"))
    parser.add_argument("--authenticator", default=os.getenv("SNOWFLAKE_AUTHENTICATOR"))

    parser.add_argument("--sf-role", default=os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"))
    parser.add_argument("--sf-warehouse", default=os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"))
    parser.add_argument("--sf-database", default=os.getenv("SNOWFLAKE_DATABASE", "NFL_WEATHER"))

    parser.add_argument("--stage-fqn", default="STG.GCS_TABLEAU_SERVING_V1")
    parser.add_argument("--stage-subpath", default="serv_indoor_outdoor_compare_v1/")

    parser.add_argument("--landing-table", default="STG.SERV_INDOOR_OUTDOOR_COMPARE_V1_LND")
    parser.add_argument("--target-table", default="SERVING.SERV_INDOOR_OUTDOOR_COMPARE_V1")
    parser.add_argument("--expected-rows", type=int, default=145)

    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def require_args(args: argparse.Namespace) -> List[str]:
    missing = []
    for key in ["user", "password", "sf_role", "sf_warehouse", "sf_database"]:
        if not getattr(args, key):
            missing.append(key)
    if not args.account and not args.account_fallback:
        missing.append("account")
    return missing


def split_schema_table(fqn: str) -> Tuple[str, str]:
    parts = [p.strip().upper() for p in fqn.split(".") if p.strip()]
    if len(parts) == 2:
        return parts[0], parts[1]
    if len(parts) == 3:
        return parts[1], parts[2]
    raise ValueError(f"Expected <schema>.<table> or <db>.<schema>.<table>, got: {fqn}")


def connect_with_fallback(args: argparse.Namespace):
    try:
        import snowflake.connector  # type: ignore
    except Exception as exc:
        raise RuntimeError(
            "snowflake-connector-python not installed. Run: pip install snowflake-connector-python"
        ) from exc

    accounts: List[str] = []
    if args.account:
        accounts.append(args.account)
    if args.account_fallback and args.account_fallback not in accounts:
        accounts.append(args.account_fallback)

    errors = []
    for acct in accounts:
        kwargs = {
            "account": acct,
            "user": args.user,
            "password": args.password,
            "autocommit": True,
        }
        if args.authenticator:
            kwargs["authenticator"] = args.authenticator

        try:
            conn = snowflake.connector.connect(**kwargs)
            return conn, acct
        except Exception as exc:
            errors.append(f"{acct}: {exc}")

    raise RuntimeError("Unable to connect with provided account ids. " + " | ".join(errors))


def fetch_scalar(cur, sql: str):
    cur.execute(sql)
    row = cur.fetchone()
    return row[0] if row else None


def fetch_schema(cur, database: str, table_fqn: str) -> List[Tuple[str, str, int, int]]:
    schema_name, table_name = split_schema_table(table_fqn)
    cur.execute(
        f"""
        SELECT COLUMN_NAME, DATA_TYPE,
               COALESCE(NUMERIC_PRECISION, -1) AS NUMERIC_PRECISION,
               COALESCE(NUMERIC_SCALE, -1) AS NUMERIC_SCALE
        FROM {database}.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{schema_name}'
          AND TABLE_NAME = '{table_name}'
        ORDER BY ORDINAL_POSITION
        """
    )
    return [(r[0], r[1], int(r[2]), int(r[3])) for r in cur.fetchall()]


def duplicate_row_count(cur, table_fqn: str, keys: Sequence[str]) -> int:
    key_sql = ", ".join(keys)
    sql = f"""
    SELECT COALESCE(SUM(cnt - 1), 0) AS duplicate_rows
    FROM (
      SELECT {key_sql}, COUNT(*) AS cnt
      FROM {table_fqn}
      GROUP BY {key_sql}
      HAVING COUNT(*) > 1
    ) d
    """
    return int(fetch_scalar(cur, sql) or 0)


def main() -> int:
    args = parse_args()

    missing = require_args(args)
    if missing:
        print("ERROR: missing required args/env: " + ", ".join(missing), file=sys.stderr)
        return 2

    stage_subpath = args.stage_subpath.strip("/")
    stage_path = f"@{args.stage_fqn}/{stage_subpath}/"

    if args.dry_run:
        print("DRY_RUN_OK")
        print(f"account_primary={args.account}")
        print(f"account_fallback={args.account_fallback}")
        print(f"database={args.sf_database}")
        print(f"stage_path={stage_path}")
        print(f"landing_table={args.landing_table}")
        print(f"target_table={args.target_table}")
        print(f"expected_rows={args.expected_rows}")
        return 0

    try:
        conn, used_account = connect_with_fallback(args)
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    print(f"CONNECTED_ACCOUNT={used_account}")

    errors: List[str] = []
    copy_rows_loaded = 0
    copy_file_count = 0

    try:
        cur = conn.cursor()
        try:
            cur.execute(f"USE ROLE {args.sf_role}")
            cur.execute(f"USE WAREHOUSE {args.sf_warehouse}")
            cur.execute(f"CREATE DATABASE IF NOT EXISTS {args.sf_database}")
            cur.execute(f"USE DATABASE {args.sf_database}")
            cur.execute("CREATE SCHEMA IF NOT EXISTS STG")
            cur.execute("CREATE SCHEMA IF NOT EXISTS SERVING")
            cur.execute("CREATE SCHEMA IF NOT EXISTS AUDIT")
            cur.execute(
                """
                CREATE FILE FORMAT IF NOT EXISTS STG.FF_PARQUET_V1
                  TYPE = PARQUET
                  USE_LOGICAL_TYPE = TRUE
                  BINARY_AS_TEXT = FALSE
                  NULL_IF = ('NULL', 'null')
                """
            )

            # Verify stage exists and source files are visible.
            cur.execute("SHOW STAGES LIKE 'GCS_TABLEAU_SERVING_V1' IN SCHEMA STG")
            stage_rows = cur.fetchall()
            if not stage_rows:
                errors.append("missing stage: STG.GCS_TABLEAU_SERVING_V1")
            cur.execute(f"LIST {stage_path} PATTERN='.*\\\\.parquet'")
            listed_files = cur.fetchall()
            copy_file_count = len(listed_files)
            if copy_file_count == 0:
                errors.append(f"no parquet files found at {stage_path}")

            # Ensure landing + target table exist (idempotent).
            cur.execute(CREATE_TABLE_DDL.format(table_fqn=args.landing_table))
            cur.execute(CREATE_TABLE_DDL.format(table_fqn=args.target_table))

            # Refresh landing and load from stage.
            cur.execute(f"TRUNCATE TABLE {args.landing_table}")
            copy_sql = f"""
                COPY INTO {args.landing_table}
                  ({', '.join(EXPECTED_COLUMNS)})
                FROM (
                  SELECT
                    TO_NUMBER(REGEXP_SUBSTR(METADATA$FILENAME, 'season=([0-9]+)', 1, 1, 'e', 1)) AS SEASON,
                    $1:roof_env_bin::STRING AS ROOF_ENV_BIN,
                    REGEXP_SUBSTR(METADATA$FILENAME, 'weather_sample=([^/]+)', 1, 1, 'e', 1)::STRING AS WEATHER_SAMPLE,
                    $1:n_plays::NUMBER(38,0) AS N_PLAYS,
                    $1:n_pass_attempts::NUMBER(38,0) AS N_PASS_ATTEMPTS,
                    $1:n_rush_attempts::NUMBER(38,0) AS N_RUSH_ATTEMPTS,
                    $1:n_completions::NUMBER(38,0) AS N_COMPLETIONS,
                    $1:n_epa_plays::NUMBER(38,0) AS N_EPA_PLAYS,
                    $1:n_games::NUMBER(38,0) AS N_GAMES,
                    $1:n_games_points::NUMBER(38,0) AS N_GAMES_POINTS,
                    $1:n_team_games_points::NUMBER(38,0) AS N_TEAM_GAMES_POINTS,
                    $1:n_teams::NUMBER(38,0) AS N_TEAMS,
                    $1:pass_rate::FLOAT AS PASS_RATE,
                    $1:completion_pct::FLOAT AS COMPLETION_PCT,
                    $1:epa_per_play::FLOAT AS EPA_PER_PLAY,
                    $1:points_per_game::FLOAT AS POINTS_PER_GAME,
                    $1:batch_a_build_run_ts::STRING AS BATCH_A_BUILD_RUN_TS,
                    $1:batch_b_build_run_ts::STRING AS BATCH_B_BUILD_RUN_TS,
                    $1:source_batch_a_build_run_ts::STRING AS SOURCE_BATCH_A_BUILD_RUN_TS,
                    $1:source_batch_b_build_run_ts::STRING AS SOURCE_BATCH_B_BUILD_RUN_TS,
                    $1:serving_build_run_ts::STRING AS SERVING_BUILD_RUN_TS,
                    $1:serving_version::STRING AS SERVING_VERSION,
                    $1:serving_run_id::STRING AS SERVING_RUN_ID
                  FROM {stage_path}
                    (FILE_FORMAT => 'STG.FF_PARQUET_V1', PATTERN => '.*\\\\.parquet')
                )
                ON_ERROR = 'ABORT_STATEMENT'
            """
            cur.execute(copy_sql)
            copy_results = cur.fetchall()
            if not copy_results:
                errors.append("COPY INTO returned no result rows")
            else:
                for row in copy_results:
                    # Expected Snowflake COPY output: file,status,rows_parsed,rows_loaded,...
                    status = str(row[1]).upper() if len(row) > 1 and row[1] is not None else "UNKNOWN"
                    rows_loaded = int(row[3]) if len(row) > 3 and row[3] is not None else 0
                    copy_rows_loaded += rows_loaded
                    if status not in {"LOADED", "PARTIALLY_LOADED"}:
                        errors.append(f"COPY file status not loaded: {row}")

            landing_rows = int(fetch_scalar(cur, f"SELECT COUNT(*) FROM {args.landing_table}") or 0)
            if landing_rows != args.expected_rows:
                errors.append(f"landing row count mismatch expected={args.expected_rows} actual={landing_rows}")

            landing_dupes = duplicate_row_count(cur, args.landing_table, KEY_COLUMNS)
            if landing_dupes != 0:
                errors.append(f"landing duplicate key rows={landing_dupes}")
            landing_null_partition = int(
                fetch_scalar(
                    cur,
                    f"SELECT COUNT(*) FROM {args.landing_table} WHERE SEASON IS NULL OR WEATHER_SAMPLE IS NULL",
                )
                or 0
            )
            if landing_null_partition != 0:
                errors.append(f"landing null partition-key rows={landing_null_partition}")
            for metric_col in ["PASS_RATE", "COMPLETION_PCT"]:
                out_of_range = int(
                    fetch_scalar(
                        cur,
                        f"""SELECT COUNT(*) FROM {args.landing_table}
                            WHERE {metric_col} IS NOT NULL AND ({metric_col} < 0 OR {metric_col} > 1)""",
                    )
                    or 0
                )
                if out_of_range != 0:
                    errors.append(f"landing {metric_col} out-of-range rows={out_of_range}")

            # Schema alignment checks.
            landing_schema = fetch_schema(cur, args.sf_database, args.landing_table)
            target_schema = fetch_schema(cur, args.sf_database, args.target_table)

            landing_cols = [c[0] for c in landing_schema]
            target_cols = [c[0] for c in target_schema]

            if landing_cols != EXPECTED_COLUMNS:
                errors.append(
                    f"landing column order mismatch expected={EXPECTED_COLUMNS} actual={landing_cols}"
                )
            if target_cols != EXPECTED_COLUMNS:
                errors.append(
                    f"target column order mismatch expected={EXPECTED_COLUMNS} actual={target_cols}"
                )
            if landing_schema != target_schema:
                errors.append("landing/target schema type mismatch")

            if not errors:
                tmp_table = "SERVING.SERV_INDOOR_OUTDOOR_COMPARE_V1__LOAD_TMP"
                cur.execute(f"CREATE OR REPLACE TABLE {tmp_table} LIKE {args.target_table}")
                cur.execute(
                    f"INSERT INTO {tmp_table} ({', '.join(EXPECTED_COLUMNS)}) "
                    f"SELECT {', '.join(EXPECTED_COLUMNS)} FROM {args.landing_table}"
                )

                tmp_rows = int(fetch_scalar(cur, f"SELECT COUNT(*) FROM {tmp_table}") or 0)
                if tmp_rows != args.expected_rows:
                    errors.append(f"tmp row count mismatch expected={args.expected_rows} actual={tmp_rows}")

                tmp_dupes = duplicate_row_count(cur, tmp_table, KEY_COLUMNS)
                if tmp_dupes != 0:
                    errors.append(f"tmp duplicate key rows={tmp_dupes}")

                if not errors:
                    cur.execute(f"ALTER TABLE {args.target_table} SWAP WITH {tmp_table}")
                    cur.execute(f"DROP TABLE IF EXISTS {tmp_table}")

            target_rows = int(fetch_scalar(cur, f"SELECT COUNT(*) FROM {args.target_table}") or 0)
            target_dupes = duplicate_row_count(cur, args.target_table, KEY_COLUMNS)

            if target_rows != args.expected_rows:
                errors.append(f"target row count mismatch expected={args.expected_rows} actual={target_rows}")
            if target_dupes != 0:
                errors.append(f"target duplicate key rows={target_dupes}")
            target_null_partition = int(
                fetch_scalar(
                    cur,
                    f"SELECT COUNT(*) FROM {args.target_table} WHERE SEASON IS NULL OR WEATHER_SAMPLE IS NULL",
                )
                or 0
            )
            if target_null_partition != 0:
                errors.append(f"target null partition-key rows={target_null_partition}")

            print(f"STAGE_PATH={stage_path}")
            print(f"STAGE_PARQUET_FILES={copy_file_count}")
            print(f"COPY_ROWS_LOADED={copy_rows_loaded}")
            print(f"LANDING_ROWS={landing_rows}")
            print(f"TARGET_ROWS={target_rows}")
            print(f"TARGET_DUPLICATE_KEY_ROWS={target_dupes}")
            print(f"TARGET_NULL_PARTITION_KEY_ROWS={target_null_partition}")

            if errors:
                print("PHASE2_INDOOR_LOAD_FAILED")
                for err in errors:
                    print(" -", err)
                return 1

            print("PHASE2_INDOOR_LOAD_PASSED")
            return 0
        finally:
            cur.close()
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
