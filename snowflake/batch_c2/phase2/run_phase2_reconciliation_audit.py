#!/usr/bin/env python3
"""Batch C2 reconciliation/audit for loaded serving tables."""

from __future__ import annotations

import argparse
import datetime as dt
import os
import sys
from typing import Dict, List, Sequence, Tuple


TABLE_CONFIGS: List[Dict[str, object]] = [
    {
        "label": "team",
        "table_fqn": "SERVING.SERV_TEAM_SEASON_WEATHER_V1",
        "expected_rows": 21858,
        "key_columns": ["SEASON", "TEAM", "TEMP_BIN", "WIND_BIN", "PRECIP_BIN", "ROOF_ENV_BIN", "WEATHER_SAMPLE"],
        "metric_cols_0_1": ["PASS_RATE", "COMPLETION_PCT"],
    },
    {
        "label": "play",
        "table_fqn": "SERVING.SERV_PLAY_CALLING_WEATHER_V1",
        "expected_rows": 16792,
        "key_columns": ["SEASON", "TEAM", "TEMP_BIN", "WIND_BIN", "PRECIP_BIN", "WEATHER_SAMPLE"],
        "metric_cols_0_1": ["PASS_RATE", "RUSH_RATE", "PASS_RATE_BASELINE", "RUSH_RATE_BASELINE"],
    },
    {
        "label": "indoor",
        "table_fqn": "SERVING.SERV_INDOOR_OUTDOOR_COMPARE_V1",
        "expected_rows": 145,
        "key_columns": ["SEASON", "ROOF_ENV_BIN", "WEATHER_SAMPLE"],
        "metric_cols_0_1": ["PASS_RATE", "COMPLETION_PCT"],
    },
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Batch C2 reconciliation and audit writer")
    parser.add_argument("--account", default=os.getenv("SNOWFLAKE_ACCOUNT", "ekbpfux-vob93769"))
    parser.add_argument("--account-fallback", default="hbb86970")
    parser.add_argument("--user", default=os.getenv("SNOWFLAKE_USER"))
    parser.add_argument("--password", default=os.getenv("SNOWFLAKE_PASSWORD"))
    parser.add_argument("--authenticator", default=os.getenv("SNOWFLAKE_AUTHENTICATOR"))

    parser.add_argument("--sf-role", default=os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"))
    parser.add_argument("--sf-warehouse", default=os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"))
    parser.add_argument("--sf-database", default=os.getenv("SNOWFLAKE_DATABASE", "NFL_WEATHER"))

    parser.add_argument("--expected-team-rows", type=int, default=21858)
    parser.add_argument("--expected-play-rows", type=int, default=16792)
    parser.add_argument("--expected-indoor-rows", type=int, default=145)
    return parser.parse_args()


def require_args(args: argparse.Namespace) -> List[str]:
    missing = []
    for key in ["user", "password", "sf_role", "sf_warehouse", "sf_database"]:
        if not getattr(args, key):
            missing.append(key)
    if not args.account and not args.account_fallback:
        missing.append("account")
    return missing


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


def metric_violation_count(cur, table_fqn: str, metric_cols: Sequence[str]) -> int:
    clauses = [f"({c} IS NOT NULL AND ({c} < 0 OR {c} > 1))" for c in metric_cols]
    where_sql = " OR ".join(clauses)
    return int(fetch_scalar(cur, f"SELECT COUNT(*) FROM {table_fqn} WHERE {where_sql}") or 0)


def write_audit_tables(cur) -> None:
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS AUDIT.BATCH_C2_RUN_AUDIT_V1 (
          RUN_ID VARCHAR,
          RUN_TS TIMESTAMP_NTZ,
          STATUS VARCHAR,
          ERROR_COUNT NUMBER(38,0),
          ACCOUNT VARCHAR,
          DATABASE_NAME VARCHAR,
          ROLE_NAME VARCHAR,
          WAREHOUSE_NAME VARCHAR,
          TEAM_EXPECTED_ROWS NUMBER(38,0),
          TEAM_ACTUAL_ROWS NUMBER(38,0),
          PLAY_EXPECTED_ROWS NUMBER(38,0),
          PLAY_ACTUAL_ROWS NUMBER(38,0),
          INDOOR_EXPECTED_ROWS NUMBER(38,0),
          INDOOR_ACTUAL_ROWS NUMBER(38,0),
          NOTES VARCHAR
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS AUDIT.BATCH_C2_TABLE_RECON_V1 (
          RUN_ID VARCHAR,
          RUN_TS TIMESTAMP_NTZ,
          TABLE_NAME VARCHAR,
          EXPECTED_ROWS NUMBER(38,0),
          ACTUAL_ROWS NUMBER(38,0),
          DUPLICATE_KEY_ROWS NUMBER(38,0),
          NULL_PARTITION_KEY_ROWS NUMBER(38,0),
          METRIC_RANGE_VIOLATIONS NUMBER(38,0),
          MIN_SEASON NUMBER(10,0),
          MAX_SEASON NUMBER(10,0),
          STATUS VARCHAR,
          DETAILS VARCHAR
        )
        """
    )


def main() -> int:
    args = parse_args()

    missing = require_args(args)
    if missing:
        print("ERROR: missing required args/env: " + ", ".join(missing), file=sys.stderr)
        return 2

    expected_by_label = {
        "team": args.expected_team_rows,
        "play": args.expected_play_rows,
        "indoor": args.expected_indoor_rows,
    }

    try:
        conn, used_account = connect_with_fallback(args)
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    print(f"CONNECTED_ACCOUNT={used_account}")

    run_ts = dt.datetime.now(dt.UTC)
    run_id = f"c2_recon_{run_ts.strftime('%Y%m%dT%H%M%SZ')}"

    try:
        cur = conn.cursor()
        try:
            cur.execute(f"USE ROLE {args.sf_role}")
            cur.execute(f"USE WAREHOUSE {args.sf_warehouse}")
            cur.execute(f"CREATE DATABASE IF NOT EXISTS {args.sf_database}")
            cur.execute(f"USE DATABASE {args.sf_database}")
            cur.execute("CREATE SCHEMA IF NOT EXISTS AUDIT")

            write_audit_tables(cur)

            table_rows: List[Tuple] = []
            total_errors = 0
            team_actual = None
            play_actual = None
            indoor_actual = None

            for cfg in TABLE_CONFIGS:
                label = str(cfg["label"])
                table_fqn = str(cfg["table_fqn"])
                expected_rows = int(expected_by_label[label])
                key_columns = list(cfg["key_columns"])
                metric_cols = list(cfg["metric_cols_0_1"])

                actual_rows = int(fetch_scalar(cur, f"SELECT COUNT(*) FROM {table_fqn}") or 0)
                dup_rows = duplicate_row_count(cur, table_fqn, key_columns)
                null_partition = int(
                    fetch_scalar(cur, f"SELECT COUNT(*) FROM {table_fqn} WHERE SEASON IS NULL OR WEATHER_SAMPLE IS NULL")
                    or 0
                )
                metric_bad = metric_violation_count(cur, table_fqn, metric_cols)

                cur.execute(f"SELECT MIN(SEASON), MAX(SEASON) FROM {table_fqn}")
                min_season, max_season = cur.fetchone()

                details = []
                if actual_rows != expected_rows:
                    details.append(f"row_count expected={expected_rows} actual={actual_rows}")
                if dup_rows != 0:
                    details.append(f"duplicate_key_rows={dup_rows}")
                if null_partition != 0:
                    details.append(f"null_partition_key_rows={null_partition}")
                if metric_bad != 0:
                    details.append(f"metric_range_violations={metric_bad}")

                status = "PASS" if not details else "FAIL"
                if status == "FAIL":
                    total_errors += 1

                table_rows.append(
                    (
                        run_id,
                        run_ts,
                        table_fqn,
                        expected_rows,
                        actual_rows,
                        dup_rows,
                        null_partition,
                        metric_bad,
                        min_season,
                        max_season,
                        status,
                        "; ".join(details) if details else "OK",
                    )
                )

                if label == "team":
                    team_actual = actual_rows
                elif label == "play":
                    play_actual = actual_rows
                elif label == "indoor":
                    indoor_actual = actual_rows

                print(
                    f"TABLE={table_fqn} expected={expected_rows} actual={actual_rows} "
                    f"dup={dup_rows} null_partition={null_partition} metric_bad={metric_bad} status={status}"
                )

            cur.executemany(
                """
                INSERT INTO AUDIT.BATCH_C2_TABLE_RECON_V1 (
                  RUN_ID, RUN_TS, TABLE_NAME, EXPECTED_ROWS, ACTUAL_ROWS,
                  DUPLICATE_KEY_ROWS, NULL_PARTITION_KEY_ROWS, METRIC_RANGE_VIOLATIONS,
                  MIN_SEASON, MAX_SEASON, STATUS, DETAILS
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                table_rows,
            )

            run_status = "PASS" if total_errors == 0 else "FAIL"
            notes = "All serving tables reconciled" if run_status == "PASS" else "See table-level failures"

            cur.execute(
                """
                INSERT INTO AUDIT.BATCH_C2_RUN_AUDIT_V1 (
                  RUN_ID, RUN_TS, STATUS, ERROR_COUNT,
                  ACCOUNT, DATABASE_NAME, ROLE_NAME, WAREHOUSE_NAME,
                  TEAM_EXPECTED_ROWS, TEAM_ACTUAL_ROWS,
                  PLAY_EXPECTED_ROWS, PLAY_ACTUAL_ROWS,
                  INDOOR_EXPECTED_ROWS, INDOOR_ACTUAL_ROWS,
                  NOTES
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    run_id,
                    run_ts,
                    run_status,
                    total_errors,
                    used_account,
                    args.sf_database,
                    args.sf_role,
                    args.sf_warehouse,
                    args.expected_team_rows,
                    team_actual,
                    args.expected_play_rows,
                    play_actual,
                    args.expected_indoor_rows,
                    indoor_actual,
                    notes,
                ),
            )

            print(f"RUN_ID={run_id}")
            print(f"RUN_STATUS={run_status}")
            print(f"ERROR_COUNT={total_errors}")

            if total_errors != 0:
                print("BATCH_C2_RECON_FAILED")
                return 1

            print("BATCH_C2_RECON_PASSED")
            return 0
        finally:
            cur.close()
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
