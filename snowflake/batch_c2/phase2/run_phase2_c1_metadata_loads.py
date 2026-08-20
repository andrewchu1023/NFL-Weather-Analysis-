#!/usr/bin/env python3
"""Batch C2: load C1 metadata/audit tables into Snowflake.

Loads from stage:
- run_audit_v1
- table_audit_v1
- dq_audit_v1
- data_dictionary_v1

Pattern: stage -> STG landing -> AUDIT target (swap)
"""

from __future__ import annotations

import argparse
import os
import sys
from typing import Dict, List, Sequence, Tuple


def table_config() -> List[Dict[str, object]]:
    return [
        {
            "label": "run_audit_v1",
            "stage_subpath": "run_audit_v1/",
            "landing_table": "STG.RUN_AUDIT_V1_LND",
            "target_table": "AUDIT.RUN_AUDIT_V1",
            "tmp_table": "AUDIT.RUN_AUDIT_V1__LOAD_TMP",
            "expected_rows": 1,
            "columns": [
                ("COMPLETED_AT_UTC", "STRING", "VARCHAR"),
                ("DQ_FAIL_COUNT", "NUMBER(38,0)", "NUMBER(38,0)"),
                ("SERVING_BUILD_RUN_TS", "STRING", "VARCHAR"),
                ("SERVING_INDOOR_ROWS", "NUMBER(38,0)", "NUMBER(38,0)"),
                ("SERVING_PLAYCALL_ROWS", "NUMBER(38,0)", "NUMBER(38,0)"),
                ("SERVING_RUN_ID", "STRING", "VARCHAR"),
                ("SERVING_TEAM_ROWS", "NUMBER(38,0)", "NUMBER(38,0)"),
                ("SERVING_VERSION", "STRING", "VARCHAR"),
                ("SOURCE_BATCH_A_BUILD_RUN_TS", "STRING", "VARCHAR"),
                ("SOURCE_BATCH_B_BUILD_RUN_TS", "STRING", "VARCHAR"),
                ("SOURCE_INDOOR_ROWS", "NUMBER(38,0)", "NUMBER(38,0)"),
                ("SOURCE_PLAYCALL_ROWS", "NUMBER(38,0)", "NUMBER(38,0)"),
                ("SOURCE_TEAM_ROWS", "NUMBER(38,0)", "NUMBER(38,0)"),
                ("STARTED_AT_UTC", "STRING", "VARCHAR"),
                ("STATUS", "STRING", "VARCHAR"),
            ],
            "key_columns": ["SERVING_RUN_ID"],
            "not_null_columns": ["SERVING_RUN_ID", "SERVING_BUILD_RUN_TS", "STATUS"],
        },
        {
            "label": "table_audit_v1",
            "stage_subpath": "table_audit_v1/",
            "landing_table": "STG.TABLE_AUDIT_V1_LND",
            "target_table": "AUDIT.TABLE_AUDIT_V1",
            "tmp_table": "AUDIT.TABLE_AUDIT_V1__LOAD_TMP",
            "expected_rows": 3,
            "columns": [
                ("DUPLICATE_GRAIN_ROWS", "NUMBER(38,0)", "NUMBER(38,0)"),
                ("MAX_SEASON", "NUMBER(38,0)", "NUMBER(38,0)"),
                ("MIN_SEASON", "NUMBER(38,0)", "NUMBER(38,0)"),
                ("N_WEATHER_SAMPLES", "NUMBER(38,0)", "NUMBER(38,0)"),
                ("SERVING_BUILD_RUN_TS", "STRING", "VARCHAR"),
                ("SERVING_ROW_COUNT", "NUMBER(38,0)", "NUMBER(38,0)"),
                ("SERVING_RUN_ID", "STRING", "VARCHAR"),
                ("SERVING_VERSION", "STRING", "VARCHAR"),
                ("SOURCE_BATCH_A_BUILD_RUN_TS", "STRING", "VARCHAR"),
                ("SOURCE_BATCH_B_BUILD_RUN_TS", "STRING", "VARCHAR"),
                ("SOURCE_ROW_COUNT", "NUMBER(38,0)", "NUMBER(38,0)"),
                ("SUM_N_PLAYS", "NUMBER(38,0)", "NUMBER(38,0)"),
                ("TABLE_NAME", "STRING", "VARCHAR"),
            ],
            "key_columns": ["SERVING_RUN_ID", "TABLE_NAME"],
            "not_null_columns": ["SERVING_RUN_ID", "TABLE_NAME", "SERVING_ROW_COUNT"],
        },
        {
            "label": "dq_audit_v1",
            "stage_subpath": "dq_audit_v1/",
            "landing_table": "STG.DQ_AUDIT_V1_LND",
            "target_table": "AUDIT.DQ_AUDIT_V1",
            "tmp_table": "AUDIT.DQ_AUDIT_V1__LOAD_TMP",
            "expected_rows": 27,
            "columns": [
                ("ACTUAL_VALUE", "STRING", "VARCHAR"),
                ("CHECK_NAME", "STRING", "VARCHAR"),
                ("EXPECTED_VALUE", "STRING", "VARCHAR"),
                ("SERVING_BUILD_RUN_TS", "STRING", "VARCHAR"),
                ("SERVING_RUN_ID", "STRING", "VARCHAR"),
                ("SERVING_VERSION", "STRING", "VARCHAR"),
                ("SEVERITY", "STRING", "VARCHAR"),
                ("SOURCE_BATCH_A_BUILD_RUN_TS", "STRING", "VARCHAR"),
                ("SOURCE_BATCH_B_BUILD_RUN_TS", "STRING", "VARCHAR"),
                ("STATUS", "STRING", "VARCHAR"),
                ("TABLE_NAME", "STRING", "VARCHAR"),
            ],
            "key_columns": ["SERVING_RUN_ID", "TABLE_NAME", "CHECK_NAME"],
            "not_null_columns": ["SERVING_RUN_ID", "TABLE_NAME", "CHECK_NAME", "STATUS"],
        },
        {
            "label": "data_dictionary_v1",
            "stage_subpath": "data_dictionary_v1/",
            "landing_table": "STG.DATA_DICTIONARY_V1_LND",
            "target_table": "AUDIT.DATA_DICTIONARY_V1",
            "tmp_table": "AUDIT.DATA_DICTIONARY_V1__LOAD_TMP",
            "expected_rows": 72,
            "columns": [
                ("COLUMN_DESCRIPTION", "STRING", "VARCHAR"),
                ("COLUMN_NAME", "STRING", "VARCHAR"),
                ("DATA_TYPE", "STRING", "VARCHAR"),
                ("IS_GRAIN_KEY", "NUMBER(38,0)", "NUMBER(38,0)"),
                ("ORDINAL_POSITION", "NUMBER(38,0)", "NUMBER(38,0)"),
                ("SERVING_BUILD_RUN_TS", "STRING", "VARCHAR"),
                ("SERVING_RUN_ID", "STRING", "VARCHAR"),
                ("SERVING_VERSION", "STRING", "VARCHAR"),
                ("TABLE_NAME", "STRING", "VARCHAR"),
            ],
            "key_columns": ["SERVING_RUN_ID", "TABLE_NAME", "COLUMN_NAME"],
            "not_null_columns": ["SERVING_RUN_ID", "TABLE_NAME", "COLUMN_NAME", "ORDINAL_POSITION"],
        },
    ]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Batch C2 load C1 metadata/audit tables")
    parser.add_argument("--account", default=os.getenv("SNOWFLAKE_ACCOUNT", "ekbpfux-vob93769"))
    parser.add_argument("--account-fallback", default="hbb86970")
    parser.add_argument("--user", default=os.getenv("SNOWFLAKE_USER"))
    parser.add_argument("--password", default=os.getenv("SNOWFLAKE_PASSWORD"))
    parser.add_argument("--authenticator", default=os.getenv("SNOWFLAKE_AUTHENTICATOR"))

    parser.add_argument("--sf-role", default=os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"))
    parser.add_argument("--sf-warehouse", default=os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"))
    parser.add_argument("--sf-database", default=os.getenv("SNOWFLAKE_DATABASE", "NFL_WEATHER"))

    parser.add_argument("--stage-fqn", default="STG.GCS_TABLEAU_SERVING_V1")
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


def split_schema_table(fqn: str) -> Tuple[str, str]:
    parts = [p.strip().upper() for p in fqn.split(".") if p.strip()]
    if len(parts) == 2:
        return parts[0], parts[1]
    if len(parts) == 3:
        return parts[1], parts[2]
    raise ValueError(f"Expected <schema>.<table> or <db>.<schema>.<table>, got: {fqn}")


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


def create_table_sql(table_fqn: str, cols: Sequence[Tuple[str, str, str]]) -> str:
    ddl_cols = ",\n  ".join([f"{c[0]} {c[2]}" for c in cols])
    return f"CREATE TABLE IF NOT EXISTS {table_fqn} (\n  {ddl_cols}\n)"


def copy_sql(landing_table: str, stage_path: str, cols: Sequence[Tuple[str, str, str]]) -> str:
    col_names = [c[0] for c in cols]
    select_exprs = [f"$1:{c[0].lower()}::{c[1]} AS {c[0]}" for c in cols]
    return f"""
        COPY INTO {landing_table}
          ({', '.join(col_names)})
        FROM (
          SELECT
            {', '.join(select_exprs)}
          FROM {stage_path}
            (FILE_FORMAT => 'STG.FF_PARQUET_V1', PATTERN => '.*\\\\.parquet')
        )
        ON_ERROR = 'ABORT_STATEMENT'
    """


def main() -> int:
    args = parse_args()
    missing = require_args(args)
    if missing:
        print("ERROR: missing required args/env: " + ", ".join(missing), file=sys.stderr)
        return 2

    cfgs = table_config()
    if args.dry_run:
        print("DRY_RUN_OK")
        for cfg in cfgs:
            print(f"{cfg['label']} stage=@{args.stage_fqn}/{cfg['stage_subpath']}")
        return 0

    try:
        conn, used_account = connect_with_fallback(args)
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    print(f"CONNECTED_ACCOUNT={used_account}")

    errors: List[str] = []

    try:
        cur = conn.cursor()
        try:
            cur.execute(f"USE ROLE {args.sf_role}")
            cur.execute(f"USE WAREHOUSE {args.sf_warehouse}")
            cur.execute(f"CREATE DATABASE IF NOT EXISTS {args.sf_database}")
            cur.execute(f"USE DATABASE {args.sf_database}")
            cur.execute("CREATE SCHEMA IF NOT EXISTS STG")
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

            cur.execute("SHOW STAGES LIKE 'GCS_TABLEAU_SERVING_V1' IN SCHEMA STG")
            if not cur.fetchall():
                errors.append("missing stage: STG.GCS_TABLEAU_SERVING_V1")

            for cfg in cfgs:
                label = str(cfg["label"])
                stage_subpath = str(cfg["stage_subpath"]).strip("/")
                stage_path = f"@{args.stage_fqn}/{stage_subpath}/"
                landing_table = str(cfg["landing_table"])
                target_table = str(cfg["target_table"])
                tmp_table = str(cfg["tmp_table"])
                exp_rows = int(cfg["expected_rows"])
                cols = list(cfg["columns"])
                key_cols = list(cfg["key_columns"])
                not_null_cols = list(cfg["not_null_columns"])

                cur.execute(f"LIST {stage_path} PATTERN='.*\\\\.parquet'")
                files = cur.fetchall()
                if len(files) == 0:
                    errors.append(f"{label}: no parquet files at {stage_path}")
                    continue

                cur.execute(create_table_sql(landing_table, cols))
                cur.execute(create_table_sql(target_table, cols))

                cur.execute(f"TRUNCATE TABLE {landing_table}")
                cur.execute(copy_sql(landing_table, stage_path, cols))
                copy_res = cur.fetchall()
                if not copy_res:
                    errors.append(f"{label}: COPY INTO returned no result rows")

                col_names = [c[0] for c in cols]
                landing_rows = int(fetch_scalar(cur, f"SELECT COUNT(*) FROM {landing_table}") or 0)
                if landing_rows != exp_rows:
                    errors.append(f"{label}: landing row count mismatch expected={exp_rows} actual={landing_rows}")

                dup_rows = duplicate_row_count(cur, landing_table, key_cols)
                if dup_rows != 0:
                    errors.append(f"{label}: landing duplicate key rows={dup_rows}")

                for nn_col in not_null_cols:
                    nn_bad = int(fetch_scalar(cur, f"SELECT COUNT(*) FROM {landing_table} WHERE {nn_col} IS NULL") or 0)
                    if nn_bad != 0:
                        errors.append(f"{label}: nulls in {nn_col} rows={nn_bad}")

                landing_schema = fetch_schema(cur, args.sf_database, landing_table)
                target_schema = fetch_schema(cur, args.sf_database, target_table)
                if landing_schema != target_schema:
                    errors.append(f"{label}: landing/target schema type mismatch")
                if [r[0] for r in landing_schema] != col_names:
                    errors.append(f"{label}: landing column order mismatch")

                if not errors:
                    cur.execute(f"CREATE OR REPLACE TABLE {tmp_table} LIKE {target_table}")
                    cur.execute(
                        f"INSERT INTO {tmp_table} ({', '.join(col_names)}) "
                        f"SELECT {', '.join(col_names)} FROM {landing_table}"
                    )
                    tmp_rows = int(fetch_scalar(cur, f"SELECT COUNT(*) FROM {tmp_table}") or 0)
                    if tmp_rows != exp_rows:
                        errors.append(f"{label}: tmp row count mismatch expected={exp_rows} actual={tmp_rows}")
                    else:
                        cur.execute(f"ALTER TABLE {target_table} SWAP WITH {tmp_table}")
                        cur.execute(f"DROP TABLE IF EXISTS {tmp_table}")

                target_rows = int(fetch_scalar(cur, f"SELECT COUNT(*) FROM {target_table}") or 0)
                target_dup = duplicate_row_count(cur, target_table, key_cols)
                if target_rows != exp_rows:
                    errors.append(f"{label}: target row count mismatch expected={exp_rows} actual={target_rows}")
                if target_dup != 0:
                    errors.append(f"{label}: target duplicate key rows={target_dup}")

                print(
                    f"TABLE={label} files={len(files)} expected={exp_rows} "
                    f"landing={landing_rows} target={target_rows} dup={target_dup}"
                )

            if errors:
                print("PHASE2_C1_METADATA_LOAD_FAILED")
                for e in errors:
                    print(" -", e)
                return 1

            print("PHASE2_C1_METADATA_LOAD_PASSED")
            return 0
        finally:
            cur.close()
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
