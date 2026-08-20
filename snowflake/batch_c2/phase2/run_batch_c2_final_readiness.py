#!/usr/bin/env python3
"""Final Batch C2 readiness validation checklist."""

from __future__ import annotations

import argparse
import datetime as dt
import os
import sys
from typing import Dict, List, Sequence, Tuple


SERVING_CONFIGS: List[Dict[str, object]] = [
    {
        "table": "SERVING.SERV_TEAM_SEASON_WEATHER_V1",
        "expected_rows": 21858,
        "key_columns": ["SEASON", "TEAM", "TEMP_BIN", "WIND_BIN", "PRECIP_BIN", "ROOF_ENV_BIN", "WEATHER_SAMPLE"],
    },
    {
        "table": "SERVING.SERV_PLAY_CALLING_WEATHER_V1",
        "expected_rows": 16792,
        "key_columns": ["SEASON", "TEAM", "TEMP_BIN", "WIND_BIN", "PRECIP_BIN", "WEATHER_SAMPLE"],
    },
    {
        "table": "SERVING.SERV_INDOOR_OUTDOOR_COMPARE_V1",
        "expected_rows": 145,
        "key_columns": ["SEASON", "ROOF_ENV_BIN", "WEATHER_SAMPLE"],
    },
]

META_CONFIGS: List[Tuple[str, int, Sequence[str]]] = [
    ("AUDIT.RUN_AUDIT_V1", 1, ["SERVING_RUN_ID"]),
    ("AUDIT.TABLE_AUDIT_V1", 3, ["SERVING_RUN_ID", "TABLE_NAME"]),
    ("AUDIT.DQ_AUDIT_V1", 27, ["SERVING_RUN_ID", "TABLE_NAME", "CHECK_NAME"]),
    ("AUDIT.DATA_DICTIONARY_V1", 72, ["SERVING_RUN_ID", "TABLE_NAME", "COLUMN_NAME"]),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Batch C2 final readiness checklist")
    parser.add_argument("--account", default=os.getenv("SNOWFLAKE_ACCOUNT", "ekbpfux-vob93769"))
    parser.add_argument("--account-fallback", default="hbb86970")
    parser.add_argument("--user", default=os.getenv("SNOWFLAKE_USER"))
    parser.add_argument("--password", default=os.getenv("SNOWFLAKE_PASSWORD"))
    parser.add_argument("--authenticator", default=os.getenv("SNOWFLAKE_AUTHENTICATOR"))

    parser.add_argument("--sf-role", default=os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"))
    parser.add_argument("--sf-warehouse", default=os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"))
    parser.add_argument("--sf-database", default=os.getenv("SNOWFLAKE_DATABASE", "NFL_WEATHER"))

    parser.add_argument("--summary-path", default="snowflake/batch_c2/FINAL_READINESS_SUMMARY.md")
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


def main() -> int:
    args = parse_args()
    missing = require_args(args)
    if missing:
        print("ERROR: missing required args/env: " + ", ".join(missing), file=sys.stderr)
        return 2

    try:
        conn, used_account = connect_with_fallback(args)
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    run_ts = dt.datetime.now(dt.UTC).strftime("%Y-%m-%d %H:%M:%SZ")
    blockers: List[str] = []
    details: List[str] = []

    try:
        cur = conn.cursor()
        try:
            cur.execute(f"USE ROLE {args.sf_role}")
            cur.execute(f"USE WAREHOUSE {args.sf_warehouse}")
            cur.execute(f"USE DATABASE {args.sf_database}")

            # 1) serving complete + reconciled
            serving_ok = True
            for cfg in SERVING_CONFIGS:
                table = str(cfg["table"])
                expected_rows = int(cfg["expected_rows"])
                key_cols = list(cfg["key_columns"])

                rows = int(fetch_scalar(cur, f"SELECT COUNT(*) FROM {table}") or 0)
                dup = duplicate_row_count(cur, table, key_cols)
                null_partition = int(fetch_scalar(cur, f"SELECT COUNT(*) FROM {table} WHERE SEASON IS NULL OR WEATHER_SAMPLE IS NULL") or 0)
                details.append(f"{table}: rows={rows} expected={expected_rows} dup={dup} null_partition={null_partition}")

                if rows != expected_rows or dup != 0 or null_partition != 0:
                    serving_ok = False

            cur.execute(
                """
                SELECT RUN_ID, STATUS, ERROR_COUNT
                FROM AUDIT.BATCH_C2_RUN_AUDIT_V1
                ORDER BY RUN_TS DESC
                LIMIT 1
                """
            )
            latest_run = cur.fetchone()
            if not latest_run:
                serving_ok = False
                details.append("AUDIT.BATCH_C2_RUN_AUDIT_V1: no runs")
            else:
                run_id, status, error_count = latest_run
                details.append(f"latest_recon_run={run_id} status={status} error_count={error_count}")
                if status != "PASS" or int(error_count) != 0:
                    serving_ok = False
                recon_pass_count = int(fetch_scalar(cur, f"SELECT COUNT(*) FROM AUDIT.BATCH_C2_TABLE_RECON_V1 WHERE RUN_ID='{run_id}' AND STATUS='PASS'") or 0)
                recon_total_count = int(fetch_scalar(cur, f"SELECT COUNT(*) FROM AUDIT.BATCH_C2_TABLE_RECON_V1 WHERE RUN_ID='{run_id}'") or 0)
                details.append(f"latest_recon_tables pass={recon_pass_count} total={recon_total_count}")
                if recon_pass_count != 3 or recon_total_count != 3:
                    serving_ok = False

            # 2) metadata/audit tables loaded
            meta_ok = True
            for table, expected_rows, key_cols in META_CONFIGS:
                rows = int(fetch_scalar(cur, f"SELECT COUNT(*) FROM {table}") or 0)
                dup = duplicate_row_count(cur, table, key_cols)
                details.append(f"{table}: rows={rows} expected={expected_rows} dup={dup}")
                if rows != expected_rows or dup != 0:
                    meta_ok = False

            # 3) SERVING is only Tableau-facing contract
            contract_ok = True
            serving_contract_count = int(
                fetch_scalar(
                    cur,
                    """
                    SELECT COUNT(*)
                    FROM INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_SCHEMA='SERVING'
                      AND TABLE_NAME IN (
                        'SERV_TEAM_SEASON_WEATHER_V1',
                        'SERV_PLAY_CALLING_WEATHER_V1',
                        'SERV_INDOOR_OUTDOOR_COMPARE_V1'
                      )
                    """,
                )
                or 0
            )
            if serving_contract_count != 3:
                contract_ok = False
            details.append(f"SERVING contract table count={serving_contract_count}")

            non_serving_contracts = int(
                fetch_scalar(
                    cur,
                    """
                    SELECT COUNT(*)
                    FROM INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_SCHEMA <> 'SERVING'
                      AND TABLE_TYPE='BASE TABLE'
                      AND REGEXP_LIKE(TABLE_NAME, '^SERV_')
                      AND NOT REGEXP_LIKE(TABLE_NAME, '_LND$')
                      AND NOT REGEXP_LIKE(TABLE_NAME, '_TMP$')
                      AND NOT REGEXP_LIKE(TABLE_NAME, '__LOAD_TMP$')
                    """,
                )
                or 0
            )
            details.append(f"non_SERVING contract-like tables={non_serving_contracts}")
            if non_serving_contracts != 0:
                contract_ok = False

            # 4) no known completeness blockers for Tableau
            tableau_ready = serving_ok and meta_ok and contract_ok

            if not serving_ok:
                blockers.append("Serving completeness/reconciliation check failed")
            if not meta_ok:
                blockers.append("Metadata/audit table load check failed")
            if not contract_ok:
                blockers.append("SERVING-only contract boundary check failed")

            checklist = [
                ("1. All 3 serving tables are complete and reconciled", serving_ok),
                ("2. All required metadata/audit tables are loaded", meta_ok),
                ("3. Snowflake SERVING is the only Tableau-facing contract", contract_ok),
                ("4. Tableau can start safely with no known completeness blockers", tableau_ready),
            ]

            print(f"CONNECTED_ACCOUNT={used_account}")
            print(f"VALIDATED_AT_UTC={run_ts}")
            for label, ok in checklist:
                print(f"[{ 'PASS' if ok else 'FAIL' }] {label}")

            if blockers:
                print("BLOCKERS:")
                for b in blockers:
                    print(f" - {b}")
            else:
                print("BLOCKERS: none")

            summary_lines = [
                "# Batch C2 Final Readiness Summary",
                "",
                f"- validated_at_utc: `{run_ts}`",
                f"- snowflake_account: `{used_account}`",
                f"- database: `{args.sf_database}`",
                "",
                "## Checklist",
            ]
            for label, ok in checklist:
                summary_lines.append(f"- {'PASS' if ok else 'FAIL'}: {label}")

            summary_lines.append("")
            summary_lines.append("## Detail")
            for line in details:
                summary_lines.append(f"- {line}")

            summary_lines.append("")
            summary_lines.append("## Known Blockers")
            if blockers:
                for b in blockers:
                    summary_lines.append(f"- {b}")
            else:
                summary_lines.append("- None")

            with open(args.summary_path, "w", encoding="utf-8") as f:
                f.write("\n".join(summary_lines) + "\n")

            print(f"SUMMARY_WRITTEN={args.summary_path}")

            return 0 if tableau_ready else 1
        finally:
            cur.close()
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
