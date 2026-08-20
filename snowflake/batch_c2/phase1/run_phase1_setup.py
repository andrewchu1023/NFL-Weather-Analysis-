#!/usr/bin/env python3
"""Batch C2 Phase 1 Snowflake setup runner.

Creates and validates:
- Schemas: STG, SERVING, AUDIT
- File format: STG.FF_PARQUET_V1
- Stage: STG.GCS_TABLEAU_SERVING_V1
- Empty target table: SERVING.SERV_TEAM_SEASON_WEATHER_V1
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from pathlib import Path
from typing import Dict, List, Tuple


DEFAULT_GCS_SERVING_URL = "gcs://msba405-nfl-weather-raw/gold/tableau_serving/v1/"


def split_sql_statements(sql_text: str) -> List[str]:
    """Split SQL text into statements by semicolon, respecting single quotes."""
    statements: List[str] = []
    buf: List[str] = []
    in_single_quote = False
    i = 0
    while i < len(sql_text):
        ch = sql_text[i]
        if ch == "'":
            if in_single_quote and i + 1 < len(sql_text) and sql_text[i + 1] == "'":
                buf.append("''")
                i += 2
                continue
            in_single_quote = not in_single_quote
            buf.append(ch)
            i += 1
            continue
        if ch == ";" and not in_single_quote:
            stmt = "".join(buf).strip()
            if stmt:
                statements.append(stmt)
            buf = []
            i += 1
            continue
        buf.append(ch)
        i += 1

    tail = "".join(buf).strip()
    if tail:
        statements.append(tail)

    # Drop full-line comments
    cleaned: List[str] = []
    for stmt in statements:
        lines = [ln for ln in stmt.splitlines() if not ln.strip().startswith("--")]
        candidate = "\n".join(lines).strip()
        if candidate:
            cleaned.append(candidate)
    return cleaned


def render_template(template_text: str, values: Dict[str, str]) -> str:
    rendered = template_text
    for key, value in values.items():
        rendered = rendered.replace("{{" + key + "}}", value)
    unreplaced = set(re.findall(r"\{\{([A-Z0-9_]+)\}\}", rendered))
    if unreplaced:
        raise ValueError(f"Unresolved placeholders in SQL template: {sorted(unreplaced)}")
    return rendered


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Batch C2 Phase 1 Snowflake setup")
    parser.add_argument("--sql-path", default=str(Path(__file__).with_name("phase1_setup.sql")))

    parser.add_argument("--account", default=os.getenv("SNOWFLAKE_ACCOUNT"))
    parser.add_argument("--user", default=os.getenv("SNOWFLAKE_USER"))
    parser.add_argument("--password", default=os.getenv("SNOWFLAKE_PASSWORD"))
    parser.add_argument("--authenticator", default=os.getenv("SNOWFLAKE_AUTHENTICATOR"))

    parser.add_argument("--sf-role", default=os.getenv("SNOWFLAKE_ROLE", "SYSADMIN"))
    parser.add_argument("--sf-warehouse", default=os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"))
    parser.add_argument("--sf-database", default=os.getenv("SNOWFLAKE_DATABASE", "NFL_WEATHER"))
    parser.add_argument("--sf-storage-integration", default=os.getenv("SNOWFLAKE_STORAGE_INTEGRATION"))

    parser.add_argument("--gcs-serving-url", default=os.getenv("GCS_SERVING_URL", DEFAULT_GCS_SERVING_URL))

    parser.add_argument("--dry-run", action="store_true", help="Render SQL and validate locally; do not connect")
    parser.add_argument("--print-sql", action="store_true")
    return parser.parse_args()


def require_nonempty(args: argparse.Namespace, keys: List[str]) -> List[str]:
    missing = []
    for key in keys:
        if not getattr(args, key):
            missing.append(key)
    return missing


def validate_phase1(cursor, sf_database: str, expected_stage_url: str, expected_storage_integration: str) -> Tuple[bool, List[str]]:
    errors: List[str] = []

    cursor.execute(
        f"""
        SELECT COUNT(*)
        FROM {sf_database}.INFORMATION_SCHEMA.SCHEMATA
        WHERE SCHEMA_NAME IN ('STG', 'SERVING', 'AUDIT')
        """
    )
    schema_count = cursor.fetchone()[0]
    if schema_count != 3:
        errors.append(f"schema_count expected=3 actual={schema_count}")

    cursor.execute(
        f"""
        SELECT COUNT(*)
        FROM {sf_database}.INFORMATION_SCHEMA.FILE_FORMATS
        WHERE FILE_FORMAT_SCHEMA = 'STG'
          AND FILE_FORMAT_NAME = 'FF_PARQUET_V1'
        """
    )
    ff_count = cursor.fetchone()[0]
    if ff_count != 1:
        errors.append(f"file_format_count expected=1 actual={ff_count}")

    cursor.execute(
        f"""
        SELECT STAGE_URL, STORAGE_INTEGRATION
        FROM {sf_database}.INFORMATION_SCHEMA.STAGES
        WHERE STAGE_SCHEMA = 'STG'
          AND STAGE_NAME = 'GCS_TABLEAU_SERVING_V1'
        """
    )
    stage_row = cursor.fetchone()
    if not stage_row:
        errors.append("stage missing: STG.GCS_TABLEAU_SERVING_V1")
    else:
        stage_url = (stage_row[0] or "").rstrip("/")
        want_url = expected_stage_url.rstrip("/")
        if stage_url != want_url:
            errors.append(f"stage_url mismatch expected={want_url} actual={stage_url}")

        integration_name = (stage_row[1] or "").upper()
        want_integration = expected_storage_integration.upper()
        if integration_name != want_integration:
            errors.append(
                "storage_integration mismatch "
                f"expected={want_integration} actual={integration_name}"
            )

    cursor.execute(
        f"""
        SELECT COUNT(*)
        FROM {sf_database}.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = 'SERVING'
          AND TABLE_NAME = 'SERV_TEAM_SEASON_WEATHER_V1'
        """
    )
    table_count = cursor.fetchone()[0]
    if table_count != 1:
        errors.append(f"target_table_count expected=1 actual={table_count}")

    cursor.execute(
        f"SELECT COUNT(*) FROM {sf_database}.SERVING.SERV_TEAM_SEASON_WEATHER_V1"
    )
    row_count = cursor.fetchone()[0]
    if row_count != 0:
        errors.append(f"target_table_row_count expected=0 actual={row_count}")

    cursor.execute(
        f"""
        SELECT COLUMN_NAME
        FROM {sf_database}.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = 'SERVING'
          AND TABLE_NAME = 'SERV_TEAM_SEASON_WEATHER_V1'
        ORDER BY ORDINAL_POSITION
        """
    )
    got_cols = [r[0] for r in cursor.fetchall()]
    exp_cols = [
        "SEASON", "TEAM", "TEMP_BIN", "WIND_BIN", "PRECIP_BIN", "ROOF_ENV_BIN", "WEATHER_SAMPLE",
        "N_PLAYS", "N_PASS_ATTEMPTS", "N_RUSH_ATTEMPTS", "N_COMPLETIONS", "N_EPA_PLAYS", "N_GAMES",
        "N_GAMES_POINTS", "N_TEAM_GAMES_POINTS", "PASS_RATE", "COMPLETION_PCT", "EPA_PER_PLAY",
        "POINTS_PER_GAME", "BATCH_A_BUILD_RUN_TS", "BATCH_B_BUILD_RUN_TS", "SOURCE_BATCH_A_BUILD_RUN_TS",
        "SOURCE_BATCH_B_BUILD_RUN_TS", "SERVING_BUILD_RUN_TS", "SERVING_VERSION", "SERVING_RUN_ID",
    ]
    if got_cols != exp_cols:
        errors.append(
            "target_table_columns mismatch "
            f"expected={exp_cols} actual={got_cols}"
        )

    return (len(errors) == 0, errors)


def main() -> int:
    args = parse_args()

    if not args.sf_storage_integration:
        print("ERROR: missing --sf-storage-integration (or SNOWFLAKE_STORAGE_INTEGRATION)", file=sys.stderr)
        return 2

    sql_path = Path(args.sql_path)
    if not sql_path.exists():
        print(f"ERROR: SQL file not found: {sql_path}", file=sys.stderr)
        return 2

    template = sql_path.read_text(encoding="utf-8")
    rendered_sql = render_template(
        template,
        {
            "SF_ROLE": args.sf_role,
            "SF_WAREHOUSE": args.sf_warehouse,
            "SF_DATABASE": args.sf_database,
            "SF_STORAGE_INTEGRATION": args.sf_storage_integration,
            "GCS_SERVING_URL": args.gcs_serving_url,
        },
    )

    statements = split_sql_statements(rendered_sql)
    if not statements:
        print("ERROR: no SQL statements parsed", file=sys.stderr)
        return 2

    if args.print_sql or args.dry_run:
        print("----- Rendered SQL -----")
        print(rendered_sql)
        print("----- End SQL -----")

    if args.dry_run:
        print(f"DRY_RUN_OK statements={len(statements)} sql_path={sql_path}")
        return 0

    missing_conn = require_nonempty(args, ["account", "user", "password"])
    if missing_conn:
        print(
            "ERROR: missing Snowflake connection args: " + ", ".join(missing_conn) +
            " (or SNOWFLAKE_ACCOUNT/SNOWFLAKE_USER/SNOWFLAKE_PASSWORD)",
            file=sys.stderr,
        )
        return 2

    try:
        import snowflake.connector  # type: ignore
    except Exception as exc:  # pragma: no cover
        print(
            "ERROR: snowflake-connector-python is not installed. "
            "Install with: pip install snowflake-connector-python",
            file=sys.stderr,
        )
        print(f"DETAIL: {exc}", file=sys.stderr)
        return 2

    connect_kwargs = {
        "account": args.account,
        "user": args.user,
        "password": args.password,
        "autocommit": True,
    }
    if args.authenticator:
        connect_kwargs["authenticator"] = args.authenticator

    conn = snowflake.connector.connect(**connect_kwargs)
    try:
        cur = conn.cursor()
        try:
            for i, stmt in enumerate(statements, start=1):
                cur.execute(stmt)
                print(f"EXECUTED statement_{i}")

            ok, errors = validate_phase1(
                cur,
                sf_database=args.sf_database,
                expected_stage_url=args.gcs_serving_url,
                expected_storage_integration=args.sf_storage_integration,
            )
            if not ok:
                print("PHASE1_VALIDATION_FAILED")
                for err in errors:
                    print(" -", err)
                return 1

            print("PHASE1_VALIDATION_PASSED")
            print(f"database={args.sf_database}")
            print("schemas=STG,SERVING,AUDIT")
            print("file_format=STG.FF_PARQUET_V1")
            print("stage=STG.GCS_TABLEAU_SERVING_V1")
            print("target_table=SERVING.SERV_TEAM_SEASON_WEATHER_V1")
            return 0
        finally:
            cur.close()
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
