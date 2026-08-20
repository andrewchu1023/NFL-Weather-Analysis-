import json
import os
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


BUCKET = "msba405-nfl-weather-raw"

TEAM_SRC = f"gs://{BUCKET}/gold/mart_team_season_weather_v1/"
PLAY_SRC = f"gs://{BUCKET}/gold/mart_play_calling_weather_v1/"
INDOOR_SRC = f"gs://{BUCKET}/gold/mart_indoor_outdoor_compare_v1/"

BASE_OUT = f"gs://{BUCKET}/gold/tableau_serving/v1/"
TEAM_OUT = BASE_OUT + "serv_team_season_weather_v1/"
PLAY_OUT = BASE_OUT + "serv_play_calling_weather_v1/"
INDOOR_OUT = BASE_OUT + "serv_indoor_outdoor_compare_v1/"
RUN_AUDIT_OUT = BASE_OUT + "run_audit_v1/"
TABLE_AUDIT_OUT = BASE_OUT + "table_audit_v1/"
DQ_AUDIT_OUT = BASE_OUT + "dq_audit_v1/"
DATA_DICTIONARY_OUT = BASE_OUT + "data_dictionary_v1/"

TEAM_GRAIN = ["season", "team", "temp_bin", "wind_bin", "precip_bin", "roof_env_bin", "weather_sample"]
PLAY_GRAIN = ["season", "team", "temp_bin", "wind_bin", "precip_bin", "weather_sample"]
INDOOR_GRAIN = ["season", "roof_env_bin", "weather_sample"]

REQUIRED_META_COLS = [
    "source_batch_a_build_run_ts",
    "source_batch_b_build_run_ts",
    "serving_build_run_ts",
    "serving_version",
    "serving_run_id",
]


def detect_google_credentials():
    explicit = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if explicit:
        path = explicit
    else:
        adc_fallback = os.environ.get(
            "GOOGLE_APPLICATION_DEFAULT_CREDENTIALS",
            str(Path.home() / ".config/gcloud/application_default_credentials.json"),
        )
        path = adc_fallback if os.path.exists(adc_fallback) else None
        if path:
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = path

    if not path:
        return None, None

    cred_type = None
    try:
        with open(path, "r", encoding="utf-8") as f:
            cred_type = json.load(f).get("type")
    except Exception:
        cred_type = None

    return path, cred_type


def build_spark(app_name: str) -> SparkSession:
    builder = SparkSession.builder.appName(app_name)

    gcs_connector = os.environ.get(
        "SPARK_GCS_CONNECTOR",
        "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.28",
    )

    builder = (
        builder.config("spark.jars.packages", gcs_connector)
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")
    )

    creds_path, creds_type = detect_google_credentials()
    if creds_path:
        builder = builder.config("spark.executorEnv.GOOGLE_APPLICATION_CREDENTIALS", creds_path)
    if creds_path and creds_type == "service_account":
        builder = (
            builder.config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
            .config("spark.hadoop.fs.gs.auth.service.account.enable", "true")
            .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", creds_path)
            .config("spark.hadoop.fs.gs.auth.service.account.json.keyfile", creds_path)
        )

    return builder.getOrCreate()


def duplicate_grain_count(df: DataFrame, grain_cols):
    return df.groupBy(*grain_cols).count().filter(F.col("count") > 1).count()


def range_violation_count(df: DataFrame, col_name: str, low: float, high: float):
    return (
        df.filter(
            F.col(col_name).isNotNull()
            & ((F.col(col_name) < F.lit(low)) | (F.col(col_name) > F.lit(high)))
        ).count()
    )


def path_success_exists(spark: SparkSession, path: str) -> bool:
    jvm = spark._jvm
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    marker = jvm.org.apache.hadoop.fs.Path(path.rstrip("/") + "/_SUCCESS")
    fs = marker.getFileSystem(conf)
    return fs.exists(marker)


def require_cols(df: DataFrame, cols, label: str, errors):
    missing = [c for c in cols if c not in df.columns]
    if missing:
        errors.append(f"{label} missing columns: {missing}")


def main():
    spark = build_spark("validate_gold_batch_c1_tableau_serving_v1")
    spark.sparkContext.setLogLevel("WARN")

    errors = []

    team_src = spark.read.parquet(TEAM_SRC)
    play_src = spark.read.parquet(PLAY_SRC)
    indoor_src = spark.read.parquet(INDOOR_SRC)

    team = spark.read.parquet(TEAM_OUT)
    play = spark.read.parquet(PLAY_OUT)
    indoor = spark.read.parquet(INDOOR_OUT)

    run_audit = spark.read.parquet(RUN_AUDIT_OUT)
    table_audit = spark.read.parquet(TABLE_AUDIT_OUT)
    dq_audit = spark.read.parquet(DQ_AUDIT_OUT)
    data_dict = spark.read.parquet(DATA_DICTIONARY_OUT)

    for label, df in [
        ("serv_team", team),
        ("serv_play", play),
        ("serv_indoor", indoor),
    ]:
        for c in REQUIRED_META_COLS:
            require_cols(df, [c], label, errors)

    # Determine latest successful run
    latest_run = (
        run_audit.filter(F.col("status") == "SUCCESS")
        .orderBy(F.col("serving_build_run_ts").desc())
        .limit(1)
        .collect()
    )
    if not latest_run:
        errors.append("run_audit_v1 has no SUCCESS runs")
        latest_run_id = None
        latest_run_ts = None
    else:
        latest_run_id = latest_run[0]["serving_run_id"]
        latest_run_ts = latest_run[0]["serving_build_run_ts"]

    # Row counts should match source marts.
    team_src_cnt = team_src.count()
    play_src_cnt = play_src.count()
    indoor_src_cnt = indoor_src.count()

    team_cnt = team.count()
    play_cnt = play.count()
    indoor_cnt = indoor.count()

    if team_cnt != team_src_cnt:
        errors.append(f"team row count mismatch: source={team_src_cnt}, serving={team_cnt}")
    if play_cnt != play_src_cnt:
        errors.append(f"play row count mismatch: source={play_src_cnt}, serving={play_cnt}")
    if indoor_cnt != indoor_src_cnt:
        errors.append(f"indoor row count mismatch: source={indoor_src_cnt}, serving={indoor_cnt}")

    # Duplicates by grain should be zero.
    team_dup = duplicate_grain_count(team, TEAM_GRAIN)
    play_dup = duplicate_grain_count(play, PLAY_GRAIN)
    indoor_dup = duplicate_grain_count(indoor, INDOOR_GRAIN)

    if team_dup > 0:
        errors.append(f"team duplicate grain rows: {team_dup}")
    if play_dup > 0:
        errors.append(f"play duplicate grain rows: {play_dup}")
    if indoor_dup > 0:
        errors.append(f"indoor duplicate grain rows: {indoor_dup}")

    # Metric range checks.
    checks = [
        ("team", team, "pass_rate"),
        ("team", team, "completion_pct"),
        ("play", play, "pass_rate"),
        ("play", play, "rush_rate"),
        ("indoor", indoor, "pass_rate"),
        ("indoor", indoor, "completion_pct"),
    ]
    for label, df, col in checks:
        bad = range_violation_count(df, col, 0.0, 1.0)
        if bad > 0:
            errors.append(f"{label} {col} out-of-range rows: {bad}")

    # Metadata consistency checks.
    if latest_run_ts is not None:
        for label, df in [("team", team), ("play", play), ("indoor", indoor)]:
            run_ts_distinct = [r[0] for r in df.select("serving_build_run_ts").distinct().collect()]
            if run_ts_distinct != [latest_run_ts]:
                errors.append(f"{label} serving_build_run_ts mismatch: {run_ts_distinct}, expected [{latest_run_ts}]")

            version_distinct = [r[0] for r in df.select("serving_version").distinct().collect()]
            if version_distinct != ["v1"]:
                errors.append(f"{label} serving_version mismatch: {version_distinct}")

            run_id_distinct = [r[0] for r in df.select("serving_run_id").distinct().collect()]
            if run_id_distinct != [latest_run_id]:
                errors.append(f"{label} serving_run_id mismatch: {run_id_distinct}, expected [{latest_run_id}]")

    # _SUCCESS checks for all outputs.
    for p in [TEAM_OUT, PLAY_OUT, INDOOR_OUT, RUN_AUDIT_OUT, TABLE_AUDIT_OUT, DQ_AUDIT_OUT, DATA_DICTIONARY_OUT]:
        if not path_success_exists(spark, p):
            errors.append(f"missing _SUCCESS marker: {p}")

    # Audit table checks.
    if latest_run_id is not None:
        table_audit_rows = table_audit.filter(F.col("serving_run_id") == latest_run_id).count()
        if table_audit_rows < 3:
            errors.append(f"table_audit rows for run {latest_run_id} < 3: {table_audit_rows}")

        dq_run = dq_audit.filter(F.col("serving_run_id") == latest_run_id)
        dq_run_cnt = dq_run.count()
        dq_fail_cnt = dq_run.filter(F.col("status") == "FAIL").count()
        if dq_run_cnt == 0:
            errors.append(f"dq_audit has no rows for run {latest_run_id}")
        if dq_fail_cnt > 0:
            errors.append(f"dq_audit has FAIL rows for run {latest_run_id}: {dq_fail_cnt}")

        dd_tables = {
            r[0]
            for r in data_dict.filter(F.col("serving_run_id") == latest_run_id)
            .select("table_name")
            .distinct()
            .collect()
        }
        expected_dd_tables = {
            "serv_team_season_weather_v1",
            "serv_play_calling_weather_v1",
            "serv_indoor_outdoor_compare_v1",
        }
        if dd_tables != expected_dd_tables:
            errors.append(f"data_dictionary table set mismatch: actual={sorted(dd_tables)}, expected={sorted(expected_dd_tables)}")

    print(f"latest_serving_run_id={latest_run_id}")
    print(f"latest_serving_build_run_ts={latest_run_ts}")
    print(f"team_rows_source={team_src_cnt}, team_rows_serving={team_cnt}")
    print(f"play_rows_source={play_src_cnt}, play_rows_serving={play_cnt}")
    print(f"indoor_rows_source={indoor_src_cnt}, indoor_rows_serving={indoor_cnt}")
    print(f"duplicate_grain_rows team={team_dup}, play={play_dup}, indoor={indoor_dup}")

    if errors:
        print("VALIDATION_FAILED")
        for e in errors:
            print(" -", e)
        raise RuntimeError("Batch C1 validation failed")

    print("VALIDATION_PASSED")
    spark.stop()


if __name__ == "__main__":
    main()
