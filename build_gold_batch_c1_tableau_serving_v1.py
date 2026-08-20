import datetime as dt
import json
import os
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


BUCKET = "msba405-nfl-weather-raw"
SERVING_VERSION = "v1"

TEAM_IN = f"gs://{BUCKET}/gold/mart_team_season_weather_v1/"
PLAYCALL_IN = f"gs://{BUCKET}/gold/mart_play_calling_weather_v1/"
INDOOR_IN = f"gs://{BUCKET}/gold/mart_indoor_outdoor_compare_v1/"

BASE_OUT = f"gs://{BUCKET}/gold/tableau_serving/v1/"
TEAM_OUT = BASE_OUT + "serv_team_season_weather_v1/"
PLAYCALL_OUT = BASE_OUT + "serv_play_calling_weather_v1/"
INDOOR_OUT = BASE_OUT + "serv_indoor_outdoor_compare_v1/"
RUN_AUDIT_OUT = BASE_OUT + "run_audit_v1/"
TABLE_AUDIT_OUT = BASE_OUT + "table_audit_v1/"
DQ_AUDIT_OUT = BASE_OUT + "dq_audit_v1/"
DATA_DICTIONARY_OUT = BASE_OUT + "data_dictionary_v1/"

TEAM_GRAIN = ["season", "team", "temp_bin", "wind_bin", "precip_bin", "roof_env_bin", "weather_sample"]
PLAYCALL_GRAIN = ["season", "team", "temp_bin", "wind_bin", "precip_bin", "weather_sample"]
INDOOR_GRAIN = ["season", "roof_env_bin", "weather_sample"]

REQUIRED_METADATA_COLS = [
    "source_batch_a_build_run_ts",
    "source_batch_b_build_run_ts",
    "serving_build_run_ts",
    "serving_version",
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
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.shuffle.partitions", "64")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.cleanup-failures.ignored", "true")
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


def require_columns(df: DataFrame, cols, label: str):
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise RuntimeError(f"{label} missing columns: {missing}")


def duplicate_grain_count(df: DataFrame, grain_cols):
    return df.groupBy(*grain_cols).count().filter(F.col("count") > 1).count()


def range_violation_count(df: DataFrame, col_name: str, low: float, high: float):
    return (
        df.filter(
            F.col(col_name).isNotNull()
            & ((F.col(col_name) < F.lit(low)) | (F.col(col_name) > F.lit(high)))
        ).count()
    )


def single_distinct_value(df: DataFrame, col_name: str, label: str):
    vals = [r[col_name] for r in df.select(col_name).distinct().collect()]
    if len(vals) != 1:
        raise RuntimeError(f"{label} expected exactly one distinct {col_name}, found {len(vals)}: {vals}")
    if vals[0] is None:
        raise RuntimeError(f"{label} has null {col_name}")
    return vals[0]


def success_marker_exists(spark: SparkSession, path: str) -> bool:
    jvm = spark._jvm
    jsc = spark.sparkContext._jsc
    conf = jsc.hadoopConfiguration()
    marker = jvm.org.apache.hadoop.fs.Path(path.rstrip("/") + "/_SUCCESS")
    fs = marker.getFileSystem(conf)
    return fs.exists(marker)


def add_dq_row(rows, check_name, table_name, expected, actual, status, severity="ERROR"):
    rows.append(
        {
            "check_name": check_name,
            "table_name": table_name,
            "expected_value": str(expected),
            "actual_value": str(actual),
            "status": status,
            "severity": severity,
        }
    )


def summarize_serving_table(df: DataFrame, table_name: str, source_row_count: int, duplicate_rows: int):
    stats = (
        df.agg(
            F.count(F.lit(1)).cast("long").alias("serving_row_count"),
            F.min("season").cast("int").alias("min_season"),
            F.max("season").cast("int").alias("max_season"),
            F.countDistinct("weather_sample").cast("long").alias("n_weather_samples"),
            F.sum("n_plays").cast("long").alias("sum_n_plays"),
        )
        .collect()[0]
    )

    return {
        "table_name": table_name,
        "source_row_count": int(source_row_count),
        "serving_row_count": int(stats["serving_row_count"]),
        "min_season": int(stats["min_season"]),
        "max_season": int(stats["max_season"]),
        "n_weather_samples": int(stats["n_weather_samples"]),
        "sum_n_plays": int(stats["sum_n_plays"]),
        "duplicate_grain_rows": int(duplicate_rows),
    }


def build_data_dictionary_rows(table_name: str, df: DataFrame, grain_cols, serving_build_run_ts: str, serving_run_id: str):
    descriptions = {
        "season": "NFL season year.",
        "team": "Offense team abbreviation.",
        "temp_bin": "Temperature bin label from dim_weather_bins_v1.",
        "wind_bin": "Wind bin label from dim_weather_bins_v1.",
        "precip_bin": "Precipitation bin label from dim_weather_bins_v1.",
        "roof_env_bin": "Roof environment category.",
        "weather_sample": "Weather sampling scope: ALL_PLAYS or OBSERVED_ONLY.",
        "n_plays": "Number of offensive plays in the grain.",
        "n_pass_attempts": "Pass attempts count.",
        "n_rush_attempts": "Rush attempts count.",
        "n_completions": "Completions count.",
        "n_epa_plays": "EPA-valid play count.",
        "n_games": "Distinct games count.",
        "pass_rate": "Pass attempts divided by offensive attempts.",
        "rush_rate": "Rush attempts divided by offensive attempts.",
        "completion_pct": "Completions divided by pass attempts.",
        "epa_per_play": "EPA sum divided by EPA-valid plays.",
        "points_per_game": "Average final points per team-game in grain.",
        "source_batch_a_build_run_ts": "Upstream Batch A run timestamp.",
        "source_batch_b_build_run_ts": "Upstream Batch B run timestamp.",
        "serving_build_run_ts": "Batch C1 serving build timestamp.",
        "serving_version": "Serving contract version.",
        "serving_run_id": "Unique Batch C1 run identifier.",
    }

    rows = []
    for i, field in enumerate(df.schema.fields, start=1):
        rows.append(
            {
                "table_name": table_name,
                "column_name": field.name,
                "data_type": field.dataType.simpleString(),
                "ordinal_position": i,
                "is_grain_key": 1 if field.name in grain_cols else 0,
                "column_description": descriptions.get(field.name, "Serving column mirrored from Batch B mart."),
                "serving_build_run_ts": serving_build_run_ts,
                "serving_version": SERVING_VERSION,
                "serving_run_id": serving_run_id,
            }
        )
    return rows


def main():
    spark = build_spark("build_gold_batch_c1_tableau_serving_v1")
    spark.sparkContext.setLogLevel("WARN")

    started_at = dt.datetime.now(dt.UTC)
    serving_build_run_ts = started_at.strftime("%Y%m%dT%H%M%SZ")
    serving_run_id = f"c1_{serving_build_run_ts}"

    team_src = spark.read.parquet(TEAM_IN)
    play_src = spark.read.parquet(PLAYCALL_IN)
    indoor_src = spark.read.parquet(INDOOR_IN)

    required_team = [
        "season", "team", "temp_bin", "wind_bin", "precip_bin", "roof_env_bin", "weather_sample",
        "n_plays", "n_pass_attempts", "n_rush_attempts", "n_completions", "n_epa_plays", "n_games",
        "pass_rate", "completion_pct", "epa_per_play", "points_per_game", "batch_a_build_run_ts", "batch_b_build_run_ts",
    ]
    required_play = [
        "season", "team", "temp_bin", "wind_bin", "precip_bin", "weather_sample", "n_plays", "n_pass_attempts",
        "n_rush_attempts", "n_games", "pass_rate", "rush_rate", "pass_rate_baseline", "rush_rate_baseline",
        "pass_rate_shift_vs_team_season", "rush_rate_shift_vs_team_season", "batch_a_build_run_ts", "batch_b_build_run_ts",
    ]
    required_indoor = [
        "season", "roof_env_bin", "weather_sample", "n_plays", "n_pass_attempts", "n_rush_attempts", "n_completions",
        "n_epa_plays", "n_games", "n_teams", "pass_rate", "completion_pct", "epa_per_play", "points_per_game",
        "batch_a_build_run_ts", "batch_b_build_run_ts",
    ]

    require_columns(team_src, required_team, "team source")
    require_columns(play_src, required_play, "play-calling source")
    require_columns(indoor_src, required_indoor, "indoor source")

    team_src_cnt = team_src.count()
    play_src_cnt = play_src.count()
    indoor_src_cnt = indoor_src.count()
    if team_src_cnt == 0 or play_src_cnt == 0 or indoor_src_cnt == 0:
        raise RuntimeError(f"source marts contain empty table: team={team_src_cnt}, play={play_src_cnt}, indoor={indoor_src_cnt}")

    team_a_ts = single_distinct_value(team_src, "batch_a_build_run_ts", "team source")
    play_a_ts = single_distinct_value(play_src, "batch_a_build_run_ts", "play source")
    indoor_a_ts = single_distinct_value(indoor_src, "batch_a_build_run_ts", "indoor source")

    team_b_ts = single_distinct_value(team_src, "batch_b_build_run_ts", "team source")
    play_b_ts = single_distinct_value(play_src, "batch_b_build_run_ts", "play source")
    indoor_b_ts = single_distinct_value(indoor_src, "batch_b_build_run_ts", "indoor source")

    if not (team_a_ts == play_a_ts == indoor_a_ts):
        raise RuntimeError(f"batch_a_build_run_ts mismatch across sources: team={team_a_ts}, play={play_a_ts}, indoor={indoor_a_ts}")
    if not (team_b_ts == play_b_ts == indoor_b_ts):
        raise RuntimeError(f"batch_b_build_run_ts mismatch across sources: team={team_b_ts}, play={play_b_ts}, indoor={indoor_b_ts}")

    def with_serving_metadata(df: DataFrame):
        return (
            df.withColumn("source_batch_a_build_run_ts", F.col("batch_a_build_run_ts"))
            .withColumn("source_batch_b_build_run_ts", F.col("batch_b_build_run_ts"))
            .withColumn("serving_build_run_ts", F.lit(serving_build_run_ts))
            .withColumn("serving_version", F.lit(SERVING_VERSION))
            .withColumn("serving_run_id", F.lit(serving_run_id))
        )

    team_serv = with_serving_metadata(team_src).cache()
    play_serv = with_serving_metadata(play_src).cache()
    indoor_serv = with_serving_metadata(indoor_src).cache()

    dq_rows = []

    team_dup = duplicate_grain_count(team_serv, TEAM_GRAIN)
    play_dup = duplicate_grain_count(play_serv, PLAYCALL_GRAIN)
    indoor_dup = duplicate_grain_count(indoor_serv, INDOOR_GRAIN)

    add_dq_row(dq_rows, "row_count_match", "serv_team_season_weather_v1", team_src_cnt, team_serv.count(), "PASS" if team_src_cnt == team_serv.count() else "FAIL")
    add_dq_row(dq_rows, "row_count_match", "serv_play_calling_weather_v1", play_src_cnt, play_serv.count(), "PASS" if play_src_cnt == play_serv.count() else "FAIL")
    add_dq_row(dq_rows, "row_count_match", "serv_indoor_outdoor_compare_v1", indoor_src_cnt, indoor_serv.count(), "PASS" if indoor_src_cnt == indoor_serv.count() else "FAIL")

    add_dq_row(dq_rows, "duplicate_grain_rows", "serv_team_season_weather_v1", 0, team_dup, "PASS" if team_dup == 0 else "FAIL")
    add_dq_row(dq_rows, "duplicate_grain_rows", "serv_play_calling_weather_v1", 0, play_dup, "PASS" if play_dup == 0 else "FAIL")
    add_dq_row(dq_rows, "duplicate_grain_rows", "serv_indoor_outdoor_compare_v1", 0, indoor_dup, "PASS" if indoor_dup == 0 else "FAIL")

    team_pass_bad = range_violation_count(team_serv, "pass_rate", 0.0, 1.0)
    team_comp_bad = range_violation_count(team_serv, "completion_pct", 0.0, 1.0)
    play_pass_bad = range_violation_count(play_serv, "pass_rate", 0.0, 1.0)
    play_rush_bad = range_violation_count(play_serv, "rush_rate", 0.0, 1.0)
    indoor_pass_bad = range_violation_count(indoor_serv, "pass_rate", 0.0, 1.0)
    indoor_comp_bad = range_violation_count(indoor_serv, "completion_pct", 0.0, 1.0)

    add_dq_row(dq_rows, "metric_range_pass_rate", "serv_team_season_weather_v1", 0, team_pass_bad, "PASS" if team_pass_bad == 0 else "FAIL")
    add_dq_row(dq_rows, "metric_range_completion_pct", "serv_team_season_weather_v1", 0, team_comp_bad, "PASS" if team_comp_bad == 0 else "FAIL")
    add_dq_row(dq_rows, "metric_range_pass_rate", "serv_play_calling_weather_v1", 0, play_pass_bad, "PASS" if play_pass_bad == 0 else "FAIL")
    add_dq_row(dq_rows, "metric_range_rush_rate", "serv_play_calling_weather_v1", 0, play_rush_bad, "PASS" if play_rush_bad == 0 else "FAIL")
    add_dq_row(dq_rows, "metric_range_pass_rate", "serv_indoor_outdoor_compare_v1", 0, indoor_pass_bad, "PASS" if indoor_pass_bad == 0 else "FAIL")
    add_dq_row(dq_rows, "metric_range_completion_pct", "serv_indoor_outdoor_compare_v1", 0, indoor_comp_bad, "PASS" if indoor_comp_bad == 0 else "FAIL")

    for table_name, df in [
        ("serv_team_season_weather_v1", team_serv),
        ("serv_play_calling_weather_v1", play_serv),
        ("serv_indoor_outdoor_compare_v1", indoor_serv),
    ]:
        invalid_ws = df.filter(~F.col("weather_sample").isin("ALL_PLAYS", "OBSERVED_ONLY")).count()
        add_dq_row(dq_rows, "weather_sample_domain", table_name, 0, invalid_ws, "PASS" if invalid_ws == 0 else "FAIL")

        for meta_col in REQUIRED_METADATA_COLS:
            null_cnt = df.filter(F.col(meta_col).isNull()).count()
            add_dq_row(dq_rows, f"metadata_not_null_{meta_col}", table_name, 0, null_cnt, "PASS" if null_cnt == 0 else "FAIL")

    dq_fail_cnt = sum(1 for r in dq_rows if r["status"] == "FAIL")
    if dq_fail_cnt > 0:
        fail_msg = "; ".join([f"{r['table_name']}:{r['check_name']}={r['actual_value']}" for r in dq_rows if r["status"] == "FAIL"])
        raise RuntimeError(f"Batch C1 DQ pre-write failures: {fail_msg}")

    team_serv.repartition(64, "season", "weather_sample").write.mode("overwrite").partitionBy("season", "weather_sample").parquet(TEAM_OUT)
    play_serv.repartition(64, "season", "weather_sample").write.mode("overwrite").partitionBy("season", "weather_sample").parquet(PLAYCALL_OUT)
    indoor_serv.repartition(32, "season", "weather_sample").write.mode("overwrite").partitionBy("season", "weather_sample").parquet(INDOOR_OUT)

    team_back = spark.read.parquet(TEAM_OUT)
    play_back = spark.read.parquet(PLAYCALL_OUT)
    indoor_back = spark.read.parquet(INDOOR_OUT)

    team_back_cnt = team_back.count()
    play_back_cnt = play_back.count()
    indoor_back_cnt = indoor_back.count()

    if team_back_cnt != team_src_cnt or play_back_cnt != play_src_cnt or indoor_back_cnt != indoor_src_cnt:
        raise RuntimeError(
            "Post-write row count mismatch: "
            f"team src={team_src_cnt} out={team_back_cnt}, "
            f"play src={play_src_cnt} out={play_back_cnt}, "
            f"indoor src={indoor_src_cnt} out={indoor_back_cnt}"
        )

    for path in [TEAM_OUT, PLAYCALL_OUT, INDOOR_OUT]:
        if not success_marker_exists(spark, path):
            raise RuntimeError(f"Missing _SUCCESS marker: {path}")

    table_audit_rows = [
        summarize_serving_table(team_back, "serv_team_season_weather_v1", team_src_cnt, team_dup),
        summarize_serving_table(play_back, "serv_play_calling_weather_v1", play_src_cnt, play_dup),
        summarize_serving_table(indoor_back, "serv_indoor_outdoor_compare_v1", indoor_src_cnt, indoor_dup),
    ]

    for r in table_audit_rows:
        r.update(
            {
                "source_batch_a_build_run_ts": team_a_ts,
                "source_batch_b_build_run_ts": team_b_ts,
                "serving_build_run_ts": serving_build_run_ts,
                "serving_version": SERVING_VERSION,
                "serving_run_id": serving_run_id,
            }
        )

    dq_audit_rows = []
    for r in dq_rows:
        row = dict(r)
        row.update(
            {
                "source_batch_a_build_run_ts": team_a_ts,
                "source_batch_b_build_run_ts": team_b_ts,
                "serving_build_run_ts": serving_build_run_ts,
                "serving_version": SERVING_VERSION,
                "serving_run_id": serving_run_id,
            }
        )
        dq_audit_rows.append(row)

    table_audit_df = spark.createDataFrame(table_audit_rows)
    dq_audit_df = spark.createDataFrame(dq_audit_rows)

    data_dict_rows = []
    data_dict_rows.extend(build_data_dictionary_rows("serv_team_season_weather_v1", team_back, TEAM_GRAIN, serving_build_run_ts, serving_run_id))
    data_dict_rows.extend(build_data_dictionary_rows("serv_play_calling_weather_v1", play_back, PLAYCALL_GRAIN, serving_build_run_ts, serving_run_id))
    data_dict_rows.extend(build_data_dictionary_rows("serv_indoor_outdoor_compare_v1", indoor_back, INDOOR_GRAIN, serving_build_run_ts, serving_run_id))
    data_dict_df = spark.createDataFrame(data_dict_rows)

    completed_at = dt.datetime.now(dt.UTC)
    run_audit_row = {
        "serving_run_id": serving_run_id,
        "serving_version": SERVING_VERSION,
        "serving_build_run_ts": serving_build_run_ts,
        "source_batch_a_build_run_ts": team_a_ts,
        "source_batch_b_build_run_ts": team_b_ts,
        "status": "SUCCESS",
        "started_at_utc": started_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "completed_at_utc": completed_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source_team_rows": int(team_src_cnt),
        "source_playcall_rows": int(play_src_cnt),
        "source_indoor_rows": int(indoor_src_cnt),
        "serving_team_rows": int(team_back_cnt),
        "serving_playcall_rows": int(play_back_cnt),
        "serving_indoor_rows": int(indoor_back_cnt),
        "dq_fail_count": int(dq_fail_cnt),
    }
    run_audit_df = spark.createDataFrame([run_audit_row])

    table_audit_df.coalesce(1).write.mode("append").parquet(TABLE_AUDIT_OUT)
    dq_audit_df.coalesce(1).write.mode("append").parquet(DQ_AUDIT_OUT)
    data_dict_df.coalesce(1).write.mode("overwrite").parquet(DATA_DICTIONARY_OUT)
    run_audit_df.coalesce(1).write.mode("append").parquet(RUN_AUDIT_OUT)

    for path in [TABLE_AUDIT_OUT, DQ_AUDIT_OUT, DATA_DICTIONARY_OUT, RUN_AUDIT_OUT]:
        if not success_marker_exists(spark, path):
            raise RuntimeError(f"Missing _SUCCESS marker: {path}")

    print("DONE")
    print(f"SERVING_RUN_ID={serving_run_id}")
    print(f"SERVING_BUILD_RUN_TS={serving_build_run_ts}")
    print(f"SOURCE_BATCH_A_BUILD_RUN_TS={team_a_ts}")
    print(f"SOURCE_BATCH_B_BUILD_RUN_TS={team_b_ts}")
    print(f"TEAM_ROWS={team_back_cnt}")
    print(f"PLAYCALL_ROWS={play_back_cnt}")
    print(f"INDOOR_ROWS={indoor_back_cnt}")

    spark.stop()


if __name__ == "__main__":
    main()
