import json
import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


BUCKET = "msba405-nfl-weather-raw"

PBP_IN = f"gs://{BUCKET}/silver/pbp_play_level_core/"
FACT_IN = f"gs://{BUCKET}/gold/fact_play_weather_hourly_v1/"
BINS_IN = f"gs://{BUCKET}/gold/dim_weather_bins_v1/"
QC_SUMMARY = f"gs://{BUCKET}/qc/gold/fact_play_weather_hourly_v1/summary/"


REQUIRED_FACT_COLUMNS = [
    "game_id",
    "play_id",
    "season",
    "stadium_id",
    "station_key",
    "venue_tz",
    "venue_tz_norm",
    "venue_roof_type",
    "station_mapped_flag",
    "play_ts_utc",
    "play_hour_utc",
    "play_time_parsed_flag",
    "play_ts_source",
    "weather_hour_utc",
    "weather_key_match_flag",
    "weather_observed_flag",
    "weather_join_tier",
    "air_temp_c",
    "wind_speed_mps",
    "relative_humidity_pct",
    "apparent_temp_c",
    "temp_bin",
    "wind_bin",
    "precip_bin",
    "roof_env_bin",
    "batch_a_build_run_ts",
]

MAPPED_PARSED_MATCH_RATE_MIN = 0.965


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


def main():
    spark = build_spark("validate_gold_batch_a_v1")
    spark.sparkContext.setLogLevel("WARN")

    pbp = spark.read.parquet(PBP_IN)
    fact = spark.read.parquet(FACT_IN)
    bins = spark.read.parquet(BINS_IN)
    qc_summary = spark.read.parquet(QC_SUMMARY)

    errors = []

    missing_cols = [c for c in REQUIRED_FACT_COLUMNS if c not in fact.columns]
    if missing_cols:
        errors.append(f"Missing required fact columns: {missing_cols}")

    pbp_cnt = pbp.count()
    fact_cnt = fact.count()
    if fact_cnt != pbp_cnt:
        errors.append(f"Row count mismatch pbp={pbp_cnt}, fact={fact_cnt}")

    dup_cnt = (
        fact.groupBy("game_id", "play_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )
    if dup_cnt > 0:
        errors.append(f"Duplicate game_id+play_id rows in fact: {dup_cnt}")

    null_pk_cnt = fact.filter(F.col("game_id").isNull() | F.col("play_id").isNull()).count()
    if null_pk_cnt > 0:
        errors.append(f"Null game_id/play_id rows in fact: {null_pk_cnt}")

    null_bin_cnt = fact.filter(
        F.col("temp_bin").isNull()
        | F.col("wind_bin").isNull()
        | F.col("precip_bin").isNull()
        | F.col("roof_env_bin").isNull()
    ).count()
    if null_bin_cnt > 0:
        errors.append(f"Null bin values found: {null_bin_cnt}")

    mapped_cnt = fact.filter(F.col("station_mapped_flag") == 1).count()
    parsed_cnt = fact.filter(F.col("play_time_parsed_flag") == 1).count()
    matched_cnt = fact.filter(F.col("weather_key_match_flag") == 1).count()
    observed_cnt = fact.filter(F.col("weather_observed_flag") == 1).count()

    mapped_rate = mapped_cnt / fact_cnt if fact_cnt else 0.0
    parsed_rate = parsed_cnt / fact_cnt if fact_cnt else 0.0
    matched_rate = matched_cnt / fact_cnt if fact_cnt else 0.0
    observed_rate = observed_cnt / fact_cnt if fact_cnt else 0.0

    if mapped_rate < 0.95:
        errors.append(f"Unexpectedly low station_mapped_flag rate: {mapped_rate:.6f}")
    if parsed_rate < 0.95:
        errors.append(f"Unexpectedly low play_time_parsed_flag rate: {parsed_rate:.6f}")

    mapped_parsed = fact.filter((F.col("station_mapped_flag") == 1) & (F.col("play_time_parsed_flag") == 1))
    mapped_parsed_cnt = mapped_parsed.count()
    mapped_parsed_matched_cnt = mapped_parsed.filter(F.col("weather_key_match_flag") == 1).count()
    mapped_parsed_match_rate = mapped_parsed_matched_cnt / mapped_parsed_cnt if mapped_parsed_cnt else 0.0
    if mapped_parsed_match_rate < MAPPED_PARSED_MATCH_RATE_MIN:
        errors.append(
            "Mapped+parsed weather-key-match rate below contract "
            f"({mapped_parsed_match_rate:.6f} < {MAPPED_PARSED_MATCH_RATE_MIN:.3f})"
        )

    # Basic weather data sanity: key weather fields should not be all-null.
    air_non_null = fact.filter(F.col("air_temp_c").isNotNull()).count()
    wind_non_null = fact.filter(F.col("wind_speed_mps").isNotNull()).count()
    rh_non_null = fact.filter(F.col("relative_humidity_pct").isNotNull()).count()
    if air_non_null == 0 or wind_non_null == 0 or rh_non_null == 0:
        errors.append(
            f"Unexpected all-null weather metrics: air={air_non_null}, wind={wind_non_null}, rh={rh_non_null}"
        )

    # Bin dimension should exist and include UNKNOWN for each bin family.
    expected_bin_types = {"temp_bin", "wind_bin", "precip_bin", "roof_env_bin"}
    found_bin_types = {r["bin_type"] for r in bins.select("bin_type").distinct().collect()}
    if expected_bin_types - found_bin_types:
        errors.append(f"Missing bin types in dim_weather_bins_v1: {sorted(expected_bin_types - found_bin_types)}")

    unknown_by_type = (
        bins.groupBy("bin_type")
        .agg(F.max(F.when(F.col("bin_value") == F.lit("UNKNOWN"), F.lit(1)).otherwise(F.lit(0))).alias("has_unknown"))
        .collect()
    )
    unknown_lookup = {r["bin_type"]: r["has_unknown"] for r in unknown_by_type}
    for t in expected_bin_types:
        if unknown_lookup.get(t, 0) != 1:
            errors.append(f"Bin type missing UNKNOWN row: {t}")

    qc_n_rows = qc_summary.select(F.col("n_rows").cast("long").alias("n_rows")).collect()[0]["n_rows"]
    if qc_n_rows != fact_cnt:
        errors.append(f"QC summary n_rows mismatch: qc={qc_n_rows}, fact={fact_cnt}")

    print("pbp_cnt=", pbp_cnt)
    print("fact_cnt=", fact_cnt)
    print("mapped_rate=", mapped_rate)
    print("parsed_rate=", parsed_rate)
    print("matched_rate=", matched_rate)
    print("observed_rate=", observed_rate)
    print("mapped_parsed_cnt=", mapped_parsed_cnt)
    print("mapped_parsed_match_rate=", mapped_parsed_match_rate)

    if errors:
        print("VALIDATION_FAILED")
        for e in errors:
            print(" -", e)
        raise RuntimeError("Validation failed; see errors above.")

    print("VALIDATION_PASSED")
    spark.stop()


if __name__ == "__main__":
    main()
