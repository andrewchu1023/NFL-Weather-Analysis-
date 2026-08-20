import json
import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


BUCKET = "msba405-nfl-weather-raw"

SOURCE_SILVER = f"gs://{BUCKET}/silver/noaa_hourly_observations/"
FEATURE_SILVER = f"gs://{BUCKET}/silver/noaa_hourly_features_extended/v1/"
QC_SUMMARY = f"gs://{BUCKET}/qc/noaa/weather_hourly_features_extended_v1/summary/"

FEATURE_VERSION = "v1"


def detect_google_credentials():
    """Return (path, type) where type is from credential JSON 'type' field if readable."""
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

    # Bootstrap GCS connector when IO paths use gs://.
    if SOURCE_SILVER.startswith("gs://") or FEATURE_SILVER.startswith("gs://") or QC_SUMMARY.startswith("gs://"):
        gcs_connector = os.environ.get(
            "SPARK_GCS_CONNECTOR",
            "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.28",
        )
        builder = (
            builder.config("spark.jars.packages", gcs_connector)
            .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
            .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
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
    spark = build_spark("validate_silver_noaa_hourly_features_extended_v1")
    spark.sparkContext.setLogLevel("WARN")

    source = spark.read.parquet(SOURCE_SILVER).select("station_key", "obs_hour_utc", "obs_year", "wind_speed_mps")
    feature = spark.read.parquet(FEATURE_SILVER)
    qc_summary = spark.read.parquet(QC_SUMMARY)

    source_row_cnt = source.count()
    feature_row_cnt = feature.count()

    source_key_cnt = source.select("station_key", "obs_hour_utc").dropDuplicates().count()
    feature_key_cnt = feature.select("station_key", "obs_hour_utc").dropDuplicates().count()

    source_dup_cnt = (
        source.groupBy("station_key", "obs_hour_utc")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )
    feature_dup_cnt = (
        feature.groupBy("station_key", "obs_hour_utc")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    source_keys = source.select("station_key", "obs_hour_utc").dropDuplicates()
    feature_keys = feature.select("station_key", "obs_hour_utc").dropDuplicates()
    missing_keys_cnt = source_keys.join(feature_keys, on=["station_key", "obs_hour_utc"], how="left_anti").count()
    extra_keys_cnt = feature_keys.join(source_keys, on=["station_key", "obs_hour_utc"], how="left_anti").count()

    rh_range_violations = feature.filter(
        F.col("relative_humidity_pct").isNotNull()
        & ((F.col("relative_humidity_pct") < F.lit(0.0)) | (F.col("relative_humidity_pct") > F.lit(100.0)))
    ).count()
    tendency_invalid_cnt = feature.filter(
        F.col("pressure_tendency_3h").isNotNull() & (~F.col("pressure_tendency_3h").isin([-1, 0, 1]))
    ).count()
    feature_version_invalid_cnt = feature.filter(F.col("feature_version") != F.lit(FEATURE_VERSION)).count()

    source_run_ts_null_cnt = feature.filter(F.col("source_build_run_ts").isNull()).count()
    feature_run_ts_null_cnt = feature.filter(F.col("feature_build_run_ts").isNull()).count()

    # Wind reconstruction check where all fields are present.
    joined_for_wind = source.join(
        feature.select("station_key", "obs_hour_utc", "wind_u_mps", "wind_v_mps"),
        on=["station_key", "obs_hour_utc"],
        how="inner",
    )
    wind_consistency_cnt = joined_for_wind.filter(
        F.col("wind_speed_mps").isNotNull()
        & F.col("wind_u_mps").isNotNull()
        & F.col("wind_v_mps").isNotNull()
        & (F.abs(F.sqrt(F.col("wind_u_mps") ** 2 + F.col("wind_v_mps") ** 2) - F.col("wind_speed_mps")) > F.lit(0.1))
    ).count()

    obs_year_range = feature.agg(F.min("obs_year").alias("min_year"), F.max("obs_year").alias("max_year")).collect()[0]
    feature_build_run_ts_distinct = feature.select("feature_build_run_ts").distinct().count()

    qc_row_cnt = qc_summary.select(F.col("n_rows").cast("long").alias("n_rows")).collect()[0]["n_rows"]
    qc_station_cnt = qc_summary.select(F.col("n_station_keys").cast("long").alias("n_station_keys")).collect()[0]["n_station_keys"]
    feature_station_cnt = feature.select("station_key").distinct().count()

    print("source_row_cnt=", source_row_cnt)
    print("feature_row_cnt=", feature_row_cnt)
    print("source_key_cnt=", source_key_cnt)
    print("feature_key_cnt=", feature_key_cnt)
    print("source_dup_cnt=", source_dup_cnt)
    print("feature_dup_cnt=", feature_dup_cnt)
    print("missing_keys_cnt=", missing_keys_cnt)
    print("extra_keys_cnt=", extra_keys_cnt)
    print("rh_range_violations=", rh_range_violations)
    print("tendency_invalid_cnt=", tendency_invalid_cnt)
    print("feature_version_invalid_cnt=", feature_version_invalid_cnt)
    print("source_run_ts_null_cnt=", source_run_ts_null_cnt)
    print("feature_run_ts_null_cnt=", feature_run_ts_null_cnt)
    print("wind_consistency_cnt_0p1mps=", wind_consistency_cnt)
    print("obs_year_range=", (obs_year_range["min_year"], obs_year_range["max_year"]))
    print("feature_build_run_ts_distinct=", feature_build_run_ts_distinct)
    print("qc_row_cnt=", qc_row_cnt)
    print("qc_station_cnt=", qc_station_cnt)
    print("feature_station_cnt=", feature_station_cnt)

    errors = []
    if source_row_cnt != feature_row_cnt:
        errors.append(f"Row count mismatch: source={source_row_cnt}, feature={feature_row_cnt}")
    if source_key_cnt != feature_key_cnt:
        errors.append(f"Key count mismatch: source={source_key_cnt}, feature={feature_key_cnt}")
    if source_dup_cnt > 0:
        errors.append(f"Source has duplicate keys: {source_dup_cnt}")
    if feature_dup_cnt > 0:
        errors.append(f"Feature has duplicate keys: {feature_dup_cnt}")
    if missing_keys_cnt > 0:
        errors.append(f"Missing keys in feature table: {missing_keys_cnt}")
    if extra_keys_cnt > 0:
        errors.append(f"Extra keys in feature table: {extra_keys_cnt}")
    if rh_range_violations > 0:
        errors.append(f"RH out-of-range rows: {rh_range_violations}")
    if tendency_invalid_cnt > 0:
        errors.append(f"Invalid pressure_tendency_3h rows: {tendency_invalid_cnt}")
    if feature_version_invalid_cnt > 0:
        errors.append(f"Invalid feature_version rows: {feature_version_invalid_cnt}")
    if source_run_ts_null_cnt > 0 or feature_run_ts_null_cnt > 0:
        errors.append(
            f"Run timestamp null rows: source_build_run_ts={source_run_ts_null_cnt}, "
            f"feature_build_run_ts={feature_run_ts_null_cnt}"
        )
    if qc_row_cnt != feature_row_cnt:
        errors.append(f"QC summary n_rows mismatch: qc={qc_row_cnt}, feature={feature_row_cnt}")
    if qc_station_cnt != feature_station_cnt:
        errors.append(f"QC summary n_station_keys mismatch: qc={qc_station_cnt}, feature={feature_station_cnt}")
    if obs_year_range["min_year"] != 2000 or obs_year_range["max_year"] != 2025:
        errors.append(
            f"Unexpected obs_year range: min={obs_year_range['min_year']}, max={obs_year_range['max_year']}"
        )
    if feature_build_run_ts_distinct != 1:
        errors.append(f"Expected exactly one feature_build_run_ts, found {feature_build_run_ts_distinct}")

    if errors:
        print("VALIDATION_FAILED")
        for err in errors:
            print(" -", err)
        raise RuntimeError("Validation failed; see errors above.")

    print("VALIDATION_PASSED")
    spark.stop()


if __name__ == "__main__":
    main()
