import datetime as dt
import json
import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql import functions as F


BUCKET = "msba405-nfl-weather-raw"

SOURCE_SILVER = f"gs://{BUCKET}/silver/noaa_hourly_observations/"
SILVER_OUT = f"gs://{BUCKET}/silver/noaa_hourly_features_extended/v1/"

QC_BASE = f"gs://{BUCKET}/qc/noaa/weather_hourly_features_extended_v1/"
QC_SUMMARY = QC_BASE + "summary/"
QC_NULLS_BY_YEAR = QC_BASE + "null_rates_by_year/"
QC_NULLS_BY_STATION_YEAR = QC_BASE + "null_rates_by_station_year/"

FEATURE_VERSION = "v1"


def ratio_null(col_name: str):
    return (
        F.sum(F.when(F.col(col_name).isNull(), F.lit(1)).otherwise(F.lit(0))).cast("double")
        / F.count(F.lit(1)).cast("double")
    )


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
            # GCS connector only checks GOOGLE_APPLICATION_CREDENTIALS for ADC discovery.
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
    if SOURCE_SILVER.startswith("gs://") or SILVER_OUT.startswith("gs://"):
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
    spark = build_spark("build_silver_noaa_hourly_features_extended_v1")
    spark.sparkContext.setLogLevel("WARN")

    run_ts = dt.datetime.now(dt.UTC).strftime("%Y%m%dT%H%M%SZ")

    source = spark.read.parquet(SOURCE_SILVER).select(
        "station_key",
        "obs_hour_utc",
        "obs_year",
        "obs_month",
        "source_dataset",
        "source_file_uri",
        "obs_present_flag",
        "air_temp_c",
        "dew_point_c",
        "sea_level_pressure_hpa",
        "wind_speed_mps",
        "wind_dir_deg",
        "build_run_ts",
    )

    source_build_run_ts = source.agg(F.max("build_run_ts").alias("source_build_run_ts")).collect()[0]["source_build_run_ts"]
    if source_build_run_ts is None:
        raise RuntimeError("Source silver build_run_ts is null for all rows.")

    t = F.col("air_temp_c")
    td = F.col("dew_point_c")
    slp = F.col("sea_level_pressure_hpa")
    ws = F.col("wind_speed_mps")
    wd = F.col("wind_dir_deg")

    # Relative humidity (Magnus), clamped to [0,100]
    a = F.lit(17.625)
    b = F.lit(243.04)
    rh_raw = F.lit(100.0) * F.exp((a * td) / (b + td)) / F.exp((a * t) / (b + t))
    rh_domain_ok = (
        t.isNotNull()
        & td.isNotNull()
        & t.between(F.lit(-90.0), F.lit(70.0))
        & td.between(F.lit(-100.0), F.lit(70.0))
    )
    rh = F.when(rh_domain_ok, F.least(F.lit(100.0), F.greatest(F.lit(0.0), rh_raw))).otherwise(
        F.lit(None).cast("double")
    )

    # Wet-bulb proxy (Stull approximation)
    tw_raw = (
        t * F.atan(F.lit(0.151977) * F.sqrt(rh + F.lit(8.313659)))
        + F.atan(t + rh)
        - F.atan(rh - F.lit(1.676331))
        + F.lit(0.00391838) * F.pow(rh, F.lit(1.5)) * F.atan(F.lit(0.023101) * rh)
        - F.lit(4.686035)
    )
    tw_ok = (
        t.isNotNull()
        & rh.isNotNull()
        & t.between(F.lit(-80.0), F.lit(60.0))
        & rh.between(F.lit(1.0), F.lit(100.0))
        & tw_raw.between(F.lit(-100.0), F.lit(70.0))
        & (tw_raw <= (t + F.lit(2.0)))
    )
    tw = F.when(tw_ok, tw_raw).otherwise(F.lit(None).cast("double"))

    # Apparent temperature:
    # - Wind chill for cold/windy conditions
    # - Humidex for warm/humid conditions
    # - Otherwise fallback to air temperature
    ws_kph = ws * F.lit(3.6)
    wc_ok = t.isNotNull() & ws_kph.isNotNull() & (t <= F.lit(10.0)) & (ws_kph > F.lit(4.8)) & (ws_kph <= F.lit(200.0))
    wc = (
        F.lit(13.12)
        + F.lit(0.6215) * t
        - F.lit(11.37) * F.pow(ws_kph, F.lit(0.16))
        + F.lit(0.3965) * t * F.pow(ws_kph, F.lit(0.16))
    )

    humidex_ok = t.isNotNull() & td.isNotNull() & (t >= F.lit(20.0)) & td.between(F.lit(-40.0), F.lit(50.0))
    e = F.lit(6.11) * F.exp(F.lit(5417.7530) * ((F.lit(1.0) / F.lit(273.16)) - (F.lit(1.0) / (td + F.lit(273.15)))))
    humidex = t + F.lit(0.5555) * (e - F.lit(10.0))

    apparent = (
        F.when(t.isNull(), F.lit(None).cast("double"))
        .when(wc_ok, wc)
        .when(humidex_ok, humidex)
        .otherwise(t)
    )
    apparent = F.when(apparent.between(F.lit(-100.0), F.lit(70.0)), apparent).otherwise(F.lit(None).cast("double"))

    # Wind components (meteorological convention)
    wd_norm = F.when(wd == F.lit(360.0), F.lit(0.0)).otherwise(wd)
    wind_ok = ws.isNotNull() & wd_norm.isNotNull() & ws.between(F.lit(0.0), F.lit(150.0)) & wd_norm.between(F.lit(0.0), F.lit(360.0))
    wd_rad = F.radians(wd_norm)
    wind_u = F.when(wind_ok, -ws * F.sin(wd_rad)).otherwise(F.lit(None).cast("double"))
    wind_v = F.when(wind_ok, -ws * F.cos(wd_rad)).otherwise(F.lit(None).cast("double"))

    w = Window.partitionBy("station_key").orderBy("obs_hour_utc")
    slp_lag_1 = F.lag(slp, 1).over(w)
    slp_lag_3 = F.lag(slp, 3).over(w)
    slp_lag_6 = F.lag(slp, 6).over(w)

    p1_raw = slp - slp_lag_1
    p3_raw = slp - slp_lag_3
    p6_raw = slp - slp_lag_6

    p1 = F.when(slp.isNotNull() & slp_lag_1.isNotNull() & (F.abs(p1_raw) <= F.lit(20.0)), p1_raw).otherwise(
        F.lit(None).cast("double")
    )
    p3 = F.when(slp.isNotNull() & slp_lag_3.isNotNull() & (F.abs(p3_raw) <= F.lit(30.0)), p3_raw).otherwise(
        F.lit(None).cast("double")
    )
    p6 = F.when(slp.isNotNull() & slp_lag_6.isNotNull() & (F.abs(p6_raw) <= F.lit(40.0)), p6_raw).otherwise(
        F.lit(None).cast("double")
    )

    p3_tendency = (
        F.when(p3.isNull(), F.lit(None).cast("int"))
        .when(p3 > F.lit(0.5), F.lit(1))
        .when(p3 < F.lit(-0.5), F.lit(-1))
        .otherwise(F.lit(0))
    )

    features = (
        source.withColumn("relative_humidity_pct", rh)
        .withColumn("wet_bulb_temp_c", tw)
        .withColumn("apparent_temp_c", apparent)
        .withColumn("wind_u_mps", wind_u)
        .withColumn("wind_v_mps", wind_v)
        .withColumn("pressure_delta_1h_hpa", p1)
        .withColumn("pressure_delta_3h_hpa", p3)
        .withColumn("pressure_delta_6h_hpa", p6)
        .withColumn("pressure_tendency_3h", p3_tendency)
        .withColumn("rh_valid_flag", F.when(F.col("relative_humidity_pct").isNotNull(), F.lit(1)).otherwise(F.lit(0)))
        .withColumn("wet_bulb_valid_flag", F.when(F.col("wet_bulb_temp_c").isNotNull(), F.lit(1)).otherwise(F.lit(0)))
        .withColumn("apparent_temp_valid_flag", F.when(F.col("apparent_temp_c").isNotNull(), F.lit(1)).otherwise(F.lit(0)))
        .withColumn("wind_components_valid_flag", F.when(F.col("wind_u_mps").isNotNull() & F.col("wind_v_mps").isNotNull(), F.lit(1)).otherwise(F.lit(0)))
        .withColumn("pressure_delta_valid_1h_flag", F.when(F.col("pressure_delta_1h_hpa").isNotNull(), F.lit(1)).otherwise(F.lit(0)))
        .withColumn("pressure_delta_valid_3h_flag", F.when(F.col("pressure_delta_3h_hpa").isNotNull(), F.lit(1)).otherwise(F.lit(0)))
        .withColumn("pressure_delta_valid_6h_flag", F.when(F.col("pressure_delta_6h_hpa").isNotNull(), F.lit(1)).otherwise(F.lit(0)))
        .withColumn("source_build_run_ts", F.lit(source_build_run_ts))
        .withColumn("feature_build_run_ts", F.lit(run_ts))
        .withColumn("feature_version", F.lit(FEATURE_VERSION))
        .select(
            "station_key",
            "obs_hour_utc",
            "obs_year",
            "obs_month",
            "source_dataset",
            "obs_present_flag",
            "relative_humidity_pct",
            "wet_bulb_temp_c",
            "apparent_temp_c",
            "wind_u_mps",
            "wind_v_mps",
            "pressure_delta_1h_hpa",
            "pressure_delta_3h_hpa",
            "pressure_delta_6h_hpa",
            "pressure_tendency_3h",
            "rh_valid_flag",
            "wet_bulb_valid_flag",
            "apparent_temp_valid_flag",
            "wind_components_valid_flag",
            "pressure_delta_valid_1h_flag",
            "pressure_delta_valid_3h_flag",
            "pressure_delta_valid_6h_flag",
            "source_build_run_ts",
            "feature_build_run_ts",
            "feature_version",
        )
    )

    # Hard gate: output must remain unique on join key.
    dup_cnt = (
        features.groupBy("station_key", "obs_hour_utc")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )
    if dup_cnt > 0:
        raise RuntimeError(f"Duplicate keys found in features output: {dup_cnt}")

    summary = features.agg(
        F.count(F.lit(1)).cast("long").alias("n_rows"),
        F.countDistinct("station_key").cast("long").alias("n_station_keys"),
        F.min("obs_hour_utc").alias("min_obs_hour_utc"),
        F.max("obs_hour_utc").alias("max_obs_hour_utc"),
        ratio_null("relative_humidity_pct").alias("null_rate_relative_humidity_pct"),
        ratio_null("wet_bulb_temp_c").alias("null_rate_wet_bulb_temp_c"),
        ratio_null("apparent_temp_c").alias("null_rate_apparent_temp_c"),
        ratio_null("wind_u_mps").alias("null_rate_wind_u_mps"),
        ratio_null("wind_v_mps").alias("null_rate_wind_v_mps"),
        ratio_null("pressure_delta_1h_hpa").alias("null_rate_pressure_delta_1h_hpa"),
        ratio_null("pressure_delta_3h_hpa").alias("null_rate_pressure_delta_3h_hpa"),
        ratio_null("pressure_delta_6h_hpa").alias("null_rate_pressure_delta_6h_hpa"),
        F.lit(source_build_run_ts).alias("source_build_run_ts"),
        F.lit(run_ts).alias("feature_build_run_ts"),
        F.lit(FEATURE_VERSION).alias("feature_version"),
    )

    nulls_by_year = (
        features.groupBy("obs_year")
        .agg(
            F.count(F.lit(1)).cast("long").alias("n_rows"),
            ratio_null("relative_humidity_pct").alias("null_rate_relative_humidity_pct"),
            ratio_null("wet_bulb_temp_c").alias("null_rate_wet_bulb_temp_c"),
            ratio_null("apparent_temp_c").alias("null_rate_apparent_temp_c"),
            ratio_null("wind_u_mps").alias("null_rate_wind_u_mps"),
            ratio_null("wind_v_mps").alias("null_rate_wind_v_mps"),
            ratio_null("pressure_delta_1h_hpa").alias("null_rate_pressure_delta_1h_hpa"),
            ratio_null("pressure_delta_3h_hpa").alias("null_rate_pressure_delta_3h_hpa"),
            ratio_null("pressure_delta_6h_hpa").alias("null_rate_pressure_delta_6h_hpa"),
        )
        .withColumn("feature_build_run_ts", F.lit(run_ts))
        .orderBy("obs_year")
    )

    nulls_by_station_year = (
        features.groupBy("station_key", "obs_year")
        .agg(
            F.count(F.lit(1)).cast("long").alias("n_rows"),
            ratio_null("relative_humidity_pct").alias("null_rate_relative_humidity_pct"),
            ratio_null("wet_bulb_temp_c").alias("null_rate_wet_bulb_temp_c"),
            ratio_null("apparent_temp_c").alias("null_rate_apparent_temp_c"),
            ratio_null("pressure_delta_3h_hpa").alias("null_rate_pressure_delta_3h_hpa"),
        )
        .withColumn("feature_build_run_ts", F.lit(run_ts))
        .orderBy(F.desc("null_rate_relative_humidity_pct"), F.asc("station_key"), F.asc("obs_year"))
    )

    features.write.mode("overwrite").partitionBy("obs_year").parquet(SILVER_OUT)
    summary.write.mode("overwrite").parquet(QC_SUMMARY)
    nulls_by_year.write.mode("overwrite").parquet(QC_NULLS_BY_YEAR)
    nulls_by_station_year.write.mode("overwrite").parquet(QC_NULLS_BY_STATION_YEAR)

    print("DONE")
    print(f"Features silver: {SILVER_OUT}")
    print(f"QC summary: {QC_SUMMARY}")
    print(f"QC null rates by year: {QC_NULLS_BY_YEAR}")
    print(f"QC null rates by station-year: {QC_NULLS_BY_STATION_YEAR}")
    summary.show(truncate=False)
    nulls_by_year.show(100, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
