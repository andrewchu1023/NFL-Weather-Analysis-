import datetime as dt

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window


BUCKET = "msba405-nfl-weather-raw"

BRONZE_IN = f"gs://{BUCKET}/bronze/noaa/isd/"
MANIFEST_RESOLVED = f"gs://{BUCKET}/qc/noaa/production_station_year_manifest_v1/manifest_resolved/"

SILVER_OUT = f"gs://{BUCKET}/silver/noaa_hourly_observations/"

QC_BASE = f"gs://{BUCKET}/qc/noaa/noaa_hourly_build_v1/"
QC_SUMMARY = QC_BASE + "summary/"
QC_MISSING = QC_BASE + "missing_hours_by_station_year/"


def safe_div_tenths(col):
    return F.when((col <= -9990) | (col >= 9990), F.lit(None).cast("double")).otherwise(col.cast("double") / F.lit(10.0))


def main():
    spark = SparkSession.builder.appName("build_silver_noaa_hourly_observations_v1").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    run_ts = dt.datetime.now(dt.UTC).strftime("%Y%m%dT%H%M%SZ")

    raw = (
        spark.read.text(BRONZE_IN)
        .withColumn("source_file_uri", F.input_file_name())
        .withColumn("source_dataset", F.regexp_extract(F.col("source_file_uri"), r"/source=([^/]+)/", 1))
        .withColumn("year_part", F.regexp_extract(F.col("source_file_uri"), r"/year=([0-9]{4})/", 1).cast("int"))
        .withColumn("station_key", F.regexp_extract(F.col("source_file_uri"), r"/station_key=([^/]+)/", 1))
    )

    # ISD-Lite parser (space-delimited)
    lite_parts = F.split(F.trim(F.col("value")), r"\s+")
    lite = (
        raw.filter(F.col("source_dataset") == "isd_lite")
        .withColumn("nparts", F.size(lite_parts))
        .filter(F.col("nparts") >= 12)
        .withColumn("yy", lite_parts.getItem(0).cast("int"))
        .withColumn("mm", lite_parts.getItem(1).cast("int"))
        .withColumn("dd", lite_parts.getItem(2).cast("int"))
        .withColumn("hh", lite_parts.getItem(3).cast("int"))
        .withColumn("obs_utc_ts", F.to_timestamp(F.format_string("%04d-%02d-%02d %02d:00:00", F.col("yy"), F.col("mm"), F.col("dd"), F.col("hh"))))
        .withColumn("obs_minute", F.lit(0))
        .withColumn("air_temp_c", safe_div_tenths(lite_parts.getItem(4).cast("int")))
        .withColumn("dew_point_c", safe_div_tenths(lite_parts.getItem(5).cast("int")))
        .withColumn("sea_level_pressure_hpa", safe_div_tenths(lite_parts.getItem(6).cast("int")))
        .withColumn("wind_dir_deg", F.when((lite_parts.getItem(7).cast("int") < 0) | (lite_parts.getItem(7).cast("int") >= 999), F.lit(None).cast("double")).otherwise(lite_parts.getItem(7).cast("double")))
        .withColumn("wind_speed_mps", safe_div_tenths(lite_parts.getItem(8).cast("int")))
        .select(
            "station_key",
            "source_dataset",
            "source_file_uri",
            "obs_utc_ts",
            "obs_minute",
            "air_temp_c",
            "dew_point_c",
            "sea_level_pressure_hpa",
            "wind_dir_deg",
            "wind_speed_mps",
        )
        .filter(F.col("obs_utc_ts").isNotNull())
    )

    # ISD parser (fixed-width)
    isd = (
        raw.filter(F.col("source_dataset") == "isd")
        .withColumn("yyyy", F.substring("value", 16, 4).cast("int"))
        .withColumn("mm", F.substring("value", 20, 2).cast("int"))
        .withColumn("dd", F.substring("value", 22, 2).cast("int"))
        .withColumn("hh", F.substring("value", 24, 2).cast("int"))
        .withColumn("mi", F.substring("value", 26, 2).cast("int"))
        .withColumn("obs_utc_ts", F.to_timestamp(F.format_string("%04d-%02d-%02d %02d:%02d:00", F.col("yyyy"), F.col("mm"), F.col("dd"), F.col("hh"), F.col("mi"))))
        .withColumn("obs_minute", F.col("mi"))
        .withColumn("wind_dir_raw", F.substring("value", 61, 3).cast("int"))
        .withColumn("wind_speed_raw", F.substring("value", 66, 4).cast("int"))
        .withColumn("air_temp_raw", F.substring("value", 88, 5).cast("int"))
        .withColumn("dew_point_raw", F.substring("value", 94, 5).cast("int"))
        .withColumn("slp_raw", F.substring("value", 100, 5).cast("int"))
        .withColumn("air_temp_c", safe_div_tenths(F.col("air_temp_raw")))
        .withColumn("dew_point_c", safe_div_tenths(F.col("dew_point_raw")))
        .withColumn("sea_level_pressure_hpa", safe_div_tenths(F.col("slp_raw")))
        .withColumn("wind_speed_mps", safe_div_tenths(F.col("wind_speed_raw")))
        .withColumn("wind_dir_deg", F.when((F.col("wind_dir_raw") < 0) | (F.col("wind_dir_raw") >= 999), F.lit(None).cast("double")).otherwise(F.col("wind_dir_raw").cast("double")))
        .select(
            "station_key",
            "source_dataset",
            "source_file_uri",
            "obs_utc_ts",
            "obs_minute",
            "air_temp_c",
            "dew_point_c",
            "sea_level_pressure_hpa",
            "wind_dir_deg",
            "wind_speed_mps",
        )
        .filter(F.col("obs_utc_ts").isNotNull())
    )

    obs = lite.unionByName(isd, allowMissingColumns=True)
    obs = obs.withColumn("obs_hour_utc", F.date_trunc("hour", F.col("obs_utc_ts")))

    # Deduplicate to one row per station_key + obs_hour_utc
    # Preference: source_dataset isd first, then smallest minute-offset from :00.
    dedup_w = (
        Window.partitionBy("station_key", "obs_hour_utc")
        .orderBy(
            F.when(F.col("source_dataset") == "isd", F.lit(0)).otherwise(F.lit(1)).asc(),
            F.abs(F.col("obs_minute")).asc(),
            F.col("source_file_uri").asc(),
        )
    )
    obs_hourly = (
        obs.withColumn("rn", F.row_number().over(dedup_w))
        .filter(F.col("rn") == 1)
        .drop("rn")
        .select(
            "station_key",
            "obs_hour_utc",
            "source_dataset",
            "source_file_uri",
            "air_temp_c",
            "dew_point_c",
            "sea_level_pressure_hpa",
            "wind_speed_mps",
            "wind_dir_deg",
        )
    )

    combos = (
        spark.read.parquet(MANIFEST_RESOLVED)
        .select(F.col("effective_station_key").alias("station_key"), "year_utc")
        .dropDuplicates(["station_key", "year_utc"])
    )

    # Complete hourly grid for each station-year from manifest-resolved universe.
    grid = (
        combos
        .withColumn("start_ts", F.to_timestamp(F.format_string("%04d-01-01 00:00:00", F.col("year_utc"))))
        .withColumn("end_ts", F.to_timestamp(F.format_string("%04d-12-31 23:00:00", F.col("year_utc"))))
        .withColumn("obs_hour_utc", F.explode(F.expr("sequence(start_ts, end_ts, interval 1 hour)")))
        .select("station_key", "year_utc", "obs_hour_utc")
    )

    silver = (
        grid.alias("g")
        .join(obs_hourly.alias("o"), on=["station_key", "obs_hour_utc"], how="left")
        .withColumn("obs_year", F.year("obs_hour_utc"))
        .withColumn("obs_month", F.month("obs_hour_utc"))
        .withColumn(
            "obs_present_flag",
            F.when(
                F.col("o.source_dataset").isNotNull()
                | F.col("o.air_temp_c").isNotNull()
                | F.col("o.dew_point_c").isNotNull()
                | F.col("o.sea_level_pressure_hpa").isNotNull()
                | F.col("o.wind_speed_mps").isNotNull()
                | F.col("o.wind_dir_deg").isNotNull(),
                F.lit(1),
            ).otherwise(F.lit(0)),
        )
        .withColumn("build_run_ts", F.lit(run_ts))
        .select(
            "station_key",
            "obs_hour_utc",
            "obs_year",
            "obs_month",
            F.col("o.source_dataset").alias("source_dataset"),
            F.col("o.source_file_uri").alias("source_file_uri"),
            F.col("o.air_temp_c").alias("air_temp_c"),
            F.col("o.dew_point_c").alias("dew_point_c"),
            F.col("o.sea_level_pressure_hpa").alias("sea_level_pressure_hpa"),
            F.col("o.wind_speed_mps").alias("wind_speed_mps"),
            F.col("o.wind_dir_deg").alias("wind_dir_deg"),
            "obs_present_flag",
            "build_run_ts",
        )
    )

    # Safety dedupe check: station_key + obs_hour_utc must be unique.
    dup_cnt = (
        silver.groupBy("station_key", "obs_hour_utc")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )
    if dup_cnt > 0:
        raise RuntimeError(f"Found duplicate keys in silver noaa_hourly_observations: {dup_cnt}")

    summary = (
        silver.agg(
            F.count("*").cast("long").alias("n_rows"),
            F.sum("obs_present_flag").cast("long").alias("n_observed_rows"),
            (F.count("*") - F.sum("obs_present_flag")).cast("long").alias("n_missing_rows"),
            F.countDistinct("station_key").cast("long").alias("n_station_keys"),
            F.min("obs_hour_utc").alias("min_obs_hour_utc"),
            F.max("obs_hour_utc").alias("max_obs_hour_utc"),
        )
        .withColumn("build_run_ts", F.lit(run_ts))
    )

    missing_by_station_year = (
        silver.groupBy("station_key", F.col("obs_year").alias("year_utc"))
        .agg(
            F.count("*").cast("long").alias("n_expected_hours"),
            F.sum("obs_present_flag").cast("long").alias("n_observed_hours"),
            (F.count("*") - F.sum("obs_present_flag")).cast("long").alias("n_missing_hours"),
        )
        .orderBy(F.desc("n_missing_hours"), F.asc("station_key"), F.asc("year_utc"))
    )

    silver.write.mode("overwrite").partitionBy("obs_year").parquet(SILVER_OUT)
    summary.write.mode("overwrite").parquet(QC_SUMMARY)
    missing_by_station_year.write.mode("overwrite").parquet(QC_MISSING)

    print("DONE")
    print(f"Silver: {SILVER_OUT}")
    print(f"QC summary: {QC_SUMMARY}")
    print(f"QC missing by station/year: {QC_MISSING}")
    summary.show(truncate=False)
    missing_by_station_year.show(50, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
