from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

BUCKET = "msba405-nfl-weather-raw"
IN_ISD_HISTORY = f"gs://{BUCKET}/bronze/reference/noaa/isd-history.csv"
OUT_DIM_STATION = f"gs://{BUCKET}/silver/dim_station_hourly/"
OUT_QC = f"gs://{BUCKET}/silver/dim_station_hourly_qc/"

MIN_YEAR = 2000
MAX_YEAR = 2025

def main():
    spark = SparkSession.builder.appName("build_dim_station_hourly").getOrCreate()

    raw = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(IN_ISD_HISTORY)
    )

    # Normalize columns (ISD history uses these exact headers typically)
    usaf = F.lpad(F.trim(F.col("USAF").cast("string")), 6, "0")
    wban = F.lpad(F.trim(F.col("WBAN").cast("string")), 5, "0")
    station_key = F.concat_ws("-", usaf, wban)

    df = raw.select(
        usaf.alias("usaf"),
        wban.alias("wban"),
        station_key.alias("station_key"),
        F.trim(F.col("STATION NAME")).alias("station_name"),
        F.upper(F.trim(F.col("CTRY"))).alias("country"),
        F.upper(F.trim(F.col("STATE"))).alias("state"),
        F.upper(F.trim(F.col("ICAO"))).alias("icao"),
        F.trim(F.col("LAT")).cast("double").alias("station_lat"),
        F.trim(F.col("LON")).cast("double").alias("station_lon"),
        F.trim(F.col("ELEV(M)")).cast("double").alias("elev_m"),
        F.trim(F.col("BEGIN")).cast("int").alias("begin_yyyymmdd"),
        F.trim(F.col("END")).cast("int").alias("end_yyyymmdd"),
    )

    # ---- US-only filter ----
    # ISD history uses CTRY='US' for United States. Keep only US.
    df = df.filter(F.col("country") == F.lit("US"))

    # Basic validity
    df = df.filter(F.col("station_lat").isNotNull() & F.col("station_lon").isNotNull())
    df = df.filter((F.col("station_lat") >= -90) & (F.col("station_lat") <= 90))
    df = df.filter((F.col("station_lon") >= -180) & (F.col("station_lon") <= 180))
    df = df.filter(F.col("begin_yyyymmdd").isNotNull() & F.col("end_yyyymmdd").isNotNull())

    df = df.withColumn("begin_year", (F.col("begin_yyyymmdd") / 10000).cast("int"))
    df = df.withColumn("end_year", (F.col("end_yyyymmdd") / 10000).cast("int"))

    # Coverage filter (optional but useful for your 2000-2025 project window)
    df = df.filter((F.col("begin_year") <= MIN_YEAR) & (F.col("end_year") >= MAX_YEAR))

    # De-dup: one row per station_key
    # Prefer: has ICAO, longer name, higher end_year
    win = Window.partitionBy("station_key").orderBy(
        F.desc(F.col("end_year")),
        F.desc(F.length(F.col("icao"))),
        F.desc(F.length(F.col("station_name")))
    )
    df = df.withColumn("rn", F.row_number().over(win)).filter(F.col("rn") == 1).drop("rn")

    # Write outputs
    df.write.mode("overwrite").parquet(OUT_DIM_STATION)

    qc = spark.createDataFrame(
        [
            ("dim_station_rows", df.count()),
            ("min_begin_year", df.agg(F.min("begin_year")).collect()[0][0]),
            ("max_end_year", df.agg(F.max("end_year")).collect()[0][0]),
            ("distinct_states", df.select("state").distinct().count()),
        ],
        ["metric", "value"],
    )
    qc.write.mode("overwrite").parquet(OUT_QC)

    print("DONE")
    print("Wrote:", OUT_DIM_STATION)
    print("QC:", OUT_QC)
    spark.stop()

if __name__ == "__main__":
    main()
