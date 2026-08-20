from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

BUCKET = "msba405-nfl-weather-raw"
IN_CSV = f"gs://{BUCKET}/bronze/reference/stadiums/stadiums.csv"

OUT_DIM = f"gs://{BUCKET}/silver/dim_venue/"
OUT_BAD = f"gs://{BUCKET}/silver/dim_venue_bad_rows/"
OUT_QC  = f"gs://{BUCKET}/silver/dim_venue_qc/"

TZ_REQUIRED = True  # set False if you truly don't need tz yet

def normalize_roof(col):
    c = F.lower(F.trim(col))
    return (
        F.when(c.isNull() | (c == ""), F.lit("unknown"))
         .when(c.contains("retract"), F.lit("retractable"))
         .when(c.contains("dome"), F.lit("dome"))
         .when(c == "closed", F.lit("closed"))
         .when(c == "open", F.lit("open"))
         .when(c.contains("out"), F.lit("outdoors"))
         .otherwise(F.lit("unknown"))
    )

def main():
    spark = SparkSession.builder.appName("build_dim_venue").getOrCreate()

    raw = (
        spark.read.option("header", True)
                  .option("multiLine", True)
                  .option("escape", '"')
                  .csv(IN_CSV)
    )

    needed = [
        "stadium_id", "stadium_name", "lat", "lon", "tz", "roof_type",
        "city", "state", "country", "first_game_date", "last_game_date"
    ]
    missing = [c for c in needed if c not in raw.columns]
    if missing:
        raise ValueError(f"Missing expected columns in stadiums.csv: {missing}")

    df = raw.select(*needed)

    def clean_str(c):
        return F.when(F.trim(F.col(c)) == "", F.lit(None)).otherwise(F.trim(F.col(c)))

    for c in ["stadium_id","stadium_name","tz","roof_type","city","state","country"]:
        df = df.withColumn(c, clean_str(c))

    df = (
        df.withColumn("lat", F.col("lat").cast("double"))
          .withColumn("lon", F.col("lon").cast("double"))
          .withColumn("first_game_date", F.to_date(F.col("first_game_date")))
          .withColumn("last_game_date", F.to_date(F.col("last_game_date")))
    )

    df = df.withColumn("roof_type_std", normalize_roof(F.col("roof_type")))

    lat_ok = F.col("lat").between(-90.0, 90.0)
    lon_ok = F.col("lon").between(-180.0, 180.0)
    tz_ok  = (~F.col("tz").isNull()) if TZ_REQUIRED else F.lit(True)

    failure_reason = F.concat_ws(
        ";",
        F.when(F.col("stadium_id").isNull(), F.lit("missing_stadium_id")),
        F.when(F.col("lat").isNull(), F.lit("missing_lat")),
        F.when(F.col("lon").isNull(), F.lit("missing_lon")),
        F.when(~lat_ok & F.col("lat").isNotNull(), F.lit("lat_out_of_range")),
        F.when(~lon_ok & F.col("lon").isNotNull(), F.lit("lon_out_of_range")),
        F.when(~tz_ok, F.lit("missing_tz")),
    )

    df = df.withColumn("failure_reason", failure_reason)
    bad = df.filter(F.col("failure_reason") != "")
    good = df.filter(F.col("failure_reason") == "")

    w = Window.partitionBy("stadium_id").orderBy(
        F.col("last_game_date").desc_nulls_last(),
        F.col("first_game_date").desc_nulls_last(),
        F.col("tz").isNull().cast("int").asc(),
        F.col("stadium_name").isNull().cast("int").asc(),
        F.col("stadium_name").asc_nulls_last()
    )

    good_dedup = (
        good.withColumn("rn", F.row_number().over(w))
            .filter(F.col("rn") == 1)
            .drop("rn")
    )

    dim_venue = (
        good_dedup.select(
            "stadium_id",
            "stadium_name",
            "lat",
            "lon",
            "tz",
            F.col("roof_type_std").alias("roof_type"),
            "city",
            "state",
            "country",
            "first_game_date",
            "last_game_date"
        )
    )

    total_rows = df.count()
    bad_rows = bad.count()
    dim_rows = dim_venue.count()
    unknown_roof = dim_venue.filter(F.col("roof_type") == "unknown").count()

    qc_summary = spark.createDataFrame(
        [
            ("total_input_rows", int(total_rows)),
            ("hard_fail_rows", int(bad_rows)),
            ("final_dim_venue_rows", int(dim_rows)),
            ("unknown_roof_rows", int(unknown_roof)),
        ],
        ["metric", "value"]
    )

    roof_dist = (
        dim_venue.groupBy("roof_type")
                .count()
                .orderBy(F.desc("count"))
    )

    dim_venue.write.mode("overwrite").parquet(OUT_DIM)
    bad.write.mode("overwrite").parquet(OUT_BAD)
    qc_summary.write.mode("overwrite").parquet(f"{OUT_QC}/summary/")
    roof_dist.write.mode("overwrite").parquet(f"{OUT_QC}/roof_dist/")

    print("DONE")
    print("Wrote dim_venue:", OUT_DIM)
    print("Wrote bad_rows:", OUT_BAD)
    print("Wrote qc:", OUT_QC)

    spark.stop()

if __name__ == "__main__":
    main()
