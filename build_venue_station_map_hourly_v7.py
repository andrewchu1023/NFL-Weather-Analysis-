from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

BUCKET = "msba405-nfl-weather-raw"

VENUE = f"gs://{BUCKET}/silver/dim_venue/"
STATION = f"gs://{BUCKET}/silver/dim_station_hourly/"

OUT = f"gs://{BUCKET}/silver/venue_station_map_hourly/"
OUT_QC = f"gs://{BUCKET}/silver/venue_station_map_hourly_qc/"
OUT_UNMAPPED = f"gs://{BUCKET}/silver/venue_station_map_hourly_unmapped/"

MAX_KM = 200.0

def haversine_km(lat1, lon1, lat2, lon2):
    r = F.lit(6371.0)
    p = F.lit(3.141592653589793 / 180.0)
    dlat = (lat2 - lat1) * p
    dlon = (lon2 - lon1) * p
    a = F.sin(dlat / 2) ** 2 + F.cos(lat1 * p) * F.cos(lat2 * p) * F.sin(dlon / 2) ** 2
    c = F.lit(2.0) * F.asin(F.sqrt(a))
    return r * c

def main():
    spark = SparkSession.builder.appName("build_venue_station_map_hourly_v7").getOrCreate()

    v0 = spark.read.parquet(VENUE)
    s0 = spark.read.parquet(STATION)

    # ---- Venues (US only) ----
    v = (
        v0.filter(F.col("country") == F.lit("United States"))
          .select(
              "stadium_id", "stadium_name", "city", "state", "country", "tz",
              "lat", "lon", "roof_type", "first_game_date", "last_game_date"
          )
          .withColumnRenamed("state", "venue_state")
          .withColumnRenamed("lon", "lon_raw")
          .withColumnRenamed("lat", "venue_lat")
          # Fix positive longitude bug for US venues
          .withColumn(
              "venue_lon",
              F.when((F.col("lon_raw").isNotNull()) & (F.col("lon_raw") > 0), -F.col("lon_raw"))
               .otherwise(F.col("lon_raw"))
          )
    )

    # US bounds (includes AK/HI)
    v = v.filter(
        F.col("venue_lat").between(15.0, 72.0) &
        F.col("venue_lon").between(-170.0, -60.0)
    )

    # ---- Stations (US only) ----
    s = (
        s0.filter(F.col("country") == F.lit("US"))
          .select(
              "station_key", "station_name", "state", "station_lat", "station_lon",
              "begin_year", "end_year", "country"
          )
          .withColumnRenamed("state", "station_state")
    )

    # ---- Candidate join (within MAX_KM) ----
    cand = (
        v.crossJoin(s)
         .withColumn(
             "distance_km",
             haversine_km(
                 F.col("venue_lat"), F.col("venue_lon"),
                 F.col("station_lat"), F.col("station_lon")
             )
         )
         .filter(F.col("distance_km") <= F.lit(MAX_KM))
    )

    w = Window.partitionBy("stadium_id").orderBy(F.col("distance_km").asc(), F.col("station_key").asc())
    best = (
        cand.withColumn("rn", F.row_number().over(w))
            .filter(F.col("rn") == 1)
            .drop("rn")
            .withColumn("selection_reason", F.lit("nearest_within_max_km"))
    )

    unmapped = v.join(best.select("stadium_id").distinct(), on="stadium_id", how="left_anti")

    # Debug: nearest station without MAX_KM cutoff for unmapped
    if unmapped.limit(1).count() > 0:
        dbg = (
            unmapped.crossJoin(s)
                    .withColumn(
                        "distance_km",
                        haversine_km(
                            F.col("venue_lat"), F.col("venue_lon"),
                            F.col("station_lat"), F.col("station_lon")
                        )
                    )
        )
        w2 = Window.partitionBy("stadium_id").orderBy(F.col("distance_km").asc(), F.col("station_key").asc())
        nearest_any = (
            dbg.withColumn("rn", F.row_number().over(w2))
               .filter(F.col("rn") == 1)
               .select(
                   "stadium_id", "stadium_name", "city", "venue_state", "venue_lat", "venue_lon",
                   "station_key", "station_name", "station_state", "station_lat", "station_lon",
                   "distance_km"
               )
        )
        nearest_any.write.mode("overwrite").parquet(f"{OUT_QC}/nearest_any_for_unmapped/")
    else:
        spark.createDataFrame([], "stadium_id string").write.mode("overwrite").parquet(f"{OUT_QC}/nearest_any_for_unmapped/")

    # Write outputs
    best.select(
        "stadium_id", "stadium_name", "city", "venue_state", "tz",
        "venue_lat", "venue_lon",
        "station_key", "station_name", "station_state",
        "station_lat", "station_lon",
        "distance_km", "begin_year", "end_year",
        "selection_reason"
    ).write.mode("overwrite").parquet(OUT)

    unmapped.select(
        "stadium_id", "stadium_name", "city", "venue_state", "country", "tz",
        "venue_lat", "venue_lon", "lon_raw"
    ).write.mode("overwrite").parquet(OUT_UNMAPPED)

    # QC: force ALL values to string (most stable)
    venue_cnt = v.select("stadium_id").distinct().count()
    mapped_cnt = best.select("stadium_id").distinct().count()
    unmapped_cnt = unmapped.select("stadium_id").distinct().count()

    qc_rows = [
        ("venue_us_distinct", str(int(venue_cnt))),
        ("mapped_venues", str(int(mapped_cnt))),
        ("unmapped", str(int(unmapped_cnt))),
        ("max_km", str(float(MAX_KM))),
    ]
    qc = spark.createDataFrame(qc_rows, ["metric", "value"])
    qc.write.mode("overwrite").parquet(OUT_QC)

    print("DONE")
    print("Wrote mapping:", OUT)
    print("Wrote QC:", OUT_QC)
    print("Wrote unmapped:", OUT_UNMAPPED)
    print(f"QC: venue_us_distinct={venue_cnt}, mapped_venues={mapped_cnt}, unmapped={unmapped_cnt}, max_km={MAX_KM}")

    spark.stop()

if __name__ == "__main__":
    main()
