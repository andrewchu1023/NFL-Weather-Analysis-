from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BUCKET = "msba405-nfl-weather-raw"
BASE = f"gs://{BUCKET}/bronze/pbp"
OUT  = f"gs://{BUCKET}/silver/pbp_play_level_core"

START, END = 2000, 2025

CORE_COLS = [
    "play_id","game_id","season","season_type","week","game_date",
    "home_team","away_team","posteam","defteam",
    "stadium_id","stadium","roof","surface","location",
    "start_time","time_of_day",

    "drive","qtr","down","ydstogo","yardline_100","goal_to_go",
    "quarter_seconds_remaining","game_seconds_remaining","score_differential",
    "home_score","away_score","total_home_score","total_away_score",

    "play_type","pass_attempt","rush_attempt","shotgun","no_huddle",
    "pass_length","air_yards","yards_after_catch",

    "complete_pass","yards_gained","epa","wp","wpa"
]

INT_COLS = [
    "season","week","drive","qtr","down","ydstogo","yardline_100",
    "goal_to_go","quarter_seconds_remaining","game_seconds_remaining","score_differential",
    "home_score","away_score","total_home_score","total_away_score",
    "pass_attempt","rush_attempt","complete_pass","shotgun","no_huddle"
]
DOUBLE_COLS = ["epa","wp","wpa","air_yards","yards_after_catch","yards_gained"]

def main():
    spark = SparkSession.builder.appName("build_pbp_play_level_core_by_year_v4").getOrCreate()

    # Stability + lower memory/shuffle pressure
    spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")
    spark.conf.set("spark.sql.shuffle.partitions", "16")  # keep it small on single-node
    spark.conf.set("spark.sql.adaptive.enabled", "true")

    for y in range(START, END + 1):
        in_path = f"{BASE}/play_by_play_{y}.parquet"
        out_path = f"{OUT}/season={y}/"   # IMPORTANT: write per-season directory

        df = spark.read.parquet(in_path)

        keep = [c for c in CORE_COLS if c in df.columns]
        x = df.select(*keep)

        # filter to offense plays
        x = x.filter(F.col("play_type").isin("pass", "run"))
        x = x.filter(F.col("posteam").isNotNull())

        # cast to stable types
        for c in INT_COLS:
            if c in x.columns:
                x = x.withColumn(c, F.col(c).cast("int"))
        for c in DOUBLE_COLS:
            if c in x.columns:
                x = x.withColumn(c, F.col(c).cast("double"))

        # add missing cols as nulls so schema is identical every year
        for c in CORE_COLS:
            if c not in x.columns:
                x = x.withColumn(c, F.lit(None))

        x = x.select(*CORE_COLS)

        # reduce number of output files (single-node friendly)
        x = x.coalesce(4)

        # overwrite this season only (idempotent per year)
        x.write.mode("overwrite").parquet(out_path)

        print(f"[WROTE] season={y} -> {out_path} (cols={len(x.columns)})")

    print("DONE. All seasons written under:", OUT)
    spark.stop()

if __name__ == "__main__":
    main()
