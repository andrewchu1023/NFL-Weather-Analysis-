from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BUCKET = "msba405-nfl-weather-raw"
IN_PATH = f"gs://{BUCKET}/bronze/pbp/"
OUT_PATH = f"gs://{BUCKET}/silver/pbp_game_level/"

spark = SparkSession.builder.appName("pbp_to_game_level").getOrCreate()

df = spark.read.parquet(IN_PATH)

# Keep core play types for offense metrics (exclude kickoff/punt/etc.)
df_core = df.filter(F.col("play_type").isin("pass", "run"))

game = (
    df_core.groupBy(
        "game_id","season","week","game_date","home_team","away_team",
        "roof","stadium_id","start_time"
    )
    .agg(
        F.sum(F.col("pass_attempt").cast("int")).alias("pass_attempts"),
        F.sum(F.col("rush_attempt").cast("int")).alias("rush_attempts"),
        F.sum(F.col("complete_pass").cast("int")).alias("completions"),
        F.sum(F.col("yards_gained")).alias("total_yards"),
        F.avg(F.col("epa")).alias("avg_epa_play"),
        F.sum(F.col("epa")).alias("total_epa"),
        F.max("home_score").alias("home_score_final"),
        F.max("away_score").alias("away_score_final"),
    )
    .withColumn("total_points", F.col("home_score_final") + F.col("away_score_final"))
    .withColumn("plays", F.col("pass_attempts") + F.col("rush_attempts"))
    .withColumn("pass_rate", F.when(F.col("plays") > 0, F.col("pass_attempts") / F.col("plays")))
    .withColumn("completion_pct", F.when(F.col("pass_attempts") > 0, F.col("completions") / F.col("pass_attempts")))
)

# Write silver output
game.write.mode("overwrite").parquet(OUT_PATH)

print("Wrote game-level table to:", OUT_PATH)
spark.stop()
