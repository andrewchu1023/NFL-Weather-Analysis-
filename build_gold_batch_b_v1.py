import datetime as dt
import json
import os
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


BUCKET = "msba405-nfl-weather-raw"

FACT_IN = f"gs://{BUCKET}/gold/fact_play_weather_hourly_v1/"

TEAM_OUT = f"gs://{BUCKET}/gold/mart_team_season_weather_v1/"
PLAYCALL_OUT = f"gs://{BUCKET}/gold/mart_play_calling_weather_v1/"
INDOOR_OUT = f"gs://{BUCKET}/gold/mart_indoor_outdoor_compare_v1/"

TEAM_QC_BASE = f"gs://{BUCKET}/qc/gold/mart_team_season_weather_v1/"
PLAYCALL_QC_BASE = f"gs://{BUCKET}/qc/gold/mart_play_calling_weather_v1/"
INDOOR_QC_BASE = f"gs://{BUCKET}/qc/gold/mart_indoor_outdoor_compare_v1/"

TEAM_QC_SUMMARY = TEAM_QC_BASE + "summary/"
TEAM_QC_SAMPLE_TOTALS = TEAM_QC_BASE + "sample_totals/"

PLAYCALL_QC_SUMMARY = PLAYCALL_QC_BASE + "summary/"
PLAYCALL_QC_SAMPLE_TOTALS = PLAYCALL_QC_BASE + "sample_totals/"

INDOOR_QC_SUMMARY = INDOOR_QC_BASE + "summary/"
INDOOR_QC_SAMPLE_TOTALS = INDOOR_QC_BASE + "sample_totals/"


REQUIRED_FACT_COLUMNS = [
    "game_id",
    "play_id",
    "season",
    "posteam",
    "home_team",
    "away_team",
    "pass_attempt",
    "rush_attempt",
    "complete_pass",
    "epa",
    "temp_bin",
    "wind_bin",
    "precip_bin",
    "roof_env_bin",
    "weather_key_match_flag",
    "weather_observed_flag",
    "game_home_score_final",
    "game_away_score_final",
    "batch_a_build_run_ts",
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
        # Avoid intermittent AQE final-plan waits seen on large parquet writes in local mode.
        .config("spark.sql.adaptive.enabled", "false")
        # Keep shuffle width moderate to avoid excessive tiny output files.
        .config("spark.sql.shuffle.partitions", "64")
        # Use v2 committer semantics to minimize expensive recursive rename/merge work.
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


def make_unique_columns(cols):
    seen = {}
    unique = []
    for c in cols:
        if c not in seen:
            seen[c] = 0
            unique.append(c)
        else:
            seen[c] += 1
            unique.append(f"{c}__dup{seen[c]}")
    return unique


def safe_divide(numer, denom):
    return F.when(denom > F.lit(0), numer.cast("double") / denom.cast("double"))


def check_required_columns(df: DataFrame, required_cols, label: str):
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise RuntimeError(f"{label} missing required columns: {missing}")


def check_duplicate_grain(df: DataFrame, grain_cols, label: str):
    dup_cnt = (
        df.groupBy(*grain_cols)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )
    if dup_cnt > 0:
        raise RuntimeError(f"{label} duplicate grain rows found: {dup_cnt}")


def range_violation_count(df: DataFrame, col_name: str, low: float, high: float):
    return (
        df.filter(
            F.col(col_name).isNotNull()
            & ((F.col(col_name) < F.lit(low)) | (F.col(col_name) > F.lit(high)))
        ).count()
    )


def build_base_offensive_plays(fact: DataFrame) -> DataFrame:
    season_source = F.col("season").cast("int")
    if "season__dup1" in fact.columns:
        season_source = F.coalesce(season_source, F.col("season__dup1").cast("int"))

    base = (
        fact.withColumn("season", season_source)
        .withColumn("team", F.trim(F.col("posteam").cast("string")))
        .withColumn("pass_attempt_i", F.when(F.col("pass_attempt").cast("int") == 1, F.lit(1)).otherwise(F.lit(0)))
        .withColumn("rush_attempt_i", F.when(F.col("rush_attempt").cast("int") == 1, F.lit(1)).otherwise(F.lit(0)))
        .withColumn(
            "complete_pass_i",
            F.when(
                (F.col("pass_attempt").cast("int") == 1) & (F.col("complete_pass").cast("int") == 1),
                F.lit(1),
            ).otherwise(F.lit(0)),
        )
        .withColumn("epa_d", F.col("epa").cast("double"))
        .withColumn("epa_valid_i", F.when(F.col("epa").isNotNull(), F.lit(1)).otherwise(F.lit(0)))
        .withColumn("weather_key_match_flag", F.when(F.col("weather_key_match_flag").cast("int") == 1, F.lit(1)).otherwise(F.lit(0)))
        .withColumn("weather_observed_flag", F.when(F.col("weather_observed_flag").cast("int") == 1, F.lit(1)).otherwise(F.lit(0)))
        .withColumn(
            "team_points_final",
            F.when(F.col("team") == F.col("home_team"), F.col("game_home_score_final").cast("double"))
            .when(F.col("team") == F.col("away_team"), F.col("game_away_score_final").cast("double")),
        )
        .withColumn("temp_bin", F.coalesce(F.col("temp_bin"), F.lit("UNKNOWN")))
        .withColumn("wind_bin", F.coalesce(F.col("wind_bin"), F.lit("UNKNOWN")))
        .withColumn("precip_bin", F.coalesce(F.col("precip_bin"), F.lit("UNKNOWN")))
        .withColumn("roof_env_bin", F.coalesce(F.col("roof_env_bin"), F.lit("UNKNOWN")))
    )

    base = base.filter(
        (F.col("game_id").isNotNull())
        & (F.col("play_id").isNotNull())
        & (F.col("team").isNotNull())
        & (F.col("season").isNotNull())
        & ((F.col("pass_attempt_i") + F.col("rush_attempt_i")) > F.lit(0))
    )

    return base.select(
        "season",
        "game_id",
        "play_id",
        "team",
        "pass_attempt_i",
        "rush_attempt_i",
        "complete_pass_i",
        "epa_d",
        "epa_valid_i",
        "team_points_final",
        "temp_bin",
        "wind_bin",
        "precip_bin",
        "roof_env_bin",
        "weather_key_match_flag",
        "weather_observed_flag",
        "batch_a_build_run_ts",
    )


def build_mart_team_season_weather(base_ws: DataFrame, run_ts: str) -> DataFrame:
    grain = ["season", "team", "temp_bin", "wind_bin", "precip_bin", "roof_env_bin", "weather_sample"]

    core = (
        base_ws.groupBy(*grain)
        .agg(
            F.count(F.lit(1)).cast("long").alias("n_plays"),
            F.sum("pass_attempt_i").cast("long").alias("n_pass_attempts"),
            F.sum("rush_attempt_i").cast("long").alias("n_rush_attempts"),
            F.sum("complete_pass_i").cast("long").alias("n_completions"),
            F.sum("epa_valid_i").cast("long").alias("n_epa_plays"),
            F.sum("epa_d").cast("double").alias("epa_sum"),
            F.countDistinct("game_id").cast("long").alias("n_games"),
            F.max("batch_a_build_run_ts").alias("batch_a_build_run_ts"),
        )
    )

    points = (
        base_ws.select(*(grain + ["team", "game_id", "team_points_final"]))
        .where(F.col("team_points_final").isNotNull())
        .dropDuplicates(grain + ["team", "game_id"])
        .groupBy(*grain)
        .agg(
            F.count(F.lit(1)).cast("long").alias("n_team_games_points"),
            F.countDistinct("game_id").cast("long").alias("n_games_points"),
            F.avg("team_points_final").cast("double").alias("points_per_game"),
        )
    )

    out = (
        core.join(points, on=grain, how="left")
        .withColumn("pass_rate", safe_divide(F.col("n_pass_attempts"), F.col("n_pass_attempts") + F.col("n_rush_attempts")))
        .withColumn("completion_pct", safe_divide(F.col("n_completions"), F.col("n_pass_attempts")))
        .withColumn("epa_per_play", safe_divide(F.col("epa_sum"), F.col("n_epa_plays")))
        .withColumn("batch_b_build_run_ts", F.lit(run_ts))
        .select(
            *grain,
            "n_plays",
            "n_pass_attempts",
            "n_rush_attempts",
            "n_completions",
            "n_epa_plays",
            "n_games",
            "n_games_points",
            "n_team_games_points",
            "pass_rate",
            "completion_pct",
            "epa_per_play",
            "points_per_game",
            "batch_a_build_run_ts",
            "batch_b_build_run_ts",
        )
    )

    return out


def build_mart_play_calling_weather(base_ws: DataFrame, run_ts: str) -> DataFrame:
    grain = ["season", "team", "temp_bin", "wind_bin", "precip_bin", "weather_sample"]
    baseline_grain = ["season", "team", "weather_sample"]

    core = (
        base_ws.groupBy(*grain)
        .agg(
            F.count(F.lit(1)).cast("long").alias("n_plays"),
            F.sum("pass_attempt_i").cast("long").alias("n_pass_attempts"),
            F.sum("rush_attempt_i").cast("long").alias("n_rush_attempts"),
            F.countDistinct("game_id").cast("long").alias("n_games"),
            F.max("batch_a_build_run_ts").alias("batch_a_build_run_ts"),
        )
        .withColumn("pass_rate", safe_divide(F.col("n_pass_attempts"), F.col("n_pass_attempts") + F.col("n_rush_attempts")))
        .withColumn("rush_rate", safe_divide(F.col("n_rush_attempts"), F.col("n_pass_attempts") + F.col("n_rush_attempts")))
    )

    baseline = (
        base_ws.groupBy(*baseline_grain)
        .agg(
            F.sum("pass_attempt_i").cast("long").alias("n_pass_attempts_baseline"),
            F.sum("rush_attempt_i").cast("long").alias("n_rush_attempts_baseline"),
        )
        .withColumn(
            "pass_rate_baseline",
            safe_divide(
                F.col("n_pass_attempts_baseline"),
                F.col("n_pass_attempts_baseline") + F.col("n_rush_attempts_baseline"),
            ),
        )
        .withColumn(
            "rush_rate_baseline",
            safe_divide(
                F.col("n_rush_attempts_baseline"),
                F.col("n_pass_attempts_baseline") + F.col("n_rush_attempts_baseline"),
            ),
        )
    )

    out = (
        core.join(baseline, on=baseline_grain, how="left")
        .withColumn("pass_rate_shift_vs_team_season", F.col("pass_rate") - F.col("pass_rate_baseline"))
        .withColumn("rush_rate_shift_vs_team_season", F.col("rush_rate") - F.col("rush_rate_baseline"))
        .withColumn("batch_b_build_run_ts", F.lit(run_ts))
        .select(
            *grain,
            "n_plays",
            "n_pass_attempts",
            "n_rush_attempts",
            "n_games",
            "pass_rate",
            "rush_rate",
            "pass_rate_baseline",
            "rush_rate_baseline",
            "pass_rate_shift_vs_team_season",
            "rush_rate_shift_vs_team_season",
            "batch_a_build_run_ts",
            "batch_b_build_run_ts",
        )
    )

    return out


def build_mart_indoor_outdoor_compare(base_ws: DataFrame, run_ts: str) -> DataFrame:
    grain = ["season", "roof_env_bin", "weather_sample"]

    core = (
        base_ws.groupBy(*grain)
        .agg(
            F.count(F.lit(1)).cast("long").alias("n_plays"),
            F.sum("pass_attempt_i").cast("long").alias("n_pass_attempts"),
            F.sum("rush_attempt_i").cast("long").alias("n_rush_attempts"),
            F.sum("complete_pass_i").cast("long").alias("n_completions"),
            F.sum("epa_valid_i").cast("long").alias("n_epa_plays"),
            F.sum("epa_d").cast("double").alias("epa_sum"),
            F.countDistinct("game_id").cast("long").alias("n_games"),
            F.countDistinct("team").cast("long").alias("n_teams"),
            F.max("batch_a_build_run_ts").alias("batch_a_build_run_ts"),
        )
    )

    points = (
        base_ws.select(*(grain + ["team", "game_id", "team_points_final"]))
        .where(F.col("team_points_final").isNotNull())
        .dropDuplicates(grain + ["team", "game_id"])
        .groupBy(*grain)
        .agg(
            F.count(F.lit(1)).cast("long").alias("n_team_games_points"),
            F.countDistinct("game_id").cast("long").alias("n_games_points"),
            F.avg("team_points_final").cast("double").alias("points_per_game"),
        )
    )

    out = (
        core.join(points, on=grain, how="left")
        .withColumn("pass_rate", safe_divide(F.col("n_pass_attempts"), F.col("n_pass_attempts") + F.col("n_rush_attempts")))
        .withColumn("completion_pct", safe_divide(F.col("n_completions"), F.col("n_pass_attempts")))
        .withColumn("epa_per_play", safe_divide(F.col("epa_sum"), F.col("n_epa_plays")))
        .withColumn("batch_b_build_run_ts", F.lit(run_ts))
        .select(
            *grain,
            "n_plays",
            "n_pass_attempts",
            "n_rush_attempts",
            "n_completions",
            "n_epa_plays",
            "n_games",
            "n_games_points",
            "n_team_games_points",
            "n_teams",
            "pass_rate",
            "completion_pct",
            "epa_per_play",
            "points_per_game",
            "batch_a_build_run_ts",
            "batch_b_build_run_ts",
        )
    )

    return out


def build_summary(df: DataFrame, mart_name: str, run_ts: str) -> DataFrame:
    return (
        df.agg(
            F.count(F.lit(1)).cast("long").alias("n_rows"),
            F.countDistinct("season").cast("long").alias("n_seasons"),
            F.min("season").cast("int").alias("min_season"),
            F.max("season").cast("int").alias("max_season"),
            F.countDistinct("weather_sample").cast("long").alias("n_weather_samples"),
            F.sum("n_plays").cast("long").alias("sum_n_plays"),
            F.sum("n_pass_attempts").cast("long").alias("sum_n_pass_attempts"),
            F.sum("n_rush_attempts").cast("long").alias("sum_n_rush_attempts"),
            F.max("batch_a_build_run_ts").alias("batch_a_build_run_ts"),
        )
        .withColumn("mart_name", F.lit(mart_name))
        .withColumn("batch_b_build_run_ts", F.lit(run_ts))
    )


def main():
    spark = build_spark("build_gold_batch_b_v1")
    spark.sparkContext.setLogLevel("WARN")

    run_ts = dt.datetime.now(dt.UTC).strftime("%Y%m%dT%H%M%SZ")

    fact_raw = spark.read.parquet(FACT_IN)
    fact = fact_raw.toDF(*make_unique_columns(fact_raw.columns))
    check_required_columns(fact, REQUIRED_FACT_COLUMNS, "fact_play_weather_hourly_v1")

    if "season__dup1" in fact.columns:
        season_mismatch = (
            fact.filter(
                F.col("season").isNotNull()
                & F.col("season__dup1").isNotNull()
                & (F.col("season").cast("string") != F.col("season__dup1").cast("string"))
            ).count()
        )
        if season_mismatch > 0:
            raise RuntimeError(f"Found season mismatch between duplicated season columns: {season_mismatch}")

    source_batch_a_ts = fact.select(F.max("batch_a_build_run_ts").alias("batch_a_build_run_ts")).collect()[0]["batch_a_build_run_ts"]
    if source_batch_a_ts is None:
        raise RuntimeError("Missing batch_a_build_run_ts in fact input.")

    base = build_base_offensive_plays(fact).cache()
    base_cnt = base.count()
    if base_cnt == 0:
        raise RuntimeError("No offensive plays available in Batch A fact; cannot build Batch B marts.")

    base_ws = (
        base.withColumn("weather_sample", F.lit("ALL_PLAYS"))
        .unionByName(base.filter(F.col("weather_observed_flag") == 1).withColumn("weather_sample", F.lit("OBSERVED_ONLY")))
        .cache()
    )
    base_ws_cnt = base_ws.count()
    if base_ws_cnt == 0:
        raise RuntimeError("No rows available in weather_sample-expanded base.")

    team_mart = build_mart_team_season_weather(base_ws, run_ts).cache()
    playcall_mart = build_mart_play_calling_weather(base_ws, run_ts).cache()
    indoor_mart = build_mart_indoor_outdoor_compare(base_ws, run_ts).cache()

    team_cnt = team_mart.count()
    playcall_cnt = playcall_mart.count()
    indoor_cnt = indoor_mart.count()
    if team_cnt == 0 or playcall_cnt == 0 or indoor_cnt == 0:
        raise RuntimeError(
            f"One or more marts are empty: team={team_cnt}, playcall={playcall_cnt}, indoor={indoor_cnt}"
        )

    check_duplicate_grain(
        team_mart,
        ["season", "team", "temp_bin", "wind_bin", "precip_bin", "roof_env_bin", "weather_sample"],
        "mart_team_season_weather_v1",
    )
    check_duplicate_grain(
        playcall_mart,
        ["season", "team", "temp_bin", "wind_bin", "precip_bin", "weather_sample"],
        "mart_play_calling_weather_v1",
    )
    check_duplicate_grain(
        indoor_mart,
        ["season", "roof_env_bin", "weather_sample"],
        "mart_indoor_outdoor_compare_v1",
    )

    # Hard metric sanity gates.
    violations = []
    for mart_name, mart_df, has_completion in [
        ("team", team_mart, True),
        ("playcall", playcall_mart, False),
        ("indoor", indoor_mart, True),
    ]:
        pass_rate_bad = range_violation_count(mart_df, "pass_rate", 0.0, 1.0)
        if pass_rate_bad > 0:
            violations.append(f"{mart_name}: pass_rate out-of-range rows={pass_rate_bad}")

        if "rush_rate" in mart_df.columns:
            rush_rate_bad = range_violation_count(mart_df, "rush_rate", 0.0, 1.0)
            if rush_rate_bad > 0:
                violations.append(f"{mart_name}: rush_rate out-of-range rows={rush_rate_bad}")

        if has_completion:
            completion_bad = range_violation_count(mart_df, "completion_pct", 0.0, 1.0)
            if completion_bad > 0:
                violations.append(f"{mart_name}: completion_pct out-of-range rows={completion_bad}")

        denom_bad = mart_df.filter(
            (F.col("n_pass_attempts") == 0) & F.col("completion_pct").isNotNull()
            if has_completion
            else F.lit(False)
        ).count()
        if denom_bad > 0:
            violations.append(f"{mart_name}: completion_pct populated with zero pass attempts rows={denom_bad}")

    if violations:
        raise RuntimeError("Batch B metric sanity violations: " + "; ".join(violations))

    team_summary = build_summary(team_mart, "mart_team_season_weather_v1", run_ts)
    playcall_summary = build_summary(playcall_mart, "mart_play_calling_weather_v1", run_ts)
    indoor_summary = build_summary(indoor_mart, "mart_indoor_outdoor_compare_v1", run_ts)

    team_sample_totals = team_mart.groupBy("weather_sample").agg(
        F.sum("n_plays").cast("long").alias("sum_n_plays"),
        F.sum("n_pass_attempts").cast("long").alias("sum_n_pass_attempts"),
        F.sum("n_rush_attempts").cast("long").alias("sum_n_rush_attempts"),
    ).withColumn("batch_b_build_run_ts", F.lit(run_ts))

    playcall_sample_totals = playcall_mart.groupBy("weather_sample").agg(
        F.sum("n_plays").cast("long").alias("sum_n_plays"),
        F.sum("n_pass_attempts").cast("long").alias("sum_n_pass_attempts"),
        F.sum("n_rush_attempts").cast("long").alias("sum_n_rush_attempts"),
    ).withColumn("batch_b_build_run_ts", F.lit(run_ts))

    indoor_sample_totals = indoor_mart.groupBy("weather_sample").agg(
        F.sum("n_plays").cast("long").alias("sum_n_plays"),
        F.sum("n_pass_attempts").cast("long").alias("sum_n_pass_attempts"),
        F.sum("n_rush_attempts").cast("long").alias("sum_n_rush_attempts"),
    ).withColumn("batch_b_build_run_ts", F.lit(run_ts))

    # Repartition by output partition keys to keep output file counts bounded and commits faster.
    team_mart.repartition(64, "season", "weather_sample").write.mode("overwrite").partitionBy("season", "weather_sample").parquet(TEAM_OUT)
    playcall_mart.repartition(64, "season", "weather_sample").write.mode("overwrite").partitionBy("season", "weather_sample").parquet(PLAYCALL_OUT)
    indoor_mart.repartition(64, "season", "weather_sample").write.mode("overwrite").partitionBy("season", "weather_sample").parquet(INDOOR_OUT)

    team_summary.write.mode("overwrite").parquet(TEAM_QC_SUMMARY)
    playcall_summary.write.mode("overwrite").parquet(PLAYCALL_QC_SUMMARY)
    indoor_summary.write.mode("overwrite").parquet(INDOOR_QC_SUMMARY)

    team_sample_totals.write.mode("overwrite").parquet(TEAM_QC_SAMPLE_TOTALS)
    playcall_sample_totals.write.mode("overwrite").parquet(PLAYCALL_QC_SAMPLE_TOTALS)
    indoor_sample_totals.write.mode("overwrite").parquet(INDOOR_QC_SAMPLE_TOTALS)

    print("DONE")
    print(f"SOURCE_BATCH_A_BUILD_RUN_TS={source_batch_a_ts}")
    print(f"BATCH_B_BUILD_RUN_TS={run_ts}")
    print(f"TEAM_OUT={TEAM_OUT}")
    print(f"PLAYCALL_OUT={PLAYCALL_OUT}")
    print(f"INDOOR_OUT={INDOOR_OUT}")
    print("TEAM_SUMMARY:")
    team_summary.show(truncate=False)
    print("PLAYCALL_SUMMARY:")
    playcall_summary.show(truncate=False)
    print("INDOOR_SUMMARY:")
    indoor_summary.show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
