import json
import os
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


BUCKET = "msba405-nfl-weather-raw"

FACT_IN = f"gs://{BUCKET}/gold/fact_play_weather_hourly_v1/"

TEAM_IN = f"gs://{BUCKET}/gold/mart_team_season_weather_v1/"
PLAYCALL_IN = f"gs://{BUCKET}/gold/mart_play_calling_weather_v1/"
INDOOR_IN = f"gs://{BUCKET}/gold/mart_indoor_outdoor_compare_v1/"

TEAM_QC_SUMMARY = f"gs://{BUCKET}/qc/gold/mart_team_season_weather_v1/summary/"
PLAYCALL_QC_SUMMARY = f"gs://{BUCKET}/qc/gold/mart_play_calling_weather_v1/summary/"
INDOOR_QC_SUMMARY = f"gs://{BUCKET}/qc/gold/mart_indoor_outdoor_compare_v1/summary/"

TEAM_REQUIRED = [
    "season",
    "team",
    "temp_bin",
    "wind_bin",
    "precip_bin",
    "roof_env_bin",
    "weather_sample",
    "n_plays",
    "n_pass_attempts",
    "n_rush_attempts",
    "n_completions",
    "n_epa_plays",
    "n_games",
    "pass_rate",
    "completion_pct",
    "epa_per_play",
    "points_per_game",
    "batch_a_build_run_ts",
    "batch_b_build_run_ts",
]

PLAYCALL_REQUIRED = [
    "season",
    "team",
    "temp_bin",
    "wind_bin",
    "precip_bin",
    "weather_sample",
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
]

INDOOR_REQUIRED = [
    "season",
    "roof_env_bin",
    "weather_sample",
    "n_plays",
    "n_pass_attempts",
    "n_rush_attempts",
    "n_completions",
    "n_epa_plays",
    "n_games",
    "n_teams",
    "pass_rate",
    "completion_pct",
    "epa_per_play",
    "points_per_game",
    "batch_a_build_run_ts",
    "batch_b_build_run_ts",
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


def require_columns(df: DataFrame, cols, label: str, errors):
    missing = [c for c in cols if c not in df.columns]
    if missing:
        errors.append(f"{label} missing columns: {missing}")


def duplicate_grain_count(df: DataFrame, grain_cols):
    return (
        df.groupBy(*grain_cols)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )


def range_violations(df: DataFrame, col_name: str, low: float, high: float):
    return (
        df.filter(
            F.col(col_name).isNotNull()
            & ((F.col(col_name) < F.lit(low)) | (F.col(col_name) > F.lit(high)))
        ).count()
    )


def base_offensive_plays(fact_raw: DataFrame) -> DataFrame:
    fact = fact_raw.toDF(*make_unique_columns(fact_raw.columns))
    season_source = F.col("season").cast("int")
    if "season__dup1" in fact.columns:
        season_source = F.coalesce(season_source, F.col("season__dup1").cast("int"))

    return (
        fact.withColumn("season", season_source)
        .withColumn("team", F.trim(F.col("posteam").cast("string")))
        .withColumn("pass_attempt_i", F.when(F.col("pass_attempt").cast("int") == 1, F.lit(1)).otherwise(F.lit(0)))
        .withColumn("rush_attempt_i", F.when(F.col("rush_attempt").cast("int") == 1, F.lit(1)).otherwise(F.lit(0)))
        .withColumn("weather_observed_flag", F.when(F.col("weather_observed_flag").cast("int") == 1, F.lit(1)).otherwise(F.lit(0)))
        .filter(
            F.col("game_id").isNotNull()
            & F.col("play_id").isNotNull()
            & F.col("team").isNotNull()
            & F.col("season").isNotNull()
            & ((F.col("pass_attempt_i") + F.col("rush_attempt_i")) > 0)
        )
        .select("season", "game_id", "play_id", "team", "pass_attempt_i", "rush_attempt_i", "weather_observed_flag")
    )


def check_mart_common(
    df: DataFrame,
    mart_name: str,
    grain_cols,
    errors,
    check_completion: bool,
    check_rush: bool,
):
    dup_cnt = duplicate_grain_count(df, grain_cols)
    if dup_cnt > 0:
        errors.append(f"{mart_name} duplicate grain rows: {dup_cnt}")

    pass_bad = range_violations(df, "pass_rate", 0.0, 1.0)
    if pass_bad > 0:
        errors.append(f"{mart_name} pass_rate out-of-range rows: {pass_bad}")

    if check_rush:
        rush_bad = range_violations(df, "rush_rate", 0.0, 1.0)
        if rush_bad > 0:
            errors.append(f"{mart_name} rush_rate out-of-range rows: {rush_bad}")

    if check_completion:
        comp_bad = range_violations(df, "completion_pct", 0.0, 1.0)
        if comp_bad > 0:
            errors.append(f"{mart_name} completion_pct out-of-range rows: {comp_bad}")
        comp_denom_bad = df.filter((F.col("n_pass_attempts") == 0) & F.col("completion_pct").isNotNull()).count()
        if comp_denom_bad > 0:
            errors.append(f"{mart_name} completion_pct populated with zero pass attempts rows: {comp_denom_bad}")

    pass_denom_bad = df.filter(((F.col("n_pass_attempts") + F.col("n_rush_attempts")) == 0) & F.col("pass_rate").isNotNull()).count()
    if pass_denom_bad > 0:
        errors.append(f"{mart_name} pass_rate populated with zero offensive attempts rows: {pass_denom_bad}")

    epa_denom_bad = df.filter((F.col("n_epa_plays") == 0) & F.col("epa_per_play").isNotNull()).count() if "n_epa_plays" in df.columns else 0
    if epa_denom_bad > 0:
        errors.append(f"{mart_name} epa_per_play populated with zero epa plays rows: {epa_denom_bad}")


def main():
    spark = build_spark("validate_gold_batch_b_v1")
    spark.sparkContext.setLogLevel("WARN")

    errors = []

    fact_raw = spark.read.parquet(FACT_IN)
    team = spark.read.parquet(TEAM_IN)
    playcall = spark.read.parquet(PLAYCALL_IN)
    indoor = spark.read.parquet(INDOOR_IN)

    team_qc_summary = spark.read.parquet(TEAM_QC_SUMMARY)
    playcall_qc_summary = spark.read.parquet(PLAYCALL_QC_SUMMARY)
    indoor_qc_summary = spark.read.parquet(INDOOR_QC_SUMMARY)

    require_columns(team, TEAM_REQUIRED, "team mart", errors)
    require_columns(playcall, PLAYCALL_REQUIRED, "playcalling mart", errors)
    require_columns(indoor, INDOOR_REQUIRED, "indoor/outdoor mart", errors)

    team_grain = ["season", "team", "temp_bin", "wind_bin", "precip_bin", "roof_env_bin", "weather_sample"]
    playcall_grain = ["season", "team", "temp_bin", "wind_bin", "precip_bin", "weather_sample"]
    indoor_grain = ["season", "roof_env_bin", "weather_sample"]

    check_mart_common(team, "team mart", team_grain, errors, check_completion=True, check_rush=False)
    check_mart_common(playcall, "playcalling mart", playcall_grain, errors, check_completion=False, check_rush=True)
    check_mart_common(indoor, "indoor/outdoor mart", indoor_grain, errors, check_completion=True, check_rush=False)

    # Shift definition correctness.
    shift_pass_bad = playcall.filter(
        F.abs(
            (F.col("pass_rate") - F.col("pass_rate_baseline"))
            - F.col("pass_rate_shift_vs_team_season")
        ) > F.lit(1e-9)
    ).count()
    if shift_pass_bad > 0:
        errors.append(f"playcalling mart pass shift formula mismatch rows: {shift_pass_bad}")

    shift_rush_bad = playcall.filter(
        F.abs(
            (F.col("rush_rate") - F.col("rush_rate_baseline"))
            - F.col("rush_rate_shift_vs_team_season")
        ) > F.lit(1e-9)
    ).count()
    if shift_rush_bad > 0:
        errors.append(f"playcalling mart rush shift formula mismatch rows: {shift_rush_bad}")

    # Source consistency checks.
    base = base_offensive_plays(fact_raw)
    expected_all = base.count()
    expected_obs = base.filter(F.col("weather_observed_flag") == 1).count()
    expected = {"ALL_PLAYS": expected_all, "OBSERVED_ONLY": expected_obs}

    team_totals = {r["weather_sample"]: r["n"] for r in team.groupBy("weather_sample").agg(F.sum("n_plays").cast("long").alias("n")).collect()}
    playcall_totals = {r["weather_sample"]: r["n"] for r in playcall.groupBy("weather_sample").agg(F.sum("n_plays").cast("long").alias("n")).collect()}
    indoor_totals = {r["weather_sample"]: r["n"] for r in indoor.groupBy("weather_sample").agg(F.sum("n_plays").cast("long").alias("n")).collect()}

    for sample in ["ALL_PLAYS", "OBSERVED_ONLY"]:
        exp = expected.get(sample, 0)
        t = team_totals.get(sample, 0)
        p = playcall_totals.get(sample, 0)
        i = indoor_totals.get(sample, 0)
        if t != exp:
            errors.append(f"team mart n_plays mismatch for {sample}: mart={t}, expected={exp}")
        if p != exp:
            errors.append(f"playcalling mart n_plays mismatch for {sample}: mart={p}, expected={exp}")
        if i != exp:
            errors.append(f"indoor mart n_plays mismatch for {sample}: mart={i}, expected={exp}")
        if not (t == p == i):
            errors.append(f"cross-mart n_plays mismatch for {sample}: team={t}, playcall={p}, indoor={i}")

    # QC summary consistency checks.
    team_cnt = team.count()
    playcall_cnt = playcall.count()
    indoor_cnt = indoor.count()
    team_qc_rows = team_qc_summary.select(F.col("n_rows").cast("long").alias("n")).collect()[0]["n"]
    playcall_qc_rows = playcall_qc_summary.select(F.col("n_rows").cast("long").alias("n")).collect()[0]["n"]
    indoor_qc_rows = indoor_qc_summary.select(F.col("n_rows").cast("long").alias("n")).collect()[0]["n"]

    if team_cnt != team_qc_rows:
        errors.append(f"team QC summary n_rows mismatch: mart={team_cnt}, qc={team_qc_rows}")
    if playcall_cnt != playcall_qc_rows:
        errors.append(f"playcalling QC summary n_rows mismatch: mart={playcall_cnt}, qc={playcall_qc_rows}")
    if indoor_cnt != indoor_qc_rows:
        errors.append(f"indoor QC summary n_rows mismatch: mart={indoor_cnt}, qc={indoor_qc_rows}")

    print("expected_all_plays=", expected_all)
    print("expected_observed_plays=", expected_obs)
    print("team_rows=", team_cnt)
    print("playcall_rows=", playcall_cnt)
    print("indoor_rows=", indoor_cnt)
    print("team_totals=", team_totals)
    print("playcall_totals=", playcall_totals)
    print("indoor_totals=", indoor_totals)

    if errors:
        print("VALIDATION_FAILED")
        for e in errors:
            print(" -", e)
        raise RuntimeError("Batch B validation failed; see errors above.")

    print("VALIDATION_PASSED")
    spark.stop()


if __name__ == "__main__":
    main()
