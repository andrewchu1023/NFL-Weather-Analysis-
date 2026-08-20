import datetime as dt
import json
import os
from pathlib import Path
from zoneinfo import ZoneInfo

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


BUCKET = "msba405-nfl-weather-raw"

PBP_IN = f"gs://{BUCKET}/silver/pbp_play_level_core/"
GAMES_IN = f"gs://{BUCKET}/silver/pbp_game_level/"
VENUE_IN = f"gs://{BUCKET}/silver/dim_venue/"
MAP_IN = f"gs://{BUCKET}/silver/venue_station_map_hourly/"
OBS_IN = f"gs://{BUCKET}/silver/noaa_hourly_observations/"
EXT_IN = f"gs://{BUCKET}/silver/noaa_hourly_features_extended/v1/"

FACT_OUT = f"gs://{BUCKET}/gold/fact_play_weather_hourly_v1/"
BINS_OUT = f"gs://{BUCKET}/gold/dim_weather_bins_v1/"

QC_BASE = f"gs://{BUCKET}/qc/gold/fact_play_weather_hourly_v1/"
QC_SUMMARY = QC_BASE + "summary/"
QC_COVERAGE_SEASON = QC_BASE + "coverage_by_season/"
QC_COVERAGE_STADIUM = QC_BASE + "coverage_by_stadium/"
QC_NULL_RATES = QC_BASE + "weather_null_rates/"


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
        # Avoid driver OOM from unintended large broadcast joins.
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


def try_parse_ts(col_name: str, fmt: str):
    safe_fmt = fmt.replace("'", "''")
    return F.expr(f"try_to_timestamp({col_name}, '{safe_fmt}')")


def ratio_null(col_name: str):
    return (
        F.sum(F.when(F.col(col_name).isNull(), F.lit(1)).otherwise(F.lit(0))).cast("double")
        / F.count(F.lit(1)).cast("double")
    )


def clamp_valid_utc_ts(ts_col):
    # Keep only plausible NFL-era timestamps to avoid sentinel parse drift (e.g., 12/31/69 -> 2069/2070).
    return F.when(
        ts_col.isNotNull()
        & (F.year(ts_col) >= F.lit(1990))
        & (F.year(ts_col) <= F.lit(2030)),
        ts_col,
    )


def main():
    spark = build_spark("build_gold_batch_a_v1")
    spark.sparkContext.setLogLevel("WARN")

    run_ts = dt.datetime.now(dt.UTC).strftime("%Y%m%dT%H%M%SZ")

    pbp = spark.read.parquet(PBP_IN)
    games = spark.read.parquet(GAMES_IN)
    venue = spark.read.parquet(VENUE_IN)
    map_hourly = spark.read.parquet(MAP_IN)
    obs = spark.read.parquet(OBS_IN)
    ext = spark.read.parquet(EXT_IN)

    # Hard gate: NOAA tables must be unique on station_key + obs_hour_utc.
    obs_dup_cnt = (
        obs.groupBy("station_key", "obs_hour_utc")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )
    ext_dup_cnt = (
        ext.groupBy("station_key", "obs_hour_utc")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )
    if obs_dup_cnt > 0 or ext_dup_cnt > 0:
        raise RuntimeError(f"NOAA key duplicates found: obs={obs_dup_cnt}, ext={ext_dup_cnt}")

    weather = (
        obs.alias("o")
        .join(ext.alias("e"), on=["station_key", "obs_hour_utc"], how="inner")
        .select(
            "station_key",
            "obs_hour_utc",
            F.col("o.obs_year").alias("obs_year"),
            F.col("o.obs_month").alias("obs_month"),
            F.col("o.source_dataset").alias("weather_source_dataset"),
            F.col("o.obs_present_flag").alias("weather_obs_present_flag"),
            F.col("o.air_temp_c"),
            F.col("o.dew_point_c"),
            F.col("o.sea_level_pressure_hpa"),
            F.col("o.wind_speed_mps"),
            F.col("o.wind_dir_deg"),
            F.col("e.relative_humidity_pct"),
            F.col("e.wet_bulb_temp_c"),
            F.col("e.apparent_temp_c"),
            F.col("e.wind_u_mps"),
            F.col("e.wind_v_mps"),
            F.col("e.pressure_delta_1h_hpa"),
            F.col("e.pressure_delta_3h_hpa"),
            F.col("e.pressure_delta_6h_hpa"),
            F.col("e.pressure_tendency_3h"),
            F.col("e.rh_valid_flag"),
            F.col("e.wet_bulb_valid_flag"),
            F.col("e.apparent_temp_valid_flag"),
            F.col("e.wind_components_valid_flag"),
            F.col("e.pressure_delta_valid_1h_flag"),
            F.col("e.pressure_delta_valid_3h_flag"),
            F.col("e.pressure_delta_valid_6h_flag"),
            F.col("e.feature_version"),
            F.col("e.source_build_run_ts"),
            F.col("e.feature_build_run_ts"),
        )
    )

    weather_dup_cnt = (
        weather.groupBy("station_key", "obs_hour_utc")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )
    if weather_dup_cnt > 0:
        raise RuntimeError(f"weather_wide duplicates found: {weather_dup_cnt}")

    game_cols = [
        "game_id",
        "home_score_final",
        "away_score_final",
        "total_points",
        "pass_rate",
        "completion_pct",
        "plays",
    ]
    games_sel = (
        games.select(*game_cols)
        .dropDuplicates(["game_id"])
        .withColumnRenamed("home_score_final", "game_home_score_final")
        .withColumnRenamed("away_score_final", "game_away_score_final")
        .withColumnRenamed("total_points", "game_total_points")
        .withColumnRenamed("pass_rate", "game_pass_rate")
        .withColumnRenamed("completion_pct", "game_completion_pct")
        .withColumnRenamed("plays", "game_total_plays")
    )

    map_sel = (
        map_hourly.select(
            "stadium_id",
            "station_key",
            "distance_km",
            "begin_year",
            "end_year",
            "selection_reason",
        )
        .dropDuplicates(["stadium_id", "begin_year", "end_year"])
    )

    venue_sel = (
        venue.select("stadium_id", "tz", "roof_type")
        .dropDuplicates(["stadium_id"])
        .withColumn(
            "venue_tz_norm",
            F.when(F.col("tz") == F.lit("Europe/Frankfurt"), F.lit("Europe/Berlin"))
            .when(F.col("tz") == F.lit("Europe/Munich"), F.lit("Europe/Berlin"))
            .otherwise(F.col("tz")),
        )
    )

    # Guard against non-IANA timezone ids that would crash to_utc_timestamp.
    tz_values = [
        r["venue_tz_norm"]
        for r in venue_sel.select("venue_tz_norm")
        .where(F.col("venue_tz_norm").isNotNull())
        .distinct()
        .collect()
    ]
    invalid_tz = []
    for tzv in tz_values:
        try:
            ZoneInfo(tzv)
        except Exception:
            invalid_tz.append(tzv)
    if invalid_tz:
        venue_sel = venue_sel.withColumn(
            "venue_tz_norm",
            F.when(F.col("venue_tz_norm").isin(invalid_tz), F.lit(None).cast("string")).otherwise(F.col("venue_tz_norm")),
        )

    p = pbp.withColumn("season_int", F.col("season").cast("int")).withColumn(
        "game_date_d", F.to_date(F.col("game_date"))
    )

    p = (
        p.alias("p")
        .join(
            map_sel.alias("m"),
            (F.col("p.stadium_id") == F.col("m.stadium_id"))
            & (F.col("p.season_int") >= F.col("m.begin_year"))
            & (F.col("p.season_int") <= F.col("m.end_year")),
            how="left",
        )
        .join(venue_sel.alias("v"), F.col("p.stadium_id") == F.col("v.stadium_id"), how="left")
        .join(games_sel.alias("g"), F.col("p.game_id") == F.col("g.game_id"), how="left")
        .select(
            F.col("p.*"),
            F.col("m.station_key").alias("station_key"),
            F.col("m.distance_km").alias("station_distance_km"),
            F.col("m.selection_reason").alias("station_selection_reason"),
            F.col("v.tz").alias("venue_tz"),
            F.col("v.venue_tz_norm").alias("venue_tz_norm"),
            F.col("v.roof_type").alias("venue_roof_type"),
            F.col("g.game_home_score_final"),
            F.col("g.game_away_score_final"),
            F.col("g.game_total_points"),
            F.col("g.game_pass_rate"),
            F.col("g.game_completion_pct"),
            F.col("g.game_total_plays"),
        )
    )

    p = (
        p.withColumn("station_mapped_flag", F.when(F.col("station_key").isNotNull(), F.lit(1)).otherwise(F.lit(0)))
        .withColumn("time_of_day_s", F.trim(F.col("time_of_day").cast("string")))
        .withColumn("start_time_s", F.trim(F.col("start_time").cast("string")))
    )

    # time_of_day parsing
    p = (
        p.withColumn("tod_utc_direct", F.coalesce(
            try_parse_ts("time_of_day_s", "yyyy-MM-dd'T'HH:mm:ssX"),
            try_parse_ts("time_of_day_s", "yyyy-MM-dd'T'HH:mm:ss.SSSX"),
        ))
        .withColumn("tod_local_ts", F.coalesce(
            try_parse_ts("time_of_day_s", "M/d/yy, H:mm:ss"),
            try_parse_ts("time_of_day_s", "M/d/yyyy, H:mm:ss"),
            try_parse_ts("time_of_day_s", "yyyy-MM-dd HH:mm:ss"),
            try_parse_ts("time_of_day_s", "yyyy-MM-dd'T'HH:mm:ss"),
        ))
        .withColumn("tod_hms", F.regexp_extract(F.col("time_of_day_s"), r"^(\d{1,2}:\d{2}:\d{2})$", 1))
        .withColumn(
            "tod_local_hms_str",
            F.when(
                (F.col("tod_hms") != "") & F.col("game_date_d").isNotNull(),
                F.concat_ws(" ", F.date_format(F.col("game_date_d"), "yyyy-MM-dd"), F.col("tod_hms")),
            ),
        )
        .withColumn("tod_local_hms_ts", try_parse_ts("tod_local_hms_str", "yyyy-MM-dd H:mm:ss"))
        .withColumn("tod_local_final", F.coalesce(F.col("tod_local_ts"), F.col("tod_local_hms_ts")))
        .withColumn(
            "tod_utc_from_local",
            F.when(
                F.col("tod_local_final").isNotNull() & F.col("venue_tz_norm").isNotNull(),
                F.expr("to_utc_timestamp(tod_local_final, venue_tz_norm)"),
            ),
        )
        .withColumn(
            "tod_utc",
            clamp_valid_utc_ts(F.coalesce(F.col("tod_utc_direct"), F.col("tod_utc_from_local"))),
        )
    )

    # start_time parsing fallback
    p = (
        p.withColumn("start_utc_direct", F.coalesce(
            try_parse_ts("start_time_s", "yyyy-MM-dd'T'HH:mm:ssX"),
            try_parse_ts("start_time_s", "yyyy-MM-dd'T'HH:mm:ss.SSSX"),
        ))
        .withColumn("start_local_ts", F.coalesce(
            try_parse_ts("start_time_s", "M/d/yy, H:mm:ss"),
            try_parse_ts("start_time_s", "M/d/yyyy, H:mm:ss"),
            try_parse_ts("start_time_s", "yyyy-MM-dd HH:mm:ss"),
            try_parse_ts("start_time_s", "yyyy-MM-dd'T'HH:mm:ss"),
        ))
        .withColumn("start_hms", F.regexp_extract(F.col("start_time_s"), r"^(\d{1,2}:\d{2}:\d{2})$", 1))
        .withColumn(
            "start_local_hms_str",
            F.when(
                (F.col("start_hms") != "") & F.col("game_date_d").isNotNull(),
                F.concat_ws(" ", F.date_format(F.col("game_date_d"), "yyyy-MM-dd"), F.col("start_hms")),
            ),
        )
        .withColumn("start_local_hms_ts", try_parse_ts("start_local_hms_str", "yyyy-MM-dd H:mm:ss"))
        .withColumn("start_local_final", F.coalesce(F.col("start_local_ts"), F.col("start_local_hms_ts")))
        .withColumn(
            "start_utc_from_local",
            F.when(
                F.col("start_local_final").isNotNull() & F.col("venue_tz_norm").isNotNull(),
                F.expr("to_utc_timestamp(start_local_final, venue_tz_norm)"),
            ),
        )
        .withColumn(
            "start_utc",
            clamp_valid_utc_ts(F.coalesce(F.col("start_utc_direct"), F.col("start_utc_from_local"))),
        )
    )

    p = (
        p.withColumn("play_ts_utc", F.coalesce(F.col("tod_utc"), F.col("start_utc")))
        .withColumn(
            "play_ts_source",
            F.when(F.col("tod_utc").isNotNull(), F.lit("time_of_day"))
            .when(F.col("start_utc").isNotNull(), F.lit("start_time"))
            .otherwise(F.lit("none")),
        )
        .withColumn("play_time_parsed_flag", F.when(F.col("play_ts_utc").isNotNull(), F.lit(1)).otherwise(F.lit(0)))
        .withColumn("play_hour_utc", F.date_trunc("hour", F.col("play_ts_utc")))
    )

    fact = (
        p.alias("p")
        .join(
            weather.alias("w"),
            (F.col("p.station_key") == F.col("w.station_key"))
            & (F.col("p.play_hour_utc") == F.col("w.obs_hour_utc")),
            how="left",
        )
        .select(
            F.col("p.*"),
            F.col("w.obs_hour_utc").alias("weather_hour_utc"),
            F.col("w.weather_source_dataset"),
            F.col("w.weather_obs_present_flag"),
            F.col("w.air_temp_c"),
            F.col("w.dew_point_c"),
            F.col("w.sea_level_pressure_hpa"),
            F.col("w.wind_speed_mps"),
            F.col("w.wind_dir_deg"),
            F.col("w.relative_humidity_pct"),
            F.col("w.wet_bulb_temp_c"),
            F.col("w.apparent_temp_c"),
            F.col("w.wind_u_mps"),
            F.col("w.wind_v_mps"),
            F.col("w.pressure_delta_1h_hpa"),
            F.col("w.pressure_delta_3h_hpa"),
            F.col("w.pressure_delta_6h_hpa"),
            F.col("w.pressure_tendency_3h"),
            F.col("w.rh_valid_flag"),
            F.col("w.wet_bulb_valid_flag"),
            F.col("w.apparent_temp_valid_flag"),
            F.col("w.wind_components_valid_flag"),
            F.col("w.pressure_delta_valid_1h_flag"),
            F.col("w.pressure_delta_valid_3h_flag"),
            F.col("w.pressure_delta_valid_6h_flag"),
            F.col("w.feature_version").alias("weather_feature_version"),
            F.col("w.source_build_run_ts").alias("weather_source_build_run_ts"),
            F.col("w.feature_build_run_ts").alias("weather_feature_build_run_ts"),
        )
        .withColumn("weather_key_match_flag", F.when(F.col("weather_hour_utc").isNotNull(), F.lit(1)).otherwise(F.lit(0)))
        .withColumn("weather_observed_flag", F.coalesce(F.col("weather_obs_present_flag"), F.lit(0)))
        .withColumn(
            "weather_join_tier",
            F.when(F.col("station_key").isNull(), F.lit("no_station_mapping"))
            .when(F.col("play_ts_utc").isNull(), F.lit("no_play_time"))
            .when(F.col("weather_hour_utc").isNull(), F.lit("no_weather_hour_key"))
            .when(F.col("weather_obs_present_flag") == F.lit(0), F.lit("weather_hour_no_observation"))
            .when(F.col("play_ts_source") == F.lit("time_of_day"), F.lit("direct_play_hour"))
            .when(F.col("play_ts_source") == F.lit("start_time"), F.lit("kickoff_fallback"))
            .otherwise(F.lit("matched_observed")),
        )
    )

    # Stable weather bins for downstream dashboard aggregates.
    fact = (
        fact.withColumn(
            "temp_bin",
            F.when(F.col("air_temp_c").isNull(), F.lit("UNKNOWN"))
            .when(F.col("air_temp_c") < F.lit(0.0), F.lit("FREEZING_LT_0C"))
            .when(F.col("air_temp_c") < F.lit(10.0), F.lit("COLD_0_10C"))
            .when(F.col("air_temp_c") < F.lit(20.0), F.lit("COOL_10_20C"))
            .when(F.col("air_temp_c") < F.lit(30.0), F.lit("MILD_20_30C"))
            .otherwise(F.lit("HOT_GE_30C")),
        )
        .withColumn(
            "wind_bin",
            F.when(F.col("wind_speed_mps").isNull(), F.lit("UNKNOWN"))
            .when(F.col("wind_speed_mps") < F.lit(3.0), F.lit("CALM_LT_3_MPS"))
            .when(F.col("wind_speed_mps") < F.lit(6.0), F.lit("BREEZY_3_6_MPS"))
            .when(F.col("wind_speed_mps") < F.lit(10.0), F.lit("WINDY_6_10_MPS"))
            .otherwise(F.lit("STRONG_GE_10_MPS")),
        )
        .withColumn("precip_bin", F.lit("UNKNOWN"))
        .withColumn(
            "roof_env_bin",
            F.when(F.col("venue_roof_type").isNull(), F.lit("UNKNOWN"))
            .when(F.lower(F.col("venue_roof_type")).contains("retract"), F.lit("RETRACTABLE"))
            .when(
                F.lower(F.col("venue_roof_type")).isin("dome", "indoors", "indoor", "closed"),
                F.lit("INDOOR"),
            )
            .when(
                F.lower(F.col("venue_roof_type")).isin("outdoors", "outdoor", "open"),
                F.lit("OUTDOOR"),
            )
            .otherwise(F.lit("UNKNOWN")),
        )
        .withColumn("batch_a_build_run_ts", F.lit(run_ts))
    )

    pbp_cnt = pbp.count()
    fact_cnt = fact.count()
    dup_cnt = (
        fact.groupBy("game_id", "play_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )
    null_pk_cnt = fact.filter(F.col("game_id").isNull() | F.col("play_id").isNull()).count()
    null_bin_cnt = fact.filter(
        F.col("temp_bin").isNull()
        | F.col("wind_bin").isNull()
        | F.col("precip_bin").isNull()
        | F.col("roof_env_bin").isNull()
    ).count()

    if fact_cnt != pbp_cnt:
        raise RuntimeError(f"Row count mismatch: pbp={pbp_cnt}, fact={fact_cnt}")
    if dup_cnt > 0:
        raise RuntimeError(f"Found duplicate game_id+play_id in fact: {dup_cnt}")
    if null_pk_cnt > 0:
        raise RuntimeError(f"Found null keys in fact: {null_pk_cnt}")
    if null_bin_cnt > 0:
        raise RuntimeError(f"Found null weather bins: {null_bin_cnt}")

    summary = fact.agg(
        F.count(F.lit(1)).cast("long").alias("n_rows"),
        F.countDistinct("game_id").cast("long").alias("n_games"),
        F.countDistinct("season").cast("long").alias("n_seasons"),
        F.sum(F.col("station_mapped_flag")).cast("long").alias("n_station_mapped_rows"),
        F.sum(F.col("play_time_parsed_flag")).cast("long").alias("n_play_time_parsed_rows"),
        F.sum(F.col("weather_key_match_flag")).cast("long").alias("n_weather_key_matched_rows"),
        F.sum(F.col("weather_observed_flag")).cast("long").alias("n_weather_observed_rows"),
        (F.sum(F.col("station_mapped_flag")).cast("double") / F.count(F.lit(1)).cast("double")).alias("rate_station_mapped"),
        (F.sum(F.col("play_time_parsed_flag")).cast("double") / F.count(F.lit(1)).cast("double")).alias("rate_play_time_parsed"),
        (F.sum(F.col("weather_key_match_flag")).cast("double") / F.count(F.lit(1)).cast("double")).alias("rate_weather_key_matched"),
        (F.sum(F.col("weather_observed_flag")).cast("double") / F.count(F.lit(1)).cast("double")).alias("rate_weather_observed"),
        F.min("play_hour_utc").alias("min_play_hour_utc"),
        F.max("play_hour_utc").alias("max_play_hour_utc"),
    ).withColumn("batch_a_build_run_ts", F.lit(run_ts))

    coverage_season = (
        fact.groupBy("season")
        .agg(
            F.count(F.lit(1)).cast("long").alias("n_rows"),
            F.sum(F.col("station_mapped_flag")).cast("long").alias("n_station_mapped_rows"),
            F.sum(F.col("play_time_parsed_flag")).cast("long").alias("n_play_time_parsed_rows"),
            F.sum(F.col("weather_key_match_flag")).cast("long").alias("n_weather_key_matched_rows"),
            F.sum(F.col("weather_observed_flag")).cast("long").alias("n_weather_observed_rows"),
            (F.sum(F.col("weather_observed_flag")).cast("double") / F.count(F.lit(1)).cast("double")).alias("rate_weather_observed"),
        )
        .withColumn("batch_a_build_run_ts", F.lit(run_ts))
        .orderBy("season")
    )

    coverage_stadium = (
        fact.groupBy("stadium_id")
        .agg(
            F.count(F.lit(1)).cast("long").alias("n_rows"),
            F.sum(F.col("station_mapped_flag")).cast("long").alias("n_station_mapped_rows"),
            F.sum(F.col("weather_observed_flag")).cast("long").alias("n_weather_observed_rows"),
            (F.sum(F.col("weather_observed_flag")).cast("double") / F.count(F.lit(1)).cast("double")).alias("rate_weather_observed"),
        )
        .withColumn("batch_a_build_run_ts", F.lit(run_ts))
        .orderBy(F.desc("n_rows"), F.asc("stadium_id"))
    )

    null_rates = fact.agg(
        ratio_null("air_temp_c").alias("null_rate_air_temp_c"),
        ratio_null("dew_point_c").alias("null_rate_dew_point_c"),
        ratio_null("wind_speed_mps").alias("null_rate_wind_speed_mps"),
        ratio_null("wind_dir_deg").alias("null_rate_wind_dir_deg"),
        ratio_null("relative_humidity_pct").alias("null_rate_relative_humidity_pct"),
        ratio_null("wet_bulb_temp_c").alias("null_rate_wet_bulb_temp_c"),
        ratio_null("apparent_temp_c").alias("null_rate_apparent_temp_c"),
        ratio_null("pressure_delta_1h_hpa").alias("null_rate_pressure_delta_1h_hpa"),
        ratio_null("pressure_delta_3h_hpa").alias("null_rate_pressure_delta_3h_hpa"),
        ratio_null("pressure_delta_6h_hpa").alias("null_rate_pressure_delta_6h_hpa"),
    ).withColumn("batch_a_build_run_ts", F.lit(run_ts))

    bins_rows = [
        ("temp_bin", "UNKNOWN", "Unknown", None, None, 0),
        ("temp_bin", "FREEZING_LT_0C", "< 0C", None, 0.0, 1),
        ("temp_bin", "COLD_0_10C", "0-10C", 0.0, 10.0, 2),
        ("temp_bin", "COOL_10_20C", "10-20C", 10.0, 20.0, 3),
        ("temp_bin", "MILD_20_30C", "20-30C", 20.0, 30.0, 4),
        ("temp_bin", "HOT_GE_30C", ">= 30C", 30.0, None, 5),
        ("wind_bin", "UNKNOWN", "Unknown", None, None, 0),
        ("wind_bin", "CALM_LT_3_MPS", "< 3 m/s", None, 3.0, 1),
        ("wind_bin", "BREEZY_3_6_MPS", "3-6 m/s", 3.0, 6.0, 2),
        ("wind_bin", "WINDY_6_10_MPS", "6-10 m/s", 6.0, 10.0, 3),
        ("wind_bin", "STRONG_GE_10_MPS", ">= 10 m/s", 10.0, None, 4),
        ("precip_bin", "UNKNOWN", "Unknown", None, None, 0),
        ("roof_env_bin", "UNKNOWN", "Unknown", None, None, 0),
        ("roof_env_bin", "INDOOR", "Indoor", None, None, 1),
        ("roof_env_bin", "OUTDOOR", "Outdoor", None, None, 2),
        ("roof_env_bin", "RETRACTABLE", "Retractable", None, None, 3),
    ]
    bins_schema = [
        "bin_type",
        "bin_value",
        "bin_label",
        "lower_inclusive",
        "upper_exclusive",
        "sort_order",
    ]
    bins_df = spark.createDataFrame(bins_rows, bins_schema).withColumn("batch_a_build_run_ts", F.lit(run_ts))

    fact.write.mode("overwrite").partitionBy("season").parquet(FACT_OUT)
    bins_df.write.mode("overwrite").parquet(BINS_OUT)
    summary.write.mode("overwrite").parquet(QC_SUMMARY)
    coverage_season.write.mode("overwrite").parquet(QC_COVERAGE_SEASON)
    coverage_stadium.write.mode("overwrite").parquet(QC_COVERAGE_STADIUM)
    null_rates.write.mode("overwrite").parquet(QC_NULL_RATES)

    print("DONE")
    print(f"FACT_OUT={FACT_OUT}")
    print(f"BINS_OUT={BINS_OUT}")
    print(f"QC_SUMMARY={QC_SUMMARY}")
    summary.show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
