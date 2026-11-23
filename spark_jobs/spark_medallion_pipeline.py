"""FitGuard-HDFS Distributed PySpark Medallion Pipeline.

Runs on Apache Spark (YARN / Spark Standalone / Local) against Hadoop HDFS:
  hdfs://namenode:9000/fitguard/{bronze,silver,quarantine,gold}

Implements:
  - 6-Dimension Automated Data Quality Engine
  - Semantic Diet-Tag Guard (prevents recommending Non-Veg/Veg items to Vegan users)
  - Unit & Currency Normalization (per_kg / per_500g / USD -> per_100g_inr)
  - City Canonicalization & Cross-Signal Wearable Validation (DEEP_SLEEP vs steps)
  - Snappy Parquet Compaction & Partitioning on HDFS
  - Gold Personalized Protein Food Recommendations (Choice + Cost + Location)
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, StringType

ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR))

from spark_jobs.dq_rules import (  # noqa: E402
    UNIT_TO_100G_FACTOR,
    canonicalize_city,
    canonicalize_diet,
    is_diet_tag_semantically_valid,
    load_aliases,
    load_config,
)


def build_spark_session(app_name: str, namenode_uri: str | None) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.adaptive.enabled", "true")
    )
    if namenode_uri:
        builder = builder.config("spark.hadoop.fs.defaultFS", namenode_uri)
    return builder.getOrCreate()


def main() -> None:
    parser = argparse.ArgumentParser(description="FitGuard-HDFS PySpark Medallion Pipeline")
    parser.add_argument(
        "--base-path",
        default="hdfs://namenode:9000/fitguard",
        help="Base HDFS or local URI (e.g. hdfs://namenode:9000/fitguard or lakehouse/fitguard)",
    )
    parser.add_argument("--namenode-uri", default="hdfs://namenode:9000", help="Hadoop fs.defaultFS URI")
    args = parser.parse_args()

    cfg = load_config()
    aliases = load_aliases()
    thresh = cfg["dq_thresholds"]
    city_map = aliases["aliases"]
    diet_map = aliases["diet_aliases"]

    namenode = args.namenode_uri if args.base_path.startswith("hdfs://") else None
    spark = build_spark_session("FitGuard-HDFS-Medallion-DQ", namenode)
    spark.sparkContext.setLogLevel("WARN")

    city_udf = F.udf(lambda c: canonicalize_city(c, city_map), StringType())
    diet_udf = F.udf(lambda d: canonicalize_diet(d, diet_map), StringType())
    sem_valid_udf = F.udf(is_diet_tag_semantically_valid, BooleanType())

    base = args.base_path.rstrip("/")
    bronze = f"{base}/bronze"
    silver = f"{base}/silver"
    quarantine = f"{base}/quarantine"
    gold = f"{base}/gold"

    # ------------------------------------------------------------------
    # 1. CLEAN USER PROFILES (Choice, Daily Budget, Location, Weight)
    # ------------------------------------------------------------------
    raw_users = spark.read.option("header", True).option("inferSchema", True).csv(f"{bronze}/user_profiles")
    u_norm = (
        raw_users.withColumn("canonical_city", city_udf(F.col("location_city")))
        .withColumn("canonical_diet", diet_udf(F.col("diet_choice")))
        .withColumn("weight_num", F.col("weight_kg").cast("double"))
        .withColumn("budget_num", F.col("daily_food_budget_inr").cast("double"))
    )
    u_win = Window.partitionBy("user_id").orderBy(F.col("updated_at").desc_nulls_last())
    u_ranked = u_norm.withColumn("rn", F.when(F.col("user_id").isNotNull(), F.row_number().over(u_win)).otherwise(1))

    u_flagged = u_ranked.withColumn(
        "dq_rule_id",
        F.when(F.col("user_id").isNull() | (F.trim(F.col("user_id")) == ""), F.lit("DQ-CMP-001"))
        .when(F.col("rn") > 1, F.lit("DQ-UNQ-001"))
        .when(F.col("canonical_diet").isNull(), F.lit("DQ-CMP-002"))
        .when(F.col("canonical_city").isNull(), F.lit("DQ-LOC-001"))
        .when(
            F.col("weight_num").isNull()
            | (F.col("weight_num") < thresh["min_weight_kg"])
            | (F.col("weight_num") > thresh["max_weight_kg"]),
            F.lit("DQ-PHY-003"),
        )
        .when(
            F.col("budget_num").isNull()
            | (F.col("budget_num") < thresh["min_budget_inr"])
            | (F.col("budget_num") > thresh["max_budget_inr"]),
            F.lit("DQ-VAL-002"),
        )
        .otherwise(F.lit("")),
    )

    quarantine_users = u_flagged.filter(F.col("dq_rule_id") != "")
    silver_users = (
        u_flagged.filter(F.col("dq_rule_id") == "")
        .drop("location_city", "diet_choice", "weight_kg", "daily_food_budget_inr", "rn", "dq_rule_id")
        .withColumnRenamed("canonical_city", "location_city")
        .withColumnRenamed("canonical_diet", "diet_choice")
        .withColumnRenamed("weight_num", "weight_kg")
        .withColumnRenamed("budget_num", "daily_food_budget_inr")
    )

    silver_users.coalesce(1).write.mode("overwrite").parquet(f"{silver}/user_profiles")
    quarantine_users.coalesce(1).write.mode("overwrite").parquet(f"{quarantine}/user_profiles")

    # ------------------------------------------------------------------
    # 2. CLEAN PROTEIN FOODS CATALOG (Semantic Guard + Unit/FX Norm)
    # ------------------------------------------------------------------
    raw_foods = spark.read.option("header", True).option("inferSchema", True).csv(f"{bronze}/protein_foods_catalog")
    unit_map_expr = F.create_map([F.lit(x) for kv in UNIT_TO_100G_FACTOR.items() for x in kv])

    f_norm = (
        raw_foods.withColumn("canonical_city", city_udf(F.col("city")))
        .withColumn("canonical_diet", diet_udf(F.col("diet_tag")))
        .withColumn("unit_factor", unit_map_expr[F.lower(F.col("unit_basis"))])
        .withColumn(
            "fx_mult",
            F.when(F.upper(F.col("currency")) == "USD", F.lit(float(thresh["usd_to_inr"]))).otherwise(F.lit(1.0)),
        )
        .withColumn("protein_per_100g", F.round(F.col("protein_g").cast("double") / F.col("unit_factor"), 2))
        .withColumn("calories_per_100g", F.round(F.col("calories_kcal").cast("double") / F.col("unit_factor"), 1))
        .withColumn(
            "cost_per_100g_inr",
            F.round((F.col("cost").cast("double") * F.col("fx_mult")) / F.col("unit_factor"), 2),
        )
        .withColumn("scraped_ts", F.to_timestamp(F.col("scraped_at")))
    )

    f_win = Window.partitionBy("food_name", "canonical_city", "vendor_name", "unit_basis").orderBy(F.col("listing_id"))
    f_ranked = f_norm.withColumn("rn", F.row_number().over(f_win))

    f_flagged = f_ranked.withColumn(
        "dq_rule_id",
        F.when(F.col("food_name").isNull() | F.col("protein_g").isNull() | F.col("calories_kcal").isNull(), F.lit("DQ-CMP-001"))
        .when(F.col("unit_factor").isNull(), F.lit("DQ-SCH-002"))
        .when(F.col("canonical_city").isNull(), F.lit("DQ-LOC-001"))
        .when(~sem_valid_udf(F.col("food_name"), F.col("canonical_diet")), F.lit("DQ-SEM-001"))
        .when(F.col("rn") > 1, F.lit("DQ-UNQ-001"))
        .when((F.col("protein_per_100g") < 0.5) | (F.col("protein_per_100g") > thresh["max_protein_per_100g"]), F.lit("DQ-PHY-002"))
        .when(F.col("calories_per_100g") < F.col("protein_per_100g") * 3.6, F.lit("DQ-PHY-002"))
        .when(
            F.col("cost_per_100g_inr").isNull()
            | (F.col("cost_per_100g_inr") < 1.0)
            | (F.col("cost_per_100g_inr") > thresh["max_cost_per_100g_inr"]),
            F.lit("DQ-VAL-002"),
        )
        .when(F.col("scraped_ts").isNull() | (F.datediff(F.current_timestamp(), F.col("scraped_ts")) > 60), F.lit("DQ-TIM-001"))
        .otherwise(F.lit("")),
    )

    quarantine_foods = f_flagged.filter(F.col("dq_rule_id") != "")
    silver_foods = (
        f_flagged.filter(F.col("dq_rule_id") == "")
        .withColumn("protein_per_100_inr", F.round((F.col("protein_per_100g") / F.col("cost_per_100g_inr")) * 100.0, 2))
        .withColumn("cost_per_20g_protein_inr", F.round((20.0 / F.col("protein_per_100g")) * F.col("cost_per_100g_inr"), 2))
        .select(
            "listing_id",
            "food_name",
            "category",
            F.col("canonical_diet").alias("diet_tag"),
            "protein_per_100g",
            "calories_per_100g",
            "cost_per_100g_inr",
            "protein_per_100_inr",
            "cost_per_20g_protein_inr",
            "vendor_name",
            F.col("canonical_city").alias("city"),
            "in_stock",
            "scraped_at",
        )
    )

    silver_foods.coalesce(1).write.mode("overwrite").parquet(f"{silver}/protein_foods_catalog")
    quarantine_foods.coalesce(1).write.mode("overwrite").parquet(f"{quarantine}/protein_foods_catalog")

    # ------------------------------------------------------------------
    # 3. CLEAN WEARABLE TELEMETRY (Small-Files Compaction + Cross-Signal)
    # ------------------------------------------------------------------
    raw_tel = spark.read.option("mode", "PERMISSIVE").option("columnNameOfCorruptRecord", "_corrupt_record").json(
        f"{bronze}/wearable_telemetry"
    )
    corrupt_tel = raw_tel.filter(F.col("_corrupt_record").isNotNull()).withColumn("dq_rule_id", F.lit("DQ-SCH-001"))
    valid_json_tel = raw_tel.filter(F.col("_corrupt_record").isNull())

    tel_parsed = valid_json_tel.withColumn(
        "event_ts_utc",
        F.when(
            F.col("event_ts").cast("long").isNotNull(),
            (F.col("event_ts").cast("long") / 1000.0).cast("timestamp"),
        ).otherwise(F.to_timestamp(F.col("event_ts"))),
    )

    # Smart Imputation: fill isolated missing heart_rate_bpm via user median
    user_hr_median = tel_parsed.groupBy("user_id").agg(F.percentile_approx("heart_rate_bpm", 0.5).alias("med_hr"))
    tel_imputed = (
        tel_parsed.join(user_hr_median, on="user_id", how="left")
        .withColumn("is_imputed", F.col("heart_rate_bpm").isNull() & F.col("user_id").isNotNull())
        .withColumn("hr_final", F.coalesce(F.col("heart_rate_bpm"), F.col("med_hr")).cast("int"))
    )

    tel_win = Window.partitionBy("user_id", "event_ts_utc").orderBy(F.col("event_id"))
    tel_ranked = tel_imputed.withColumn("rn", F.row_number().over(tel_win))

    tel_flagged = tel_ranked.withColumn(
        "dq_rule_id",
        F.when(F.col("user_id").isNull() | F.col("event_ts_utc").isNull(), F.lit("DQ-CMP-001"))
        .when(
            (F.col("event_ts_utc") > F.current_timestamp() + F.expr("INTERVAL 1 HOUR"))
            | (F.datediff(F.current_timestamp(), F.col("event_ts_utc")) > 120),
            F.lit("DQ-TIM-001"),
        )
        .when(F.col("rn") > 1, F.lit("DQ-UNQ-001"))
        .when((F.col("sleep_stage") == "DEEP_SLEEP") & (F.col("steps") > 300), F.lit("DQ-XSIG-001"))
        .when(
            F.col("hr_final").isNull()
            | (F.col("hr_final") < thresh["heart_rate_min"])
            | (F.col("hr_final") > thresh["heart_rate_max"]),
            F.lit("DQ-PHY-001"),
        )
        .when((F.col("spo2_pct") < thresh["spo2_min"]) | (F.col("spo2_pct") > thresh["spo2_max"]), F.lit("DQ-PHY-001"))
        .when((F.col("steps") < 0) | (F.col("steps") > thresh["max_steps_per_hour"]), F.lit("DQ-PHY-001"))
        .otherwise(F.lit("")),
    )

    quarantine_tel = tel_flagged.filter(F.col("dq_rule_id") != "").select("event_id", "user_id", "device_type", "dq_rule_id")
    quarantine_tel_all = quarantine_tel.unionByName(
        corrupt_tel.select(
            F.lit(None).cast("string").alias("event_id"),
            F.lit(None).cast("string").alias("user_id"),
            F.lit(None).cast("string").alias("device_type"),
            "dq_rule_id",
        ),
        allowMissingColumns=True,
    )

    silver_tel = (
        tel_flagged.filter(F.col("dq_rule_id") == "")
        .withColumn("event_date", F.date_format("event_ts_utc", "yyyy-MM-dd"))
        .select(
            "event_id",
            "user_id",
            "device_type",
            "event_ts_utc",
            "event_date",
            F.col("hr_final").alias("heart_rate_bpm"),
            "spo2_pct",
            "steps",
            "sleep_stage",
            "calories_burned",
            "is_imputed",
        )
    )

    # Compacted & Partitioned Parquet on HDFS
    silver_tel.coalesce(2).write.mode("overwrite").parquet(f"{silver}/wearable_telemetry")
    quarantine_tel_all.coalesce(1).write.mode("overwrite").parquet(f"{quarantine}/wearable_telemetry")

    # ------------------------------------------------------------------
    # 4. GOLD LAYER: ACTIVITY-ADJUSTED TARGETS & PROTEIN RECOMMENDATIONS
    # ------------------------------------------------------------------
    daily_act = silver_tel.groupBy("user_id", "event_date").agg(
        F.sum("steps").alias("daily_steps"),
        F.avg("heart_rate_bpm").alias("avg_hr"),
        F.avg("spo2_pct").alias("avg_spo2"),
        F.sum("calories_burned").alias("daily_cal"),
    )
    user_act = daily_act.groupBy("user_id").agg(
        F.round(F.avg("daily_steps"), 0).alias("avg_daily_steps"),
        F.round(F.avg("avg_hr"), 1).alias("avg_heart_rate"),
        F.round(F.avg("avg_spo2"), 1).alias("avg_spo2"),
        F.round(F.avg("daily_cal"), 1).alias("avg_daily_calories_burned"),
    )

    goal_map_expr = F.create_map([F.lit(x) for kv in cfg["protein_target_g_per_kg"].items() for x in kv])
    user_targets = (
        silver_users.join(user_act, on="user_id", how="left")
        .fillna({"avg_daily_steps": 6500.0})
        .withColumn("goal_factor", F.coalesce(goal_map_expr[F.col("fitness_goal")], F.lit(1.2)))
        .withColumn("activity_bonus", F.when(F.col("avg_daily_steps") >= 9500, F.lit(0.15)).otherwise(F.lit(0.0)))
        .withColumn("daily_protein_target_g", F.round(F.col("weight_kg") * (F.col("goal_factor") + F.col("activity_bonus")), 1))
    )
    user_targets.coalesce(1).write.mode("overwrite").parquet(f"{gold}/user_wellness_targets")

    # Diet compatibility join + Location + Budget filter -> Top 5 foods per user
    diet_pairs = [(u_diet, f_diet) for u_diet, allowed in cfg["diet_hierarchy"].items() for f_diet in allowed]
    diet_compat_df: DataFrame = spark.createDataFrame(diet_pairs, ["diet_choice", "diet_tag"])

    recs_joined = (
        user_targets.join(diet_compat_df, on="diet_choice", how="inner")
        .join(
            silver_foods.filter(F.col("in_stock") == True),  # noqa: E712
            on=[
                user_targets["location_city"] == silver_foods["city"],
                diet_compat_df["diet_tag"] == silver_foods["diet_tag"],
                silver_foods["cost_per_100g_inr"] <= user_targets["daily_food_budget_inr"] * 0.70,
            ],
            how="inner",
        )
    )
    rec_win = Window.partitionBy("user_id", "food_name").orderBy(F.col("protein_per_100_inr").desc())
    best_per_food = recs_joined.withColumn("food_rn", F.row_number().over(rec_win)).filter(F.col("food_rn") == 1)

    top5_win = Window.partitionBy("user_id").orderBy(F.col("protein_per_100_inr").desc(), F.col("protein_per_100g").desc())
    gold_recs = (
        best_per_food.withColumn("rank", F.row_number().over(top5_win))
        .filter(F.col("rank") <= 5)
        .select(
            "user_id",
            "location_city",
            F.col("diet_choice").alias("user_diet_choice"),
            "fitness_goal",
            "daily_food_budget_inr",
            "daily_protein_target_g",
            "rank",
            "food_name",
            silver_foods["diet_tag"].alias("food_diet_tag"),
            "category",
            "vendor_name",
            "protein_per_100g",
            "cost_per_100g_inr",
            "protein_per_100_inr",
        )
    )
    gold_recs.coalesce(1).write.mode("overwrite").parquet(f"{gold}/protein_recommendations")

    print("[FitGuard-HDFS PySpark] Medallion pipeline succeeded.")
    spark.stop()


if __name__ == "__main__":
    main()
