-- FitGuard-HDFS Apache Hive External Tables DDL
-- Exposes HDFS Silver, Quarantine, and Gold Parquet zones for SQL analytics.

CREATE DATABASE IF NOT EXISTS fitguard_hdfs
COMMENT 'FitGuard Medallion Data Quality & Personalized Protein Lakehouse on HDFS'
LOCATION 'hdfs://namenode:9000/fitguard';

USE fitguard_hdfs;

-- =========================================================================
-- SILVER ZONE (Validated, Deduplicated, Unit-Normalized Snappy Parquet)
-- =========================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS silver_user_profiles (
    user_id STRING,
    full_name STRING,
    age INT,
    weight_kg DOUBLE,
    fitness_goal STRING,
    diet_choice STRING,
    daily_food_budget_inr DOUBLE,
    location_city STRING,
    updated_at STRING
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/fitguard/silver/user_profiles';

CREATE EXTERNAL TABLE IF NOT EXISTS silver_protein_foods (
    listing_id STRING,
    food_name STRING,
    category STRING,
    diet_tag STRING,
    protein_per_100g DOUBLE,
    calories_per_100g DOUBLE,
    cost_per_100g_inr DOUBLE,
    protein_per_100_inr DOUBLE,
    cost_per_20g_protein_inr DOUBLE,
    vendor_name STRING,
    city STRING,
    in_stock BOOLEAN,
    scraped_at STRING
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/fitguard/silver/protein_foods_catalog';

CREATE EXTERNAL TABLE IF NOT EXISTS silver_wearable_telemetry (
    event_id STRING,
    user_id STRING,
    device_type STRING,
    event_ts_utc TIMESTAMP,
    event_date STRING,
    heart_rate_bpm INT,
    spo2_pct DOUBLE,
    steps INT,
    sleep_stage STRING,
    calories_burned DOUBLE,
    is_imputed BOOLEAN
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/fitguard/silver/wearable_telemetry';

-- =========================================================================
-- QUARANTINE ZONE (Dead-Letter Audit Tables with DQ Rule Lineage)
-- =========================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS quarantine_user_profiles (
    user_id STRING,
    full_name STRING,
    age INT,
    weight_kg DOUBLE,
    fitness_goal STRING,
    diet_choice STRING,
    daily_food_budget_inr DOUBLE,
    location_city STRING,
    updated_at STRING,
    canonical_city STRING,
    canonical_diet STRING,
    weight_kg_num DOUBLE,
    budget_num DOUBLE,
    updated_ts TIMESTAMP,
    dq_error_reason STRING,
    dq_rule_id STRING,
    dq_dimension STRING,
    source_dataset STRING
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/fitguard/quarantine/user_profiles';

CREATE EXTERNAL TABLE IF NOT EXISTS quarantine_protein_foods (
    listing_id STRING,
    food_name STRING,
    category STRING,
    diet_tag STRING,
    protein_g DOUBLE,
    calories_kcal DOUBLE,
    cost DOUBLE,
    currency STRING,
    unit_basis STRING,
    vendor_name STRING,
    city STRING,
    in_stock BOOLEAN,
    scraped_at STRING,
    canonical_city STRING,
    canonical_diet STRING,
    protein_num DOUBLE,
    calories_num DOUBLE,
    cost_num DOUBLE,
    scraped_ts TIMESTAMP,
    protein_per_100g DOUBLE,
    calories_per_100g DOUBLE,
    cost_per_100g_inr DOUBLE,
    dq_error_reason STRING,
    dq_rule_id STRING,
    dq_dimension STRING,
    source_dataset STRING
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/fitguard/quarantine/protein_foods_catalog';

CREATE EXTERNAL TABLE IF NOT EXISTS quarantine_wearable_telemetry (
    event_id STRING,
    user_id STRING,
    device_type STRING,
    event_ts STRING,
    dq_rule_id STRING,
    dq_dimension STRING,
    dq_error_reason STRING,
    source_dataset STRING,
    raw_payload STRING
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/fitguard/quarantine/wearable_telemetry';

-- =========================================================================
-- GOLD ZONE (Wellness Targets & Personalized Protein Recommendations)
-- =========================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS gold_protein_recommendations (
    user_id STRING,
    location_city STRING,
    user_diet_choice STRING,
    fitness_goal STRING,
    daily_food_budget_inr DOUBLE,
    daily_protein_target_g DOUBLE,
    rank INT,
    food_name STRING,
    food_diet_tag STRING,
    category STRING,
    vendor_name STRING,
    protein_per_100g DOUBLE,
    cost_per_100g_inr DOUBLE,
    protein_per_100_inr DOUBLE,
    recommended_portion_g DOUBLE,
    estimated_portion_protein_g DOUBLE,
    estimated_portion_cost_inr DOUBLE
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/fitguard/gold/protein_recommendations';

CREATE EXTERNAL TABLE IF NOT EXISTS gold_user_wellness_targets (
    user_id STRING,
    full_name STRING,
    age INT,
    weight_kg DOUBLE,
    fitness_goal STRING,
    diet_choice STRING,
    daily_food_budget_inr DOUBLE,
    location_city STRING,
    updated_at STRING,
    avg_daily_steps DOUBLE,
    avg_heart_rate DOUBLE,
    avg_spo2 DOUBLE,
    avg_daily_calories_burned DOUBLE,
    total_imputed_readings DOUBLE,
    daily_protein_target_g DOUBLE
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/fitguard/gold/user_wellness_targets';

CREATE EXTERNAL TABLE IF NOT EXISTS gold_dq_scorecard (
    dataset STRING,
    Completeness DOUBLE,
    Validity DOUBLE,
    Uniqueness DOUBLE,
    Consistency DOUBLE,
    Timeliness DOUBLE,
    Semantic_Integrity DOUBLE,
    Composite_Score DOUBLE,
    Pass_Rate_Pct DOUBLE,
    Total_Raw_Rows DOUBLE,
    Quarantined_Rows DOUBLE,
    Clean_Silver_Rows DOUBLE
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/fitguard/gold/dq_scorecard';

-- Example Analytical Queries for BDT Demo / Viva:
-- 1. Count critical semantic diet-tag lies blocked in Quarantine:
--    SELECT dq_rule_id, COUNT(*) AS blocked_rows FROM quarantine_protein_foods GROUP BY dq_rule_id;
-- 2. Best value Vegan protein foods in Bengaluru under Rs. 50 / 100g:
--    SELECT food_name, vendor_name, protein_per_100g, cost_per_100g_inr, protein_per_100_inr
--    FROM silver_protein_foods
--    WHERE city = 'Bengaluru' AND diet_tag = 'Vegan' AND cost_per_100g_inr <= 50
--    ORDER BY protein_per_100_inr DESC LIMIT 10;

