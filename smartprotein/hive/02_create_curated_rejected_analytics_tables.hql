-- ============================================================================
-- SmartProtein Hive DDL Part 2: Curated, Rejected & Analytics External Tables
-- Uses Columnar Parquet (Snappy) + ORC Benchmark Copy Partitioned by Region/Category
-- ============================================================================

-- ----------------------------------------------------------------------------
-- ZONE 3: CURATED TABLES (Trusted Source of Truth)
-- ----------------------------------------------------------------------------
USE smartprotein_curated;

CREATE EXTERNAL TABLE IF NOT EXISTS curated_protein_foods (
    canonical_food_id           STRING,
    canonical_name              STRING,
    diet_type                   STRING,
    state_raw_cooked            STRING,
    crude_protein_per_100g      DOUBLE,
    usable_protein_per_100g     DOUBLE,   -- DIAAS/PDCAAS + cooking + bioavailability adjusted
    diaas_score                 DOUBLE,
    bioavailability_factor      DOUBLE,
    cooking_retention_factor    DOUBLE,
    carbs_per_100g              DOUBLE,
    fat_per_100g                DOUBLE,
    fiber_per_100g              DOUBLE,
    kcal_per_100g               DOUBLE,
    protein_per_100_kcal        DOUBLE,
    leucine_g_per_100g_food     DOUBLE,
    iron_mg_per_100g            DOUBLE,
    calcium_mg_per_100g         DOUBLE,
    b12_mcg_per_100g            DOUBLE,
    allergens                   STRING,
    source_count                INT,
    protein_confidence_score    DOUBLE,   -- 0-100 source agreement confidence
    record_dq_score             DOUBLE    -- 0-100 record-level data quality score
)
PARTITIONED BY (category STRING)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/smartprotein/curated/protein_quality_enriched'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- ORC replica for Stage 12 Storage Format Benchmark (CSV vs Parquet vs ORC)
CREATE EXTERNAL TABLE IF NOT EXISTS curated_protein_foods_orc (
    canonical_food_id           STRING,
    canonical_name              STRING,
    diet_type                   STRING,
    crude_protein_per_100g      DOUBLE,
    usable_protein_per_100g     DOUBLE,
    kcal_per_100g               DOUBLE,
    protein_confidence_score    DOUBLE,
    record_dq_score             DOUBLE
)
PARTITIONED BY (category STRING)
STORED AS ORC
LOCATION 'hdfs://namenode:9000/smartprotein/curated/protein_quality_enriched_orc'
TBLPROPERTIES ('orc.compress'='SNAPPY');

CREATE EXTERNAL TABLE IF NOT EXISTS curated_store_inventory (
    offer_id                    STRING,
    store_id                    STRING,
    store_name                  STRING,
    city                        STRING,
    latitude                    DOUBLE,
    longitude                   DOUBLE,
    geohash_6                   STRING,
    canonical_food_id           STRING,
    canonical_name              STRING,
    price_per_100g_inr          DOUBLE,
    in_stock                    BOOLEAN,
    stock_qty                   INT,
    updated_at                  STRING
)
PARTITIONED BY (region STRING)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/smartprotein/curated/local_store_inventory'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE EXTERNAL TABLE IF NOT EXISTS curated_user_wellness (
    user_hash_id                STRING,   -- Salted SHA-256 anonymized ID
    city                        STRING,
    lat_grid                    DOUBLE,   -- Privacy-rounded (~1 km grid)
    lon_grid                    DOUBLE,
    age                         INT,
    weight_kg                   DOUBLE,
    goal                        STRING,
    diet_type                   STRING,
    daily_budget_inr            DOUBLE,
    allergies                   STRING,
    medical_conditions          STRING,
    workout_time                STRING,
    avg_steps_daily             DOUBLE,
    avg_workout_minutes         DOUBLE,
    avg_sleep_hours             DOUBLE,
    daily_protein_target_g      DOUBLE
)
PARTITIONED BY (region STRING)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/smartprotein/curated/user_wellness_cleaned'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- ----------------------------------------------------------------------------
-- ZONE 4: REJECTED / QUARANTINE TABLES
-- ----------------------------------------------------------------------------
USE smartprotein_rejected;

CREATE EXTERNAL TABLE IF NOT EXISTS rejected_records (
    source_dataset      STRING,
    raw_record_id       STRING,
    raw_payload         STRING,
    dq_dimension        STRING,
    reject_reason_desc  STRING,
    detected_by         STRING,   -- RULE_ENGINE, ISOLATION_FOREST, GREAT_EXPECTATIONS
    rejected_at         STRING
)
PARTITIONED BY (reject_reason_code STRING, ingestion_date STRING)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/smartprotein/rejected/all_records'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- ----------------------------------------------------------------------------
-- ZONE 5: ANALYTICS, LINEAGE & SERVING TABLES
-- ----------------------------------------------------------------------------
USE smartprotein_analytics;

CREATE EXTERNAL TABLE IF NOT EXISTS dq_scorecard_history (
    batch_id             STRING,
    run_timestamp        STRING,
    dataset_name         STRING,
    stage                STRING,   -- BEFORE_CLEANING vs AFTER_CLEANING
    total_records        BIGINT,
    clean_records        BIGINT,
    rejected_records     BIGINT,
    imputed_records      BIGINT,
    completeness_score   DOUBLE,
    validity_score       DOUBLE,
    consistency_score    DOUBLE,
    uniqueness_score     DOUBLE,
    composite_dq_score   DOUBLE,
    drift_alert_flag     BOOLEAN
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/smartprotein/analytics/dq_scorecards'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE EXTERNAL TABLE IF NOT EXISTS dq_lineage (
    lineage_id          STRING,
    batch_id            STRING,
    raw_source_file     STRING,
    raw_record_id       STRING,
    rule_applied        STRING,
    action_taken        STRING,   -- PASSED, IMPUTED, RECONCILED, QUARANTINED
    target_zone         STRING,
    target_record_id    STRING,
    processed_at        STRING
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/smartprotein/analytics/dq_lineage'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE EXTERNAL TABLE IF NOT EXISTS gold_user_recommendations (
    user_hash_id                STRING,
    city                        STRING,
    diet_type                   STRING,
    daily_budget_inr            DOUBLE,
    daily_protein_target_g      DOUBLE,
    rank                        INT,
    canonical_name              STRING,
    store_name                  STRING,
    distance_km                 DOUBLE,
    usable_protein_per_100g     DOUBLE,
    price_per_100g_inr          DOUBLE,
    cost_per_g_usable_protein   DOUBLE,
    composite_rec_score         DOUBLE,
    explanation_text            STRING
)
PARTITIONED BY (region STRING)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/smartprotein/analytics/user_recommendations'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

