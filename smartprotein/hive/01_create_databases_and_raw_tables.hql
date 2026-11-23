-- ============================================================================
-- SmartProtein Hive DDL Part 1: Databases & Raw Zone External Tables
-- Target HDFS Root: hdfs://namenode:9000/smartprotein
-- ============================================================================

CREATE DATABASE IF NOT EXISTS smartprotein_raw
COMMENT 'Zone 1: Immutable multi-source raw landing data (USDA, IFCT, OFF, Stores, Wearables)'
LOCATION 'hdfs://namenode:9000/smartprotein/raw';

CREATE DATABASE IF NOT EXISTS smartprotein_staging
COMMENT 'Zone 2: Schema-normalized, profiled & PII-hashed staging tables'
LOCATION 'hdfs://namenode:9000/smartprotein/staging';

CREATE DATABASE IF NOT EXISTS smartprotein_curated
COMMENT 'Zone 3: Trusted, cleaned, deduplicated, DIAAS-adjusted Parquet/ORC tables'
LOCATION 'hdfs://namenode:9000/smartprotein/curated';

CREATE DATABASE IF NOT EXISTS smartprotein_rejected
COMMENT 'Zone 4: Quarantined records with reject_reason_code and raw payload'
LOCATION 'hdfs://namenode:9000/smartprotein/rejected';

CREATE DATABASE IF NOT EXISTS smartprotein_analytics
COMMENT 'Zone 5: Serving tables (DQ Scorecards, Lineage, Meal Plans, Recommendations)'
LOCATION 'hdfs://namenode:9000/smartprotein/analytics';

USE smartprotein_raw;

-- 1. Multi-source Food Composition (USDA FDC / IFCT 2017 / Open Food Facts)
CREATE EXTERNAL TABLE IF NOT EXISTS raw_food_composition (
    record_id                   STRING,
    source_db                   STRING,   -- USDA_FDC, IFCT_2017, OPEN_FOOD_FACTS, OCR_LABEL
    food_name                   STRING,
    category                    STRING,
    diet_type                   STRING,
    state_raw_cooked            STRING,   -- raw, cooked_boiled, cooked_fried, cooked_grilled, cooked_roasted
    serving_unit                STRING,   -- per_100g, per_kg, per_50g_serving, per_oz
    protein_g                   DOUBLE,
    carbs_g                     DOUBLE,
    fat_g                       DOUBLE,
    fiber_g                     DOUBLE,
    moisture_g                  DOUBLE,
    reported_kcal               DOUBLE,
    energy_unit                 STRING,   -- kcal or kJ
    iron_mg                     DOUBLE,
    calcium_mg                  DOUBLE,
    vitamin_b12_mcg             DOUBLE,
    leucine_g_per_100g_protein  DOUBLE,
    phytate_mg                  DOUBLE,
    allergens                   STRING,   -- pipe-separated e.g. dairy|lactose
    last_updated                STRING
)
PARTITIONED BY (ingestion_date STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://namenode:9000/smartprotein/raw/food_composition'
TBLPROPERTIES ('skip.header.line.count'='1');

-- 2. Hyperlocal Store Locations (OpenStreetMap / Synthetic Lat-Long + GeoHash)
CREATE EXTERNAL TABLE IF NOT EXISTS raw_store_locations (
    store_id      STRING,
    store_name    STRING,
    region        STRING,
    city          STRING,
    latitude      DOUBLE,
    longitude     DOUBLE,
    geohash_6     STRING,
    store_type    STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://namenode:9000/smartprotein/raw/store_locations'
TBLPROPERTIES ('skip.header.line.count'='1');

-- 3. Store Food Prices & Live Inventory Offers (Sqoop / Batch)
CREATE EXTERNAL TABLE IF NOT EXISTS raw_store_prices (
    offer_id             STRING,
    store_id             STRING,
    food_name_raw        STRING,
    canonical_food_hint  STRING,
    pack_weight_g        DOUBLE,
    price_value          DOUBLE,
    currency             STRING,
    in_stock             BOOLEAN,
    stock_qty            INT,
    updated_at           STRING
)
PARTITIONED BY (ingestion_date STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://namenode:9000/smartprotein/raw/store_prices'
TBLPROPERTIES ('skip.header.line.count'='1');

-- 4. User Profiles & Fitness Wearable Logs
CREATE EXTERNAL TABLE IF NOT EXISTS raw_user_wearables (
    event_id            STRING,
    user_id             STRING,
    region              STRING,
    city                STRING,
    latitude            DOUBLE,
    longitude           DOUBLE,
    age                 INT,
    weight_kg           DOUBLE,
    goal                STRING,
    diet_type           STRING,
    daily_budget_inr    DOUBLE,
    allergies           STRING,
    medical_conditions  STRING,
    workout_time        STRING,
    steps_daily         INT,
    workout_minutes     INT,
    sleep_hours         DOUBLE,
    event_ts            STRING
)
PARTITIONED BY (ingestion_date STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://namenode:9000/smartprotein/raw/user_wearables'
TBLPROPERTIES ('skip.header.line.count'='1');

-- 5. Crowdsourced Correction Flags
CREATE EXTERNAL TABLE IF NOT EXISTS raw_crowd_flags (
    flag_id          STRING,
    user_id          STRING,
    user_trust_score DOUBLE,
    record_id        STRING,
    field_name       STRING,
    suggested_value  STRING,
    submitted_at     STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://namenode:9000/smartprotein/raw/crowd_flags'
TBLPROPERTIES ('skip.header.line.count'='1');

-- Run after loading partitions into HDFS:
-- MSCK REPAIR TABLE smartprotein_raw.raw_food_composition;
-- MSCK REPAIR TABLE smartprotein_raw.raw_store_prices;
-- MSCK REPAIR TABLE smartprotein_raw.raw_user_wearables;

