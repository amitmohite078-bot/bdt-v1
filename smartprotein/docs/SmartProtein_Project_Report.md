# SmartProtein: A Hadoop-Based Big Data Quality and Cleaning Pipeline for Accurate, Cost-Aware, and Location-Based Protein Recommendation in Fitness and Wellness

---

## Abstract

In fitness, sports nutrition, and preventive wellness, individuals increasingly depend on food composition catalogs, retail price feeds, and wearable telemetry to meet daily protein requirements. However, heterogeneous nutrition datasets (e.g., USDA FoodData Central, IFCT 2017, Open Food Facts), OCR-scanned labels, and store inventory feeds suffer from severe Big Data quality defects—missing values, inconsistent serving units (`per_50g_serving` vs. `per_100g`), energy unit swaps (`kJ` vs. `kcal`), duplicate SKUs, impossible macronutrient sums, Atwater calorie discrepancies, and semantic dietary-tag misclassifications (e.g., meat or dairy products tagged `Vegan`). Furthermore, standard trackers report crude label protein rather than digestible, bioavailable **usable protein**.

**SmartProtein** implements a 5-zone Hadoop HDFS Lakehouse (`raw`, `staging`, `curated`, `rejected`, `analytics`) using Apache Spark (PySpark), MapReduce, Hive, Kafka, Airflow, and a Pantone-validated Streamlit dashboard. The pipeline profiles, standardizes, deduplicates (exact + MinHash LSH/Levenshtein), checks Atwater thermodynamics, detects multivariate anomalies (Isolation Forest + K-Means), reconciles sources via trust weights, tracks row-level lineage, and computes 0–100 Data Quality (DQ) scores. An Accurate Protein Informatics Layer adjusts crude protein into usable protein using FAO DIAAS/PDCAAS, cooking retention, phytate bioavailability, and leucine synthesis triggers. Finally, a hybrid hyperlocal recommender combines configurable multi-factor scoring, Linear Programming cost optimization, Haversine/GeoHash proximity, allergy/medical safety filters, and collaborative filtering. Experimental evaluation demonstrates composite DQ improvement (`92.39 → 100.00` on food composition), `74.1%–74.2%` reduction in Protein MAE against reference values, `99.3%` fuzzy duplicate recovery, `86.9%–89.1%` storage reduction with Snappy Parquet over CSV, `3.49x–3.91x` speedup over Hadoop Streaming MapReduce, and `59.9%` average daily cost savings via Linear Programming.

---

## Chapter 1: Introduction

### 1.1 Background and Motivation
Wearable fitness trackers and nutrition applications generate continuous multi-modal data streams alongside vast public food catalogs. When food composition tables, store price feeds, and biometric logs are integrated without automated data quality engineering, downstream meal recommenders suffer from "Garbage In, Garbage Out": miscalculated calories, unsafe allergen exposure, or inflated crude protein estimates that mislead consumers—especially budget-conscious students and vegetarian/vegan athletes.

### 1.2 Problem Statement
Existing fitness and nutrition pipelines exhibit three coupled deficiencies:
1. **Unverified Multi-Source Data Quality:** Combining lab tables (USDA, IFCT) with crowdsourced catalogs (Open Food Facts), OCR scans, and vendor feeds introduces schema drift, duplicate SKUs, unit errors (`kJ` vs. `kcal`, `USD` vs. `INR`), and semantic diet-tag errors.
2. **Crude Protein vs. Usable Protein:** Crude nitrogen/protein labels ignore true ileal amino acid digestibility (**DIAAS/PDCAAS**), cooked-vs-raw moisture shifts, anti-nutrient phytate inhibition, and per-meal leucine thresholds ($\approx 2.5\text{–}3.0\text{ g}$).
3. **Budget, Safety, and Hyperlocal Disconnect:** Most recommenders ignore real-time store stock within $X\text{ km}$, regional food staples, allergy hard-exclusions, medical protein ceilings, and mathematical cost minimization.

### 1.3 Objectives
1. Design a 5-zone governed Hadoop HDFS Lakehouse (`raw`, `staging`, `curated`, `rejected`, `analytics`) using columnar Parquet/ORC partitioned by `region` and `category`.
2. Implement an automated Big Data Quality pipeline covering profiling, imputation, exact + fuzzy deduplication (MinHash LSH), Atwater cross-checks, ML anomaly detection (Isolation Forest + K-Means), self-healing category thresholds, lineage tracking, and automated DQ test gates.
3. Build an Accurate Protein Layer computing DIAAS/PDCAAS, cooking retention, phytate bioavailability, amino acid complementarity, leucine triggers, and wearable-adjusted daily protein targets ($1.2\text{–}2.0\text{ g/kg}$).
4. Develop a cost-aware hyperlocal recommender integrating configurable scoring, Linear Programming diet optimization, Haversine/GeoHash filtering, allergy/medical safety guards, and hybrid collaborative filtering.
5. Benchmark storage formats (CSV vs. Parquet vs. ORC), processing engines (MapReduce vs. Spark/vectorized), linear scalability, and domain accuracy.

### 1.4 Scope and Prioritization
Delivered across a 7-week phased roadmap (`MUST-HAVE` core HDFS/Spark DQ, Usable Protein, LP Hyperlocal Recommender, Dashboard, and Benchmarks; `SHOULD-HAVE` Kafka speed layer, ML anomaly detection, self-healing rules, ALS collaborative filtering, and governance; `OPTIONAL` Tesseract OCR label ingestion, trust-weighted crowd flags, and micronutrient gap hints).

---

## Chapter 2: Literature Survey

Modern Big Data nutrition pipelines draw upon foundational distributed systems, data quality verification frameworks, nutritional biochemistry standards, and mathematical diet optimization:

1. **Distributed Storage & Compute:** Shvachko et al. [1] established the **Hadoop Distributed File System (HDFS)** with write-once-read-many block replication for fault-tolerant storage, while Zaharia et al. [2] introduced **Resilient Distributed Datasets (RDDs)** in Apache Spark to overcome MapReduce disk-shuffle bottlenecks via in-memory execution. Thusoo et al. [3] bridged SQL warehousing over HDFS through **Apache Hive**, and Kreps et al. [4] introduced **Apache Kafka** for high-throughput, low-latency streaming ingestion.
2. **Automated Data Quality & Anomaly Detection:** Schelter et al. [5] formalized large-scale data quality as declarative "unit tests for data" in **Amazon Deequ** over Apache Spark, tracking completeness, validity, and historical metric drift. For multivariate anomalies that bypass static bounds, Liu et al. [6] proposed **Isolation Forest**, isolating anomalies via shorter average path lengths in random partition trees.
3. **Nutritional Composition & Protein Quality Standards:** Fukagawa et al. [7] detailed **USDA FoodData Central (FDC)** unifying Foundation Foods, Standard Reference, and Branded Foods, while Longvah et al. [8] published **IFCT 2017** providing laboratory-analyzed Indian food composition profiles. To measure biological protein quality accurately, the **FAO Expert Consultation (2013)** [9] recommended **DIAAS** over PDCAAS based on true ileal indispensable amino acid digestibility.
4. **Food Recommendation & Diet Optimization:** Stigler [10] formulated the classical **"Diet Problem"** using Linear Programming to minimize subsistence cost subject to nutrient constraints, while Trattner and Elsweiler [11] surveyed modern **Food Recommender Systems**, highlighting the trade-off between user taste preferences (collaborative filtering) and clinical/nutritional safety constraints.

| Ref | Author(s) & Year | Focus Area | Contribution | Gap Addressed by SmartProtein |
| :--- | :--- | :--- | :--- | :--- |
| [1] | Shvachko et al. (2010) | HDFS | Fault-tolerant 128 MB block storage | Applied via 5-zone Medallion layout & Parquet compaction |
| [2] | Zaharia et al. (2012) | Apache Spark | In-memory RDD cluster computing | Applied to iterative DQ rules, MinHash LSH, and ML |
| [3] | Thusoo et al. (2009) | Apache Hive | SQL over HDFS external tables | Used for Curated, Quarantine & `dq_lineage` warehousing |
| [4] | Kreps et al. (2011) | Apache Kafka | Distributed log streaming | Powers Lambda speed layer for live price & HR spike alerts |
| [5] | Schelter et al. (2018) | Deequ / DQ | Declarative Spark data unit tests | Extended with nutrition physics (Atwater, semantic diet checks) |
| [6] | Liu et al. (2008) | Isolation Forest | Unsupervised tree anomaly detection | Combined with K-Means to catch multivariate macro errors |
| [7] | Fukagawa et al. (2022) | USDA FDC | Unified US food composition | Reconciled with IFCT & Open Food Facts via trust weights |
| [8] | Longvah et al. (2017) | IFCT 2017 | Indian lab food composition | Integrated for regional Indian protein staples & phytates |
| [9] | FAO (2013) | DIAAS Report | True ileal protein quality scoring | Converts crude label protein into bioavailable usable protein |
| [10] | Stigler (1945) | Diet Problem LP | Minimum-cost nutrition via LP | Extended with palatability bounds, hyperlocal stock & allergies |
| [11] | Trattner & Elsweiler (2017/2018) | Food RecSys | Taste vs. health & cold-start survey | Bridged via hybrid cold-start scoring + ALS + hard safety gates |

---

## Chapter 3: System Architecture & HDFS Zone Design

SmartProtein adopts a **Lambda + 5-Zone Medallion Architecture** deployed on Dockerized Hadoop HDFS, Spark, Hive, Kafka, and Airflow:

1. **`/smartprotein/raw` (Zone 1):** Immutable landing directory partitioned by `ingestion_date=YYYY-MM-DD`, storing raw CSVs, OCR label PNGs, and archived streams.
2. **`/smartprotein/staging` (Zone 2):** Typed Snappy Parquet storing standardized schemas, column profiles, and salted SHA-256 hashed user identifiers (`user_hash_id`).
3. **`/smartprotein/curated` (Zone 3):** Trusted source of truth partitioned by `category` (`master_protein_foods`, `protein_quality_enriched`, and ORC replica `protein_quality_enriched_orc`) and `region` (`local_store_inventory`, `user_wellness_cleaned`).
4. **`/smartprotein/rejected` (Zone 4):** Dead-letter quarantine partitioned by `(reject_reason_code, ingestion_date)` ensuring zero silent data loss and full error traceability.
5. **`/smartprotein/analytics` (Zone 5):** Serving layer holding Before-vs-After `dq_scorecards`, `dq_lineage`, `gold_user_recommendations`, `lp_optimal_meal_plans`, `realtime_price_alerts`, `drift_alerts`, and benchmark results.

**Data Governance & Privacy:** Raw `user_id` is anonymized via salted SHA-256 (`24`-char digest), GPS coordinates are generalized to a ~1 km grid (`round(lat, 2)`), and Apache Ranger-style RBAC policies (`governance_rbac_policy.json`) separate `data_engineer_etl`, `data_steward_dq`, `nutrition_analyst`, and `dashboard_app_service` roles.

---

## Chapter 4: Methodology & Implementation

### 4.1 Multi-Source Ingestion & Synthetic Dirty Data Engine
`generate_smartprotein_data.py` generates configurable datasets (`dev`: ~102k rows; `1m`: ~1.06M rows; `5m`: ~5.05M rows) across 5 tables while injecting ~18% realistic errors: nulls, negative/outlier prices, `kJ`/`per_50g_serving`/`USD` mismatches, Atwater violations, `Vegan` meat/dairy tag lies, fuzzy typos, and invalid wearable biometrics. Batch files enter HDFS via `hdfs dfs -put` and Sqoop (`sqoop import --num-mappers 4`), OCR labels via `ocr_label_pipeline.py`, and real-time events via `kafka_producers.py`.

### 4.2 Data Profiling, Cleaning, Quarantine & DQ Scoring
`profile_raw_data.py` computes per-column null %, min/max/quantiles, and key duplicates. `clean_and_quarantine.py` (and `run_stage6_local.py`) standardizes energy (`kJ / 4.184`), weight (`per_50g_serving * 2`), and currency (`USD * 84.0`), maps regional synonyms, imputes secondary missing fields by category median, validates Atwater balance ($|\text{kcal} - (4P + 4C + 9F)| \le \max(35, 0.22 \times \text{Atwater})$), filters per-category $1.5 \times \text{IQR}$ outliers, blocks semantic diet lies (`ERR_DIET_TAG_LIE`), reconciles multi-source observations using trust weights (`IFCT_2017: 0.45`, `USDA_FDC: 0.40`, `OPEN_FOOD_FACTS: 0.15`, `OCR_LABEL: 0.10`), and scores datasets `0–100` across Completeness ($0.25$), Validity ($0.30$), Consistency ($0.25$), and Uniqueness ($0.20$).

### 4.3 Fuzzy Deduplication, ML Anomaly Detection, Self-Healing & Lineage
- **Fuzzy Dedup (`fuzzy_dedup_minhash_lsh.py`):** Uses **Category Blocking** + 3-gram **MinHash LSH** + **Levenshtein similarity** to rescue typo/brand-wrapped strings without $O(N^2)$ comparisons.
- **Self-Healing & ML Anomaly Detection (`ml_anomaly_and_self_healing.py`):** Learns empirical `p01`–`p99` category bounds automatically and runs an **Isolation Forest + K-Means** ensemble over standardized 5D macro features (`p_100g, c_100g, f_100g, kcal_100g, atwater_ratio`). Trust-weighted crowdsourced flags (`weighted_trust_sum >= 1.2`) surface community corrections.
- **Lineage & Automated Tests:** `lineage_tracker.py` records every record's `raw -> rule -> target_zone` trajectory; `run_dq_validation_suite.py` acts as an Airflow gate halting downstream execution on any assertion failure.

### 4.4 Accurate Protein Informatics Layer
`accurate_protein_enrichment.py` transforms crude protein into usable protein:
$$\text{Usable Protein (g/100g)} = \text{Crude Protein} \times \text{Moisture Yield} \times \text{Cooking Retention} \times \min(1.0, \text{DIAAS}) \times \text{Bioavailability}$$
where phytate inhibition is adjusted by cooking reduction (`boiled`: $-35\%$). It also computes `grams_for_2_5g_leucine` (muscle protein synthesis trigger), complementary amino acid pairings (e.g., *Moong Dal + Ragi*), cost per gram of **usable** protein, and personalized targets ($1.2\text{–}2.0\text{ g/kg}$ from wearable steps, workout minutes, sleep, age, and a `1.0 g/kg` `kidney_caution` cap).

### 4.5 Cost-Aware Hyperlocal Hybrid Recommender & Speed Layer
`hybrid_recommender.py`, `safety_and_geo.py`, and `lp_budget_optimizer.py` enforce hard allergen exclusion, match in-stock stores within $15\text{ km}$ via **Haversine + GeoHash**, apply configurable scoring ($w_1\text{AdjustedProtein} + w_2\text{ProteinPerCost} + w_3\text{Proximity} + w_4\text{PreferenceMatch} - w_5\text{CaloriePenalty}$), evaluate hybrid collaborative filtering (SVD / Spark MLlib ALS), and solve the minimum-cost **Linear Programming Diet Problem** (`PuLP`/`SciPy HiGHS`) into 4-slot daily meal plans with Iron/Calcium/B12 hints. `speed_layer_alerts.py` + `smartprotein_medallion_dag.py` provide Kafka streaming alerts, source drift detection, and Airflow scheduling, surfaced in a Pantone-validated Streamlit dashboard (`app.py`).

---

## Chapter 5: Experimental Results & Performance Evaluation

All experiments below were executed locally on the generated multi-source datasets (including the Stage 3 `dev` snapshot + Stage 5 OCR records and multi-day run verification).

### 5.1 Data Quality & Domain Accuracy Results
- **Before vs. After DQ Score (0–100):**
  - `food_composition`: **`92.39` → `100.00`** (`4,497` quarantined out of `15,002` in single-batch run; `100%` of injected semantic diet lies `171/171` caught).
  - `store_prices`: **`97.29` → `100.00`** (`3,166` invalid/negative prices quarantined out of `35,000`).
  - `user_wearables`: **`97.28` → `100.00`** (`4,540` corrupt biometric/budget logs quarantined out of `50,000`).
- **Protein MAE vs. Lab Reference (`g/100g`):** Reduced from **`2.650`** (raw mean) to **`0.685`** (trust-reconciled curated), a **`74.1%`–`74.2%` error reduction**.
- **Fuzzy Dedup (MinHash LSH + Levenshtein):** Rescued **`99.3%`** (`274/276` single-batch; `548/552` multi-batch) of typo/brand-wrapped names.
- **ML Anomaly Detection (Isolation Forest + K-Means):** Achieved **Precision = `0.7329`**, **Recall = `0.9436`**, **F1 = `0.8250`** on `14,816` evaluated records.

### 5.2 Accurate Protein & Recommender Evaluation
- **Cost per Gram of Usable Protein:** Demonstrated that **Soya Chunks** yield **`₹0.50/g`** usable protein (`29.09 g` usable/100g after DIAAS `0.91` & phytate adjustment) vs. **Eggs (`₹1.14/g`)**, **Chicken Breast (`₹1.53/g`)**, **Low-Fat Paneer (`₹2.22/g`)**, and **Whey Isolate (`₹2.90/g`**, hitting the `2.5 g` leucine trigger in just **`26 g`**).
- **Linear Programming & Collaborative Filtering:** Solved **`250/250`** LP daily meal plans (`95.2%` within user budget) with **`59.9%` average cost savings** vs. naive median selection, and achieved **`Precision@5 = 0.2408`**, **`Recall@5 = 1.0000`** across `142` evaluated users.

### 5.3 Big Data Storage, Engine & Scalability Benchmarks
- **Storage Format (50,000 `user_wearables` rows):** CSV (`7.160 MB`, 4-col read `0.038 s`) vs. **Parquet Snappy (`0.940 MB` — `86.9%` smaller)** vs. **ORC Snappy (`1.855 MB`, 4-col read `0.018 s`)**. *(At 100,000 rows: CSV `14.320 MB`, Parquet `1.566 MB` [`89.1%` smaller], ORC `3.576 MB` [`0.018 s` read].)*
- **Engine Throughput:** Vectorized columnar processing achieved **`3.49x`** speedup over Hadoop Streaming MapReduce on `15,002` rows (`0.0776 s` / `193,291 rec/s` vs. `0.2706 s` / `55,433 rec/s`) and **`3.91x`** on `30,004` rows (`0.0801 s` vs. `0.3129 s`).
- **Scalability (`15k → 300k` rows):** Scaled linearly from `0.0157 s` (`15k`) to `0.2148 s` (`300k`) at `~1.40M–1.77M rec/sec`.
- **1-Node vs. 3-Node Cluster Scaling:** Scripted in `benchmarks/cluster_1v3_nodes_benchmark.sh` (`--profile scale3`) for execution on Docker Desktop.

---

## Chapter 6: Conclusion & Future Work

### 6.1 Conclusion
SmartProtein demonstrates that combining **Big Data quality engineering on Hadoop HDFS** with **nutritional biochemistry (DIAAS, cooking, bioavailability, leucine)** and **mathematical optimization (LP + Haversine/GeoHash + hybrid CF)** produces safer, substantially more accurate (`74.1%+` lower protein MAE), and far more affordable (`59.9%` lower daily cost) protein recommendations than crude-label systems.

### 6.2 Limitations and Future Work
1. **Live Supermarket Inventory APIs:** Replace synthetic store price feeds with real retail/ONDC catalog APIs.
2. **Multi-Node Cloud Deployment:** Execute the scripted `1-vs-3` DataNode benchmark on AWS EMR or multi-host Kubernetes/Docker clusters at `5M+` rows.
3. **Clinical Personalization:** Integrate blood biomarker feedback (ferritin, B12, eGFR, HbA1c) under clinician supervision.
4. **Full Apache Atlas & Ranger Integration:** Deploy live Atlas metadata hooks and Kerberized Ranger policies for enterprise deployments.

---

## References (IEEE Format)

[1] K. Shvachko, H. Kuang, S. Radia, and R. Chansler, "The Hadoop Distributed File System," in *2010 IEEE 26th Symposium on Mass Storage Systems and Technologies (MSST)*, Incline Village, NV, May 2010, pp. 1–10, doi: 10.1109/MSST.2010.5496972.

[2] M. Zaharia, M. Chowdhury, T. Das, A. Dave, J. Ma, M. McCauley, M. J. Franklin, S. Shenker, and I. Stoica, "Resilient distributed datasets: A fault-tolerant abstraction for in-memory cluster computing," in *Proceedings of the 9th USENIX Conference on Networked Systems Design and Implementation (NSDI '12)*, San Jose, CA, Apr. 2012, p. 2.

[3] A. Thusoo, J. Sen Sarma, N. Jain, Z. Shao, P. Chakka, S. Anthony, H. Liu, P. Wyckoff, and R. Murthy, "Hive - a warehousing solution over a map-reduce framework," *Proceedings of the VLDB Endowment*, vol. 2, no. 2, pp. 1626–1629, Aug. 2009, doi: 10.14778/1687553.1687609.

[4] J. Kreps, N. Narkhede, and J. Rao, "Kafka: a distributed messaging system for log processing," in *Proceedings of the NetDB'11 (Workshop on Networking Meets Databases)*, Athens, Greece, Jun. 2011, pp. 1–7.

[5] S. Schelter, D. Lange, P. Schmidt, M. Celikel, F. Biessmann, and A. Grafberger, "Automating large-scale data quality verification," *Proceedings of the VLDB Endowment*, vol. 11, no. 12, pp. 1781–1794, Aug. 2018.

[6] F. T. Liu, K. M. Ting, and Z.-H. Zhou, "Isolation Forest," in *2008 Eighth IEEE International Conference on Data Mining (ICDM)*, Pisa, Italy, Dec. 2008, pp. 413–422, doi: 10.1109/ICDM.2008.17.

[7] N. K. Fukagawa, K. McKillop, P. R. Pehrsson, A. Moshfegh, J. Harnly, and J. Finley, "USDA's FoodData Central: what is it and why is it needed today?," *The American Journal of Clinical Nutrition*, vol. 115, no. 3, pp. 619–624, Mar. 2022, doi: 10.1093/ajcn/nqab397.

[8] T. Longvah, R. Ananthan, K. Bhaskarachary, and K. Venkaiah, *Indian Food Composition Tables 2017*. Hyderabad, India: National Institute of Nutrition (ICMR), 2017.

[9] Food and Agriculture Organization of the United Nations (FAO), *Dietary Protein Quality Evaluation in Human Nutrition: Report of an FAO Expert Consultation*, FAO Food and Nutrition Paper No. 92, Rome, Italy: FAO, 2013.

[10] G. J. Stigler, "The cost of subsistence," *Journal of Farm Economics*, vol. 27, no. 2, pp. 303–314, May 1945.

[11] C. Trattner and D. Elsweiler, "Food recommender systems: Important contributions, challenges and future research directions," in *Collaborative Recommendations: Algorithms, Practical Challenges and Applications*, World Scientific Publishing, 2018, pp. 138–166 (also arXiv:1711.02760, 2017).

