"""SmartProtein Stage 10 (Modules #9 & #31): Apache Airflow Orchestration DAG.

Coordinates the end-to-end daily Batch Lakehouse pipeline on Hadoop HDFS:
  ingest_raw -> profile_raw -> clean_and_quarantine -> dq_validation_gate
  -> [fuzzy_dedup_and_ml, lineage_tracking] -> accurate_protein_layer
  -> hybrid_recommender_and_lp -> refresh_hive_metastore -> drift_check

If `dq_validation_gate` (Great Expectations / Deequ suite) fails, Airflow halts
downstream tasks before corrupt nutrition data can reach users.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_ROOT = "/opt/smartprotein"

DEFAULT_ARGS = {
    "owner": "smartprotein-data-eng",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="smartprotein_hdfs_medallion_pipeline",
    default_args=DEFAULT_ARGS,
    description="Daily SmartProtein HDFS Data Quality, Accurate Protein & Recommender DAG",
    schedule_interval="0 2 * * *",  # Daily at 02:00 UTC
    start_date=datetime(2026, 11, 1),
    catchup=False,
    tags=["smartprotein", "hdfs", "pyspark", "data-quality"],
) as dag:

    t_ingest = BashOperator(
        task_id="ingest_raw_to_hdfs",
        bash_command=(
            f"python3 {PROJECT_ROOT}/data_generator/generate_smartprotein_data.py --scale dev && "
            f"bash {PROJECT_ROOT}/ingestion/ingest_to_hdfs.sh {{{{ ds }}}}"
        ),
    )

    t_profile = BashOperator(
        task_id="profile_raw_data",
        bash_command=f"python3 {PROJECT_ROOT}/spark_jobs/profiling/profile_raw_data.py --engine spark --base-path hdfs://namenode:9000/smartprotein",
    )

    t_clean = BashOperator(
        task_id="spark_clean_and_quarantine",
        bash_command=(
            "/spark/bin/spark-submit --master spark://spark-master:7077 "
            f"{PROJECT_ROOT}/spark_jobs/cleaning/clean_and_quarantine.py "
            "--base-path hdfs://namenode:9000/smartprotein --ingestion-date {{ ds }}"
        ),
    )

    # Automated Great Expectations / Deequ Gate: halts DAG on non-zero exit
    t_dq_gate = BashOperator(
        task_id="dq_validation_gate",
        bash_command=f"python3 {PROJECT_ROOT}/tests/run_dq_validation_suite.py",
    )

    t_fuzzy_ml = BashOperator(
        task_id="fuzzy_dedup_and_ml_anomaly",
        bash_command=(
            f"python3 {PROJECT_ROOT}/spark_jobs/cleaning/fuzzy_dedup_minhash_lsh.py && "
            f"python3 {PROJECT_ROOT}/spark_jobs/cleaning/ml_anomaly_and_self_healing.py"
        ),
    )

    t_lineage = BashOperator(
        task_id="lineage_tracking",
        bash_command=f"python3 {PROJECT_ROOT}/spark_jobs/cleaning/lineage_tracker.py",
    )

    t_protein = BashOperator(
        task_id="accurate_protein_enrichment",
        bash_command=f"python3 {PROJECT_ROOT}/spark_jobs/protein_layer/accurate_protein_enrichment.py",
    )

    t_recommend = BashOperator(
        task_id="hybrid_recommender_and_lp",
        bash_command=f"python3 {PROJECT_ROOT}/spark_jobs/recommender/hybrid_recommender.py",
    )

    t_hive_repair = BashOperator(
        task_id="refresh_hive_metastore",
        bash_command=(
            "beeline -u jdbc:hive2://hive-server:10000 -e "
            "'MSCK REPAIR TABLE smartprotein_curated.curated_protein_foods; "
            "MSCK REPAIR TABLE smartprotein_curated.curated_store_inventory; "
            "MSCK REPAIR TABLE smartprotein_rejected.rejected_records;'"
        ),
    )

    t_drift = BashOperator(
        task_id="speed_layer_drift_check",
        bash_command=f"python3 {PROJECT_ROOT}/spark_jobs/streaming/speed_layer_alerts.py --mode local-replay",
    )

    t_ingest >> t_profile >> t_clean >> t_dq_gate
    t_dq_gate >> [t_fuzzy_ml, t_lineage] >> t_protein >> t_recommend >> t_hive_repair >> t_drift

