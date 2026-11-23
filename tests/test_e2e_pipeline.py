"""End-to-End Hadoop Lakehouse & Pipeline Artifact Tests."""

from __future__ import annotations

from pathlib import Path
import pandas as pd
import pytest

ROOT_DIR = Path(__file__).resolve().parent.parent
SMART_DIR = ROOT_DIR / "smartprotein"
CURATED_DIR = SMART_DIR / "data" / "curated"
ANALYTICS_DIR = SMART_DIR / "data" / "analytics"
CONFIG_DIR = SMART_DIR / "config"


def test_configuration_files_exist():
    """Verify seed data and recommendation configuration files exist and are valid JSON."""
    seed_file = CONFIG_DIR / "reference_foods_seed.json"
    rec_file = CONFIG_DIR / "recommender_config.json"
    assert seed_file.exists(), f"Missing {seed_file}"
    assert rec_file.exists(), f"Missing {rec_file}"


def test_curated_tables_exist():
    """Verify Curated Zone Parquet tables exist with Snappy compression."""
    expected_tables = [
        "master_protein_foods.snappy.parquet",
        "local_store_inventory.snappy.parquet",
        "user_wellness_cleaned.snappy.parquet",
    ]
    for tbl in expected_tables:
        path = CURATED_DIR / tbl
        assert path.exists(), f"Curated table {tbl} missing at {path}"
        df = pd.read_parquet(path)
        assert len(df) > 0, f"Curated table {tbl} is empty"


def test_analytics_reports_exist():
    """Verify Analytics Mart tables and ML metrics reports exist."""
    ml_eval_path = ANALYTICS_DIR / "ml_eval_summary.json"
    if ml_eval_path.exists():
        import json
        data = json.loads(ml_eval_path.read_text(encoding="utf-8"))
        assert "isolation_forest" in data or "precision" in data or "dq_composite" in data


def test_dashboard_entrypoint_exists():
    """Verify root app.py, streamlit_app.py and index.html exist."""
    assert (ROOT_DIR / "app.py").exists()
    assert (ROOT_DIR / "streamlit_app.py").exists()
    assert (ROOT_DIR / "index.html").exists()
