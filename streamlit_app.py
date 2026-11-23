"""Streamlit App Entrypoint for SmartProtein | Hadoop HDFS DQ & Accurate Protein Recommender."""

import asyncio
import sys
from pathlib import Path

if sys.platform == "win32":
    try:
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    except Exception:
        pass

# Add project root and smartprotein to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "smartprotein"))

# Execute smartprotein dashboard app
app_path = PROJECT_ROOT / "smartprotein" / "dashboard" / "app.py"
with open(app_path, encoding="utf-8") as f:
    code = compile(f.read(), str(app_path), "exec")
    exec(code, {"__name__": "__main__", "__file__": str(app_path)})
