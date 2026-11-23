@echo off
title SmartProtein / FitGuard - Apache Hive HQL Local Lakehouse Runner
echo =======================================================================
echo   Executing: hive/create_external_tables.hql against Parquet Lakehouse
echo =======================================================================
python "%~dp0hive\run_hive_script.py" "%~dp0hive\create_external_tables.hql"
echo.
pause
