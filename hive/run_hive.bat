@echo off
title SmartProtein / FitGuard - Apache Hive HQL Local Lakehouse Runner
echo =======================================================================
echo   Executing: create_external_tables.hql against Parquet Lakehouse
echo =======================================================================
python "%~dp0run_hive_script.py" "%~dp0create_external_tables.hql"
echo.
pause
