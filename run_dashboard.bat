@echo off
title SmartProtein - Interactive Streamlit Dashboard
echo Launching SmartProtein Streamlit Dashboard...
streamlit run "%~dp0smartprotein\dashboard\app.py" --server.port 8501
pause
