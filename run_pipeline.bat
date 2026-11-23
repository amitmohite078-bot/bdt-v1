@echo off
title SmartProtein - Full End-to-End Pipeline Runner
echo Running Full Pipeline (Stages 3 through 12)...
python "%~dp0smartprotein\run_all.py" --scale dev
pause
