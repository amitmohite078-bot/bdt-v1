@echo off
REM Windows Command Wrapper for make / make alltest
set TARGET=%1
if "%TARGET%"=="" set TARGET=alltest

if "%TARGET%"=="alltest" goto do_alltest
if "%TARGET%"=="test" goto do_test
if "%TARGET%"=="dq-test" goto do_dqtest
if "%TARGET%"=="clean" goto do_clean
if "%TARGET%"=="help" goto do_help

echo Unknown target: %TARGET%
echo Running alltest by default...
goto do_alltest

:do_alltest
echo [1/2] Running Pytest Suite...
python -m pytest tests/ -v --tb=short
if errorlevel 1 exit /b 1
echo.
echo [2/2] Running Data Quality Validation Gate...
python smartprotein/tests/run_dq_validation_suite.py
if errorlevel 1 exit /b 1
echo.
echo ==========================================================
echo  [SUCCESS] All SmartProtein Unit, Integration and DQ Tests Passed!
echo ==========================================================
exit /b 0

:do_test
python -m pytest tests/ -v --tb=short
exit /b %errorlevel%

:do_dqtest
python smartprotein/tests/run_dq_validation_suite.py
exit /b %errorlevel%

:do_clean
python -c "import shutil, glob; [shutil.rmtree(p, ignore_errors=True) for p in glob.glob('**/__pycache__', recursive=True)]"
echo Cleaned cached files.
exit /b 0

:do_help
echo SmartProtein Windows Test Runner
echo   make alltest     Run full test suite (pytest + DQ validation)
echo   make test        Run unit tests with pytest
echo   make dq-test     Run DQ gate suite
exit /b 0
