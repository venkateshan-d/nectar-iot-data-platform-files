@echo off
REM ===========================================================================
REM  Nectar IoT Data Platform - run everything on Windows.
REM  Just double-click this file. No commands to type.
REM ===========================================================================
setlocal
cd /d "%~dp0"
set PYTHONPATH=src

echo.
echo ==========================================================
echo   NECTAR IOT DATA PLATFORM
echo ==========================================================
echo.

REM ---- check Python -------------------------------------------------------
python --version >nul 2>&1
if errorlevel 1 (
  echo [X] Python is not installed, or not on PATH.
  echo     Install Python 3.11 from python.org and tick "Add to PATH".
  goto :end
)
for /f "tokens=*" %%v in ('python --version 2^>^&1') do echo [1/6] %%v

REM ---- check Java ---------------------------------------------------------
java -version >nul 2>&1
if errorlevel 1 (
  echo.
  echo [X] Java is not installed. Spark needs it.
  echo     Install Temurin JDK 17 from adoptium.net, then run this file again.
  goto :end
)
echo [2/6] Java found

REM ---- install dependencies ----------------------------------------------
if not exist ".venv" (
  echo [3/6] Creating virtual environment and installing packages...
  echo       ^(first time only - about 5 minutes, ~300 MB download^)
  python -m venv .venv
  .venv\Scripts\python -m pip install --upgrade pip --quiet
  .venv\Scripts\pip install -r requirements.txt --quiet
  if errorlevel 1 goto :fail
) else (
  echo [3/6] Dependencies already installed
)

REM ---- generate the dataset ----------------------------------------------
echo [4/6] Generating the IoT dataset ^(~600,000 readings^)...
.venv\Scripts\python -m nectar.generator.generate_data
if errorlevel 1 goto :fail

REM ---- run the pipeline ---------------------------------------------------
echo.
echo [5/6] Running the pipeline: bronze -^> silver -^> gold -^> quality report
echo       ^(about 2 minutes^)
.venv\Scripts\python -m nectar.pipeline.run_batch --format parquet
if errorlevel 1 goto :fail

REM ---- serve + SQL --------------------------------------------------------
echo.
echo [6/6] Publishing to DuckDB and running the SQL queries...
.venv\Scripts\python -m nectar.serving.load_duckdb
.venv\Scripts\python -m nectar.serving.run_queries
if errorlevel 1 goto :fail

REM ---- done ---------------------------------------------------------------
echo.
echo ==========================================================
echo   DONE - everything ran successfully
echo ==========================================================
echo.
echo   Data quality report:
echo     data\lakehouse\quality\reports\data_quality_report_latest.html
echo.
echo   SQL query results:
echo     data\query_results\sql_results.md
echo.
echo   Architecture diagram:
echo     docs\diagrams\architecture.html
echo.
echo   Opening the quality report in your browser...
start "" "data\lakehouse\quality\reports\data_quality_report_latest.html"
goto :end

:fail
echo.
echo ==========================================================
echo   SOMETHING FAILED - copy the red text above and send it
echo ==========================================================

:end
echo.
pause
