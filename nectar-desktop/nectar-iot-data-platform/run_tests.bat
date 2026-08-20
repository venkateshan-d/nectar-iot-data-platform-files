@echo off
REM Run the 35 unit tests. Double-click to use.
setlocal
cd /d "%~dp0"
set PYTHONPATH=src
if not exist ".venv" (
  echo Run run.bat first - it sets up the environment.
  pause
  exit /b 1
)
echo Running unit tests...
echo.
.venv\Scripts\python -m pytest tests\ -v
echo.
pause
