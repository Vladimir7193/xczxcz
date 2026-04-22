@echo off
chcp 65001 >nul
cd /d "%~dp0"
if not exist .venv (
  echo [ERROR] .venv not found. Run run_ready.bat first.
  pause
  exit /b 1
)
call .venv\Scripts\activate.bat
python backtest.py
pause
