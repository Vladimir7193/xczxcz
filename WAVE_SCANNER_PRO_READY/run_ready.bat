@echo off
chcp 65001 >nul
setlocal enabledelayedexpansion
cd /d "%~dp0"

echo ==========================================
echo   WAVE SCANNER PRO - READY BUILD
echo ==========================================
echo.

where python >nul 2>nul
if errorlevel 1 (
  echo [ERROR] Python not found in PATH.
  echo Install Python 3.10+ and tick "Add Python to PATH".
  pause
  exit /b 1
)

if not exist .venv (
  echo [INFO] Creating virtual environment...
  python -m venv .venv
  if errorlevel 1 (
    echo [ERROR] Failed to create .venv
    pause
    exit /b 1
  )
)

call .venv\Scripts\activate.bat
if errorlevel 1 (
  echo [ERROR] Failed to activate .venv
  pause
  exit /b 1
)

echo [INFO] Upgrading pip...
python -m pip install --upgrade pip
if errorlevel 1 (
  echo [ERROR] pip upgrade failed
  pause
  exit /b 1
)

echo [INFO] Installing requirements...
pip install -r requirements.txt
if errorlevel 1 (
  echo [ERROR] Failed to install requirements
  pause
  exit /b 1
)

if not exist .env (
  copy .env.example .env >nul
  echo [INFO] Created .env from .env.example
)

echo.
echo [INFO] Running smoke test...
python smoke_test.py
if errorlevel 1 (
  echo [ERROR] Smoke test failed. Check .env and packages.
  pause
  exit /b 1
)

echo.
echo [INFO] Starting scanner...
python main.py

echo.
echo [INFO] Scanner stopped.
pause
