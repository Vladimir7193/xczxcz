@echo off
chcp 65001 >nul
cls
echo ========================================
echo   WAVE Scanner - AI Dashboard
echo ========================================
echo.
echo Запускаю сервер на порту 3900...
echo.
cd /d "%~dp0"

where python >nul 2>nul
if errorlevel 1 (
    echo [ERROR] Python не найден. Установи Python 3.10+ и добавь его в PATH.
    pause
    exit /b 1
)

if not exist ".venv\Scripts\python.exe" (
    echo Создаю виртуальное окружение...
    python -m venv .venv || goto :err
)

call ".venv\Scripts\activate.bat"
python -m pip install --quiet --upgrade pip
python -m pip install --quiet -r requirements.txt || goto :err

python start_dashboard.py
goto :eof

:err
echo.
echo [ERROR] Сборка окружения не удалась.
pause
exit /b 1
