@echo off
echo ========================================
echo Silver Layer Status Check
echo ========================================
echo.

REM Check if Python is installed
python --version >nul 2>&1
if errorlevel 1 (
    echo Error: Python is not installed or not in PATH
    pause
    exit /b 1
)

echo.
echo Checking Silver Layer Status...
echo.

REM Run the silver layer status check
python silver_layer_launcher.py --status

echo.
echo Press any key to exit...
pause >nul
