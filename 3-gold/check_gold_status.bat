@echo off
echo ========================================
echo Gold Layer Status Check
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
echo Checking Gold Layer Status...
echo.

REM Run the gold layer status check
python gold_layer_launcher.py --status

echo.
echo Press any key to exit...
pause >nul
