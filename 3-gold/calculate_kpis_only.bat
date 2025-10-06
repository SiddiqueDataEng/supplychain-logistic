@echo off
echo ========================================
echo Gold Layer - Calculate KPIs Only
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
echo Calculating KPIs Only...
echo.

REM Run the gold layer launcher with KPIs only
python gold_layer_launcher.py --kpis-only

echo.
echo Press any key to exit...
pause >nul
