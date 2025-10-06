@echo off
echo ========================================
echo Gold Layer - Create Facts Only
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
echo Creating Fact Tables Only...
echo.

REM Run the gold layer launcher with facts only
python gold_layer_launcher.py --facts-only

echo.
echo Press any key to exit...
pause >nul
