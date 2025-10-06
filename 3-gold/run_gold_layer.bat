@echo off
echo ========================================
echo Gold Layer Processor for Azure ADLS Gen2
echo ========================================
echo.

REM Check if Python is installed
python --version >nul 2>&1
if errorlevel 1 (
    echo Error: Python is not installed or not in PATH
    pause
    exit /b 1
)

REM Install required packages if needed
echo Installing required packages...
pip install -r ../../requirements.txt

echo.
echo Starting Gold Layer Processor...
echo.

REM Run the gold layer launcher
python gold_layer_launcher.py

echo.
echo Press any key to exit...
pause >nul
