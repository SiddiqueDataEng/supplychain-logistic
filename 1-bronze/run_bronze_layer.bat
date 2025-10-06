@echo off
echo ========================================
echo Bronze Layer Processor for Azure ADLS Gen2
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
echo Starting Bronze Layer Processor...
echo.

REM Run the bronze layer launcher
python bronze_layer_launcher.py --mode scan

echo.
echo Press any key to exit...
pause >nul
