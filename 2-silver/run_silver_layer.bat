@echo off
echo ========================================
echo Silver Layer Processor for Azure ADLS Gen2
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
echo Starting Silver Layer Processor...
echo.

REM Run the silver layer launcher
python silver_layer_launcher.py

echo.
echo Press any key to exit...
pause >nul
