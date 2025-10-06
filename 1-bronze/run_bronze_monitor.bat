@echo off
echo ========================================
echo Bronze Layer Real-time Monitor
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
echo Starting Bronze Layer Real-time Monitor...
echo Press Ctrl+C to stop monitoring
echo.

REM Run the bronze layer monitor
python bronze_layer_launcher.py --mode monitor

echo.
echo Monitor stopped.
pause
