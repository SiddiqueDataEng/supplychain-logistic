@echo off
echo Starting Supply Chain Analytics Dashboard...
echo.

REM Check if Python is installed
python --version >nul 2>&1
if errorlevel 1 (
    echo Error: Python is not installed or not in PATH
    echo Please install Python 3.8+ and try again
    pause
    exit /b 1
)

REM Install requirements if needed
echo Installing/updating requirements...
pip install -r requirements.txt --quiet

REM Start Streamlit app
echo Starting Streamlit application...
echo.
echo Dashboard will be available at: http://localhost:8501
echo Press Ctrl+C to stop the dashboard
echo.

python -m streamlit run app.py --server.port 8501 --server.address localhost

pause
