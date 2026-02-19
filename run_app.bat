@echo off
SETLOCAL

REM Change directory to where this .bat file lives
cd /d "%~dp0"

REM Create virtual environment if it doesn't exist
IF NOT EXIST ".venv" (
    echo Creating virtual environment...
    python -m venv .venv
)

REM Activate virtual environment
call ".venv\Scripts\activate"

REM Install requirements (first run only really matters)
pip install -r requirements.txt

REM Run Streamlit
streamlit run app.py

ENDLOCAL
pause
