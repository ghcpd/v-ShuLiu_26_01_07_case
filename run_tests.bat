@echo off
REM Test runner script for API Engine refactor on Windows

setlocal enabledelayedexpansion

set VENV_DIR=.venv

REM Create virtual environment if it doesn't exist
if not exist "%VENV_DIR%" (
    echo Creating virtual environment...
    python -m venv "%VENV_DIR%"
)

REM Install dependencies
echo Installing dependencies...
"%VENV_DIR%\Scripts\pip.exe" install -q pytest pytest-asyncio

REM Run tests
echo.
echo Running tests...
"%VENV_DIR%\Scripts\python.exe" -m pytest tests/test_api_engine.py -v

endlocal
