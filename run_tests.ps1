#!/usr/bin/env pwsh
Set-StrictMode -Version Latest

# Create a local virtual environment, install pytest, and run the test suite.
python -m venv .venv
& .\.venv\Scripts\Activate.ps1
python -m pip install -U pip
python -m pip install pytest
pytest -q
