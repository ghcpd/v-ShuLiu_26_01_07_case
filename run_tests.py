#!/usr/bin/env python3
"""
Test runner script for the API Engine.

This script sets up a virtual environment, installs dependencies, and runs the test suite.
"""

import os
import subprocess
import sys
import venv
from pathlib import Path

def main():
    project_root = Path(__file__).parent
    venv_dir = project_root / "venv"

    # Create virtual environment if it doesn't exist
    if not venv_dir.exists():
        print("Creating virtual environment...")
        venv.create(venv_dir, with_pip=True)

    # Activate virtual environment
    if sys.platform == "win32":
        python_exe = venv_dir / "Scripts" / "python.exe"
        pip_exe = venv_dir / "Scripts" / "pip.exe"
    else:
        python_exe = venv_dir / "bin" / "python"
        pip_exe = venv_dir / "bin" / "pip"

    # Install pytest if not already installed
    print("Installing dependencies...")
    subprocess.check_call([str(pip_exe), "install", "pytest", "pytest-asyncio"])

    # Run tests
    print("Running tests...")
    result = subprocess.run([str(python_exe), "-m", "pytest", "tests/", "-v"])

    sys.exit(result.returncode)

if __name__ == "__main__":
    main()