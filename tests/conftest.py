import os
import sys


# Ensure the project root is on sys.path when running pytest so tests can import
# the top-level modules like `api_engine` regardless of working directory.
ROOT = os.path.dirname(os.path.dirname(__file__))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
