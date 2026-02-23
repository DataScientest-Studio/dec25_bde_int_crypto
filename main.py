"""
Main entry point for the Crypto Data API.

This module imports the FastAPI app from src.api.main for easy access.
"""

from __future__ import annotations

from src.api.main import app

# This allows running with: fastapi dev main.py
# The app is available as: main:app

if __name__ == "__main__":
    # For direct execution, use the main function from src.api.main
    from src.api.main import main
    main()
