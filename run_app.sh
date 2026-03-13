#!/usr/bin/env bash
set -e

# Change directory to where this script lives
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Create virtual environment if it doesn't exist
if [ ! -d ".venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv .venv
fi

# Activate virtual environment
source ".venv/bin/activate"

# Install requirements (first run only really matters)
pip install -r requirements.txt

# Run Streamlit
exec streamlit run app.py
