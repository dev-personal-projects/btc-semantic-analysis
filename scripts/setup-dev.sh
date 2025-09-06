#!/bin/bash

echo "Setting up BTC Sentiment Analysis development environment..."

# Create virtual environment
python3 -m venv /workspace/venv

# Activate virtual environment and install dependencies
source /workspace/venv/bin/python
/workspace/venv/bin/pip install --upgrade pip

# Install project in editable mode
/workspace/venv/bin/pip install -e .

# Create initial folder structure
mkdir -p {data/{raw,processed,exports,cache},notebooks,tests,config}

echo "✅ Development environment ready!"
echo "Python virtual environment: /workspace/venv"
echo "To activate: source /workspace/venv/bin/activate"