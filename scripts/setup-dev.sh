#!/bin/bash
echo "Setting up BTC Sentiment Analysis development environment..."
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -e .
mkdir -p data/{processed,cache}
echo "✅ Development environment ready!"
echo "To activate: source .venv/bin/activate"