#!/bin/bash

# Test runner script for roster ingestion pipeline

set -e

echo "Activating virtual environment..."
source venv/bin/activate

echo "Running tests with coverage..."
python -m pytest tests/ -v --cov=src --cov=utils --cov=constants --cov-report=term-missing --cov-report=html

echo ""
echo "Test run complete!"
echo "Coverage report saved to htmlcov/index.html"

