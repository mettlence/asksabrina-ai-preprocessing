#!/bin/bash

# Load environment variables
set -a
source .env
set +a

# Activate virtual environment if exists
if [ -d "venv" ]; then
    source venv/bin/activate
fi

# Run preprocessing
echo "Starting preprocessing..."
python preprocess.py

# Check exit code
if [ $? -eq 0 ]; then
    echo "✅ Preprocessing completed successfully"
else
    echo "❌ Preprocessing failed"
    exit 1
fi