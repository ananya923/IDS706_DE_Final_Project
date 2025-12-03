#!/bin/bash

# Exit if any command fails
set -e

echo "🚀 Starting Ethereum ingestion pipeline..."

# Activate virtual environment (if you have one)
if [ -d "venv" ]; then
    echo "Activating virtual environment..."
    source venv/bin/activate
fi

echo "Running Etherscan → S3 ingestion script..."

# Run the Python ingestion job
python3 glue_jobs/etherscan_to_s3_glue.py

echo "✅ Ingestion completed successfully!"
