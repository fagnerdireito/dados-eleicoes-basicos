#!/bin/bash

# Configuration
export DATA_DIR="/Users/fagnerdossgoncalves/wwwroot/lab/elections" # Adjust as needed
export DB_CONNECTION=mysql
export DB_HOST=127.0.0.1
export DB_PORT=3306
export DB_DATABASE=eleicoes
export DB_USERNAME=root
export DB_PASSWORD=

# Activate virtual environment if exists, else assume python is available
source venv/bin/activate

echo "Starting Election Data Pipeline..."
python3 python/election_data_pipeline/scripts/run_pipeline.py --data_dir "$DATA_DIR"
