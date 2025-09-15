#!/bin/bash
# Simplified Airflow installation for POC - Using SQLite

echo "=== Installing Airflow and dbt for POC (Simplified with SQLite) ==="

# Set Airflow home
export AIRFLOW_HOME=/home/qi_chen/s3-redshift-backup/airflow_poc

# Activate virtual environment
source airflow_env/bin/activate

# Upgrade pip
pip install --upgrade pip

# Install Airflow with minimal providers (no postgres needed)
AIRFLOW_VERSION=2.8.1
PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

echo "Installing Apache Airflow ${AIRFLOW_VERSION}..."
pip install "apache-airflow[amazon]==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

# Install dbt-redshift
echo "Installing dbt-redshift..."
pip install dbt-redshift==1.7.0

# Install additional useful packages
pip install pandas pyarrow

# Set sequential executor for SQLite
export AIRFLOW__CORE__EXECUTOR=SequentialExecutor
export AIRFLOW__CORE__LOAD_EXAMPLES=False

echo "=== Installation complete ==="

# Initialize SQLite database (default)
echo "Initializing Airflow with SQLite database..."
airflow db init

# Create admin user
echo "Creating admin user..."
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin123

echo ""
echo "✅ Setup complete! Using SQLite (no PostgreSQL needed)"
echo ""
echo "To start Airflow:"
echo "1. Terminal 1: source airflow_env/bin/activate && airflow webserver --port 8080"
echo "2. Terminal 2: source airflow_env/bin/activate && airflow scheduler"
echo ""
echo "Login: admin / admin123"