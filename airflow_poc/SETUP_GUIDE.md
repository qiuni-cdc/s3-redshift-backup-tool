# Airflow & dbt POC Setup Guide

## Prerequisites Check

Before starting, ensure you have:
- Python 3.10+ (you have 3.12.3 ✓)
- PostgreSQL (for Airflow metadata database)
- Git
- AWS CLI configured (for S3 access)

## Step 1: Install PostgreSQL (if not already installed)

```bash
# Check if PostgreSQL is installed
psql --version

# If not installed, install it:
sudo apt update
sudo apt install postgresql postgresql-contrib -y

# Start PostgreSQL service
sudo service postgresql start

# Create database for Airflow
sudo -u postgres psql -c "CREATE DATABASE airflow_db;"
sudo -u postgres psql -c "CREATE USER airflow_user WITH PASSWORD 'airflow_pass';"
sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE airflow_db TO airflow_user;"
```

## Step 2: Install Airflow and dbt

```bash
cd /home/qi_chen/s3-redshift-backup/airflow_poc

# Run the installation script
./install_airflow.sh
```

This will install:
- Apache Airflow 2.8.1 with PostgreSQL and Amazon providers
- dbt-redshift 1.7.0
- Required dependencies

## Step 3: Configure Airflow

### 3.1 Set Environment Variables

```bash
# Add to your ~/.bashrc or create .env file
export AIRFLOW_HOME=/home/qi_chen/s3-redshift-backup/airflow_poc
export AIRFLOW__CORE__DAGS_FOLDER=/home/qi_chen/s3-redshift-backup/airflow_poc/dags
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__EXECUTOR=LocalExecutor
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow_user:airflow_pass@localhost/airflow_db
```

### 3.2 Initialize Airflow Database

```bash
# Activate virtual environment
source airflow_env/bin/activate

# Initialize database
airflow db init

# Create admin user
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin123
```

## Step 4: Configure Connections

### 4.1 Create Redshift Connection

```bash
# Option 1: Using CLI
airflow connections add 'redshift_default' \
    --conn-type 'postgres' \
    --conn-host 'localhost' \
    --conn-port 46407 \
    --conn-login "$REDSHIFT_USER" \
    --conn-password "$REDSHIFT_PASSWORD" \
    --conn-schema "$REDSHIFT_DATABASE"

# Option 2: Using environment variables from sync tool
source /home/qi_chen/s3-redshift-backup/.env.local
airflow connections add 'redshift_default' \
    --conn-type 'postgres' \
    --conn-host 'localhost' \
    --conn-port 46407 \
    --conn-login "$REDSHIFT_USER" \
    --conn-password "$REDSHIFT_PASSWORD" \
    --conn-schema "$REDSHIFT_DATABASE"
```

### 4.2 Create AWS Connection (for S3 completion markers)

```bash
airflow connections add 'aws_default' \
    --conn-type 'aws' \
    --conn-extra "{\"aws_access_key_id\": \"$AWS_ACCESS_KEY_ID\", \"aws_secret_access_key\": \"$AWS_SECRET_ACCESS_KEY\", \"region_name\": \"$AWS_REGION\"}"
```

## Step 5: Start Airflow Services

Open two terminal windows:

**Terminal 1 - Webserver:**
```bash
cd /home/qi_chen/s3-redshift-backup/airflow_poc
source airflow_env/bin/activate
airflow webserver --port 8080
```

**Terminal 2 - Scheduler:**
```bash
cd /home/qi_chen/s3-redshift-backup/airflow_poc
source airflow_env/bin/activate
airflow scheduler
```

## Step 6: Access Airflow UI

1. Open browser: http://localhost:8080
2. Login with: admin / admin123
3. Check connections in Admin → Connections
4. Enable DAGs as needed

## Step 7: Set up dbt Project

```bash
cd /home/qi_chen/s3-redshift-backup/airflow_poc/dbt_projects

# Initialize dbt project
dbt init parcel_analytics

# Configure profiles.yml
mkdir -p ~/.dbt
cat > ~/.dbt/profiles.yml << EOF
parcel_analytics:
  outputs:
    dev:
      type: redshift
      host: localhost
      port: 46407
      user: ${REDSHIFT_USER}
      password: ${REDSHIFT_PASSWORD}
      dbname: ${REDSHIFT_DATABASE}
      schema: analytics_dev
      threads: 4
      keepalives_idle: 0
      sslmode: prefer
    prod:
      type: redshift
      host: localhost
      port: 46407
      user: ${REDSHIFT_USER}
      password: ${REDSHIFT_PASSWORD}
      dbname: ${REDSHIFT_DATABASE}
      schema: analytics
      threads: 4
      keepalives_idle: 0
      sslmode: require
  target: dev
EOF

# Test dbt connection
cd parcel_analytics
dbt debug
```

## Troubleshooting

### PostgreSQL Issues
```bash
# Check PostgreSQL status
sudo service postgresql status

# View PostgreSQL logs
sudo tail -f /var/log/postgresql/postgresql-*.log
```

### Airflow Issues
```bash
# Check Airflow logs
tail -f $AIRFLOW_HOME/logs/scheduler/latest/*.log

# Reset Airflow database (if needed)
airflow db reset
```

### SSH Tunnel for Redshift
The sync tool already sets up SSH tunnel on port 46407. Ensure it's running:
```bash
ps aux | grep ssh | grep 46407
```

## Next Steps

1. Create POC DAG in `dags/` directory
2. Set up dbt models in `dbt_projects/parcel_analytics/`
3. Test end-to-end pipeline
4. Monitor execution in Airflow UI