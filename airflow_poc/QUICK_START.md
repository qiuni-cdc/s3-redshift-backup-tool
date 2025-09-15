# Quick Start Guide - Airflow POC (No PostgreSQL Required!)

## Why SQLite for POC?

For a POC, SQLite is perfect because:
- **Zero setup** - It just works
- **No external dependencies** - No PostgreSQL/MySQL needed
- **Lightweight** - Uses a single file for all metadata
- **Sufficient for POC** - Handles sequential task execution fine

## 5-Minute Setup

### Step 1: Install Airflow & dbt

```bash
cd /home/qi_chen/s3-redshift-backup/airflow_poc
./install_airflow_simple.sh
```

This script:
- ✅ Installs Airflow with SQLite (default)
- ✅ Installs dbt-redshift
- ✅ Creates admin user automatically
- ✅ No PostgreSQL needed!

### Step 2: Start Airflow

**Terminal 1:**
```bash
cd /home/qi_chen/s3-redshift-backup/airflow_poc
source airflow_env/bin/activate
export AIRFLOW_HOME=/home/qi_chen/s3-redshift-backup/airflow_poc
airflow webserver --port 8080
```

**Terminal 2:**
```bash
cd /home/qi_chen/s3-redshift-backup/airflow_poc
source airflow_env/bin/activate
export AIRFLOW_HOME=/home/qi_chen/s3-redshift-backup/airflow_poc
airflow scheduler
```

### Step 3: Access Airflow

1. Open: http://localhost:8080
2. Login: **admin** / **admin123**
3. You're ready!

## Configure Connections

### Add Redshift Connection (using your SSH tunnel):

```bash
source airflow_env/bin/activate
export AIRFLOW_HOME=/home/qi_chen/s3-redshift-backup/airflow_poc

# Load your sync tool environment
source /home/qi_chen/s3-redshift-backup/.env.local

# Add Redshift connection
airflow connections add 'redshift_default' \
    --conn-type 'postgres' \
    --conn-host 'localhost' \
    --conn-port 46407 \
    --conn-login "$REDSHIFT_USER" \
    --conn-password "$REDSHIFT_PASSWORD" \
    --conn-schema "$REDSHIFT_DATABASE"
```

### Add AWS Connection (for S3):

```bash
airflow connections add 'aws_default' \
    --conn-type 'aws' \
    --conn-extra "{\"aws_access_key_id\": \"$AWS_ACCESS_KEY_ID\", \"aws_secret_access_key\": \"$AWS_SECRET_ACCESS_KEY\", \"region_name\": \"us-west-2\"}"
```

## That's It! 🎉

You now have:
- ✅ Airflow running with SQLite (no PostgreSQL!)
- ✅ Web UI at http://localhost:8080
- ✅ Ready to create DAGs
- ✅ Can connect to Redshift via your SSH tunnel

## Next: Create Your First DAG

Check the `dags/` directory for the parcel POC DAG!

## Notes

- SQLite limitation: Tasks run sequentially (perfect for POC)
- For production: Upgrade to PostgreSQL for parallel execution
- Airflow metadata stored in: `airflow_poc/airflow.db`