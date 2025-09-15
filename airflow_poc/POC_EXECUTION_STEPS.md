# POC Execution Steps

## 🚀 Ready to Run the POC

### Step 1: Install Airflow (One-time setup)

```bash
cd /home/qi_chen/s3-redshift-backup/airflow_poc

# Run the installation
./install_airflow_simple.sh
```

This will:
- Install Airflow 2.8.1 with SQLite
- Install dbt-redshift 1.7.0
- Initialize the database
- Create admin user (admin/admin123)

**Expected time: 3-5 minutes**

### Step 2: Verify Installation

```bash
# Check everything is ready
./verify_installation.py
```

You should see all green checkmarks ✅

### Step 3: Start Airflow Services

**Terminal 1 - Web Server:**
```bash
cd /home/qi_chen/s3-redshift-backup/airflow_poc
source airflow_env/bin/activate
export AIRFLOW_HOME=/home/qi_chen/s3-redshift-backup/airflow_poc
airflow webserver --port 8080
```

**Terminal 2 - Scheduler:**
```bash
cd /home/qi_chen/s3-redshift-backup/airflow_poc
source airflow_env/bin/activate
export AIRFLOW_HOME=/home/qi_chen/s3-redshift-backup/airflow_poc
airflow scheduler
```

### Step 4: Configure Connections

**In Terminal 3:**
```bash
cd /home/qi_chen/s3-redshift-backup/airflow_poc
source airflow_env/bin/activate
export AIRFLOW_HOME=/home/qi_chen/s3-redshift-backup/airflow_poc

# Load your sync tool credentials
source /home/qi_chen/s3-redshift-backup/.env.local

# Add Redshift connection (using your SSH tunnel on port 46407)
airflow connections add 'redshift_default' \
    --conn-type 'postgres' \
    --conn-host 'localhost' \
    --conn-port 46407 \
    --conn-login "$REDSHIFT_USER" \
    --conn-password "$REDSHIFT_PASSWORD" \
    --conn-schema "$REDSHIFT_DATABASE"

# Add AWS connection
airflow connections add 'aws_default' \
    --conn-type 'aws' \
    --conn-extra "{\"aws_access_key_id\": \"$AWS_ACCESS_KEY_ID\", \"aws_secret_access_key\": \"$AWS_SECRET_ACCESS_KEY\", \"region_name\": \"us-west-2\"}"
```

### Step 5: Access Airflow UI

1. Open browser: **http://localhost:8080**
2. Login: **admin** / **admin123**
3. You should see the `parcel_detail_poc` DAG

### Step 6: Enable and Run the POC DAG

In the Airflow UI:
1. Find `parcel_detail_poc` DAG
2. Toggle the switch to enable it
3. Click the "▶️ Trigger DAG" button
4. Watch the tasks execute!

## 📊 What the POC DAG Does

1. **sync_parcel_detail**: Runs your sync tool to copy data from MySQL → S3 → Redshift
2. **parse_sync_results**: Reads the JSON output to get metrics
3. **validate_redshift_data**: Checks data actually arrived in Redshift
4. **dbt_transformations**: Placeholder for dbt models (to be configured)
5. **generate_summary_report**: Creates a summary of the run

## 🎯 Expected Results

When successful, you'll see:
- Green tasks in Airflow UI
- Sync metrics in task logs
- Data validation results
- Summary report with row counts

## 🐛 Troubleshooting

### If Airflow won't start:
```bash
# Check for port conflicts
lsof -i :8080

# Check logs
tail -f airflow_poc/logs/scheduler/latest/*.log
```

### If DAG doesn't appear:
```bash
# Check DAG syntax
cd /home/qi_chen/s3-redshift-backup/airflow_poc
source airflow_env/bin/activate
python dags/parcel_detail_poc_dag.py

# Restart scheduler
```

### If Redshift connection fails:
```bash
# Ensure SSH tunnel is running
ps aux | grep ssh | grep 46407

# Test connection manually
psql -h localhost -p 46407 -U $REDSHIFT_USER -d $REDSHIFT_DATABASE
```

## 🎉 Success Criteria

The POC is successful when:
1. ✅ Airflow UI is accessible
2. ✅ DAG runs without errors
3. ✅ Sync tool executes via Airflow
4. ✅ JSON metrics are parsed
5. ✅ Redshift data is validated
6. ✅ Summary report is generated

## Next Steps

After POC success:
1. Configure dbt models for deduplication
2. Add S3 completion markers
3. Enhance monitoring
4. Scale to more tables