# PROD Deployment Plan — Airflow MySQL Sync Pipeline

Detailed implementation plan for deploying the PROD-to-DW MySQL sync pipeline
on the new PROD ETL server.

---

## 1. Server Details

| Item | Value |
|---|---|
| Public IP | `34.223.171.126` |
| Private IP | `10.103.6.19` |
| OS | Ubuntu 22.04.5 LTS |
| RAM | 15 GB |
| CPU | 4 cores |
| Disk | 97 GB (95 GB free) |
| SSH | `ssh -i jasleentung.pem ubuntu@34.223.171.126` |
| Key dir | `C:\Users\Jasleen Tung\Downloads\jasleentung_keypair\` |
| UID (ubuntu) | 1000 |

### Network Access

| Target | Hostname | IP | Port | Reachable? |
|---|---|---|---|---|
| PROD MySQL (RDS Proxy) | `mgt-coc.rds-proxy.us-west-2.ro.db.uniuni.com.internal` | `10.103.7.92 / 10.103.8.192` | 3306 | **Yes** |
| DW MySQL (RDS Proxy) | `mgmt-coc.rds-proxy.db.analysis.uniuni.com.internal` | `10.103.5.124 / 10.103.4.192` | 3306 | **Yes** |

RDS Proxy endpoints are in the same `10.103.x.x` subnet as the server — **no SSH tunnels needed**.
Set `USE_SSH_TUNNEL=false` in `.env`.

---

## 2. Version Matching — QA vs PROD

Both environments must run identical versions to avoid behavioral differences.

| Component | QA (current) | PROD (target) | Match? |
|---|---|---|---|
| Airflow | 2.8.1 | 2.8.1 | Yes |
| Python | 3.8.18 | 3.8.18 (via Airflow image) | Yes |
| Postgres (metadata DB) | 13 | 13 | Yes |
| Docker image | `apache/airflow:2.8.1` | `uniuni-airflow:2.8.1` (custom, based on same image) | Yes |
| mysql-connector-python | 8.0.29 | 8.0.29 (pinned in Dockerfile) | Yes |
| boto3 | 1.33.13 | 1.33.13 (pinned in Dockerfile) | Yes |
| Executor | LocalExecutor | LocalExecutor | Yes |
| DAG file | `prod_to_dw_mysql_sync_hourly.py` | Same file | Yes |
| Backfill script | `backfill_large_tables.py` | Same file | Yes |

---

## 3. Improvements Over QA

| Area | QA (current) | PROD (improved) |
|---|---|---|
| Package install | `_PIP_ADDITIONAL_REQUIREMENTS` (slow, reinstalls on every restart) | Custom Docker image with pinned versions (fast, consistent) |
| Resource limits | None (OOM crashed server) | Memory + CPU limits per container |
| Passwords | Hardcoded in docker-compose.yml | `.env` file with `chmod 600` |
| Port | 8081 (non-standard) | 8080 (standard) |
| Auto-restart | `restart: always` only | systemd service + `restart: always` |
| Postgres backup | None (lost data on reboot) | Daily pg_dump cron job |
| Unused packages | 20+ packages installed | Only 2 packages needed (`mysql-connector-python`, `boto3`) |
| Full-sync tasks | Parallel (already improved) | Parallel (same) |
| `docker-compose.yml` | Has `version: '3.8'` (obsolete warning) | Removed |
| Health checks | Webserver + Postgres | Webserver + Postgres (same) |

---

## 4. File Structure on PROD

```
/home/ubuntu/airflow/
  docker-compose.yml      # Airflow services (PROD config)
  Dockerfile              # Custom Airflow image
  .env                    # Secrets (chmod 600)
  dags/
    prod_to_dw_mysql_sync_hourly.py   # The sync DAG
  scripts/
    backfill_large_tables.py          # One-time backfill script
  logs/                               # Airflow task logs (auto-created)
```

---

## 5. Configuration Files

### 5.1 Dockerfile

```dockerfile
FROM apache/airflow:2.8.1

# Pin exact versions to match QA
RUN pip install --no-cache-dir \
    mysql-connector-python==8.0.29 \
    boto3==1.33.13
```

### 5.2 .env

```bash
# Airflow
AIRFLOW_UID=1000

# Source — PROD MySQL (RDS Proxy, read-only)
DB_USER=jasleentung
DB_US_PROD_RO_PASSWORD=86a05b5772c55c83c9db74a01c01441f
DB_PROD_HOST=mgt-coc.rds-proxy.us-west-2.ro.db.uniuni.com.internal
DB_PROD_PORT=3306

# Target — DW MySQL (RDS Proxy, write)
DB_DW_WRITE_HOST=mgmt-coc.rds-proxy.db.analysis.uniuni.com.internal
DB_DW_WRITE_PORT=3306
DB_DW_WRITE_PASSWORD=b33c6fe2b4857c948f818f7260c9c304

# No SSH tunnel needed — RDS Proxy endpoints are in same subnet
USE_SSH_TUNNEL=false

# Email Alerts
ALERT_SMTP_HOST=smtp.office365.com
ALERT_SMTP_PORT=587
ALERT_SMTP_USER=jasleen.tung@uniuni.com
ALERT_SMTP_PASSWORD=Guruji@2312ninety
SYNC_ALERT_EMAILS=jasleen.tung@uniuni.com

# Airflow admin (change from QA default)
AIRFLOW_ADMIN_PASSWORD=Pr0d@irfl0w2026
```

### 5.3 docker-compose.yml

```yaml
x-airflow-common: &airflow-common
  image: uniuni-airflow:2.8.1
  user: "${AIRFLOW_UID:-1000}:0"
  environment:
    # Airflow core
    - AIRFLOW__CORE__EXECUTOR=LocalExecutor
    - AIRFLOW__CORE__LOAD_EXAMPLES=false
    - AIRFLOW__CORE__PARALLELISM=16
    - AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG=16
    - AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres/airflow
    - AIRFLOW__WEBSERVER__SECRET_KEY=${AIRFLOW_SECRET_KEY:-prod_secret_key_change_me}

    # DAG environment variables (from .env)
    - DB_USER=${DB_USER}
    - DB_US_PROD_RO_PASSWORD=${DB_US_PROD_RO_PASSWORD}
    - DB_PROD_HOST=${DB_PROD_HOST}
    - DB_PROD_PORT=${DB_PROD_PORT}
    - DB_DW_WRITE_HOST=${DB_DW_WRITE_HOST}
    - DB_DW_WRITE_PORT=${DB_DW_WRITE_PORT}
    - DB_DW_WRITE_PASSWORD=${DB_DW_WRITE_PASSWORD}
    - USE_SSH_TUNNEL=${USE_SSH_TUNNEL:-false}
    - ALERT_SMTP_HOST=${ALERT_SMTP_HOST}
    - ALERT_SMTP_PORT=${ALERT_SMTP_PORT}
    - ALERT_SMTP_USER=${ALERT_SMTP_USER}
    - ALERT_SMTP_PASSWORD=${ALERT_SMTP_PASSWORD}
    - SYNC_ALERT_EMAILS=${SYNC_ALERT_EMAILS}

  volumes:
    - ./dags:/opt/airflow/dags
    - ./scripts:/opt/airflow/scripts
    - ./logs:/opt/airflow/logs
  depends_on:
    postgres:
      condition: service_healthy

services:
  postgres:
    image: postgres:13
    environment:
      - POSTGRES_USER=airflow
      - POSTGRES_PASSWORD=airflow
      - POSTGRES_DB=airflow
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "airflow"]
      interval: 5s
      retries: 5
    volumes:
      - postgres-db-volume:/var/lib/postgresql/data
    deploy:
      resources:
        limits:
          memory: 1G
          cpus: '0.5'

  airflow-init:
    <<: *airflow-common
    entrypoint: /bin/bash
    command:
      - -c
      - |
        airflow db init
        airflow users create \
          --username admin \
          --password ${AIRFLOW_ADMIN_PASSWORD:-admin123} \
          --firstname Admin \
          --lastname User \
          --role Admin \
          --email admin@example.com
    depends_on:
      postgres:
        condition: service_healthy

  airflow-webserver:
    <<: *airflow-common
    command: webserver
    restart: always
    ports:
      - "8080:8080"
    healthcheck:
      test: ["CMD", "curl", "--fail", "http://localhost:8080/health"]
      interval: 10s
      timeout: 10s
      retries: 5
    deploy:
      resources:
        limits:
          memory: 2G
          cpus: '1'

  airflow-scheduler:
    <<: *airflow-common
    command: scheduler
    restart: always
    deploy:
      resources:
        limits:
          memory: 4G
          cpus: '2'

volumes:
  postgres-db-volume:
```

**Key differences from QA:**
- No `version: '3.8'`
- Custom image (`uniuni-airflow:2.8.1`) instead of `apache/airflow:2.8.1`
- No `_PIP_ADDITIONAL_REQUIREMENTS`
- All passwords from `.env` (not hardcoded)
- Resource limits on all containers
- Port `8080:8080` (standard)
- `AIRFLOW__CORE__PARALLELISM=16` and `MAX_ACTIVE_TASKS_PER_DAG=16` (supports parallel full-sync tasks)
- No SSH tunnel config or key mounts (direct RDS Proxy access)
- No unnecessary volume mounts (no `sync_tool`, no `dbt_projects`)

---

## 6. Pipeline Overview

### Tables Synced (11 total)

**Full Sync (8 tables — DELETE + INSERT, all run in parallel):**

| # | Source | Target | Rows | Sync Type |
|---|---|---|---|---|
| 1 | `kuaisong.uni_pattern_config` | `uniods.uni_pattern_config` | ~57 | Full |
| 2 | `kuaisong.uni_warehouses` | `uniods.parcel_tool_warehouses` | ~76 | Full |
| 3 | `kuaisong.uni_customer` | `uniods.uni_customer` | ~3K | Full |
| 4 | `kuaisong.uni_zipcodes` | `uniods.parcel_tool_zipcodes` | ~10K | Full |
| 5 | `kuaisong.uni_mawb_box` | `uniods.uni_mawb_box` | ~31K | Full |
| 6 | `kuaisong.ecs_staff` | `uniods.ecs_staff` | ~158K | Full |
| 7 | `kuaisong.uni_prealert_info` | `uniods.uni_prealert_info` | ~284K | Full |
| 8 | `driver_app.uni_common_dict` | `uniods.uni_common_dict` | ~116 | Full |

**Incremental Sync (3 tables — append-only via ID watermark, sequential):**

| # | Source | Target | Rows | Sync Type |
|---|---|---|---|---|
| 9 | `kuaisong.uni_tracking_addon_spath` | `uniods.uni_tracking_addon_spath` | ~15M | Incremental |
| 10 | `kuaisong.uni_prealert_order` | `uniods.uni_prealert_order` | ~29M | Incremental |
| 11 | `kuaisong.order_details` | `uniods.order_details` | ~180M | Incremental |

### Task Execution Flow

```
                   ┌─ sync_uni_pattern_config ─┐
                   ├─ sync_uni_warehouses ──────┤
                   ├─ sync_uni_customer ────────┤
                   ├─ sync_uni_zipcodes ────────┤  (parallel)
                   ├─ sync_uni_mawb_box ────────┤
                   ├─ sync_ecs_staff ───────────┤
                   ├─ sync_uni_prealert_info ───┤
                   └─ sync_uni_common_dict ─────┘
                              │
                              ▼
                   incr_uni_tracking_addon_spath  (sequential)
                              │
                              ▼
                   incr_uni_prealert_order
                              │
                              ▼
                   incr_order_details
                              │
                              ▼
                        print_summary
```

### Schedule

- **Frequency:** Every hour (`0 * * * *`)
- **Max active runs:** 1 (prevents overlap)
- **Execution timeout:** 50 minutes per task
- **Retries:** 1 (5 min delay)

---

## 7. Git Merge & Code Deployment Strategy

### Current Git State

| Item | Value |
|---|---|
| Working branch | `jasleen_prod_dw_sync` |
| Commits ahead of main | 16 |
| Files changed | 2 (`prod_to_dw_mysql_sync_hourly.py`, `backfill_large_tables.py`) |
| All changes are additive | Yes (no modifications to existing files) |

### Merge Plan

```bash
# 1. Ensure branch is up to date with remote
git checkout jasleen_prod_dw_sync
git push origin jasleen_prod_dw_sync

# 2. Create PR for review
gh pr create --base main --head jasleen_prod_dw_sync \
    --title "Add PROD-to-DW MySQL sync DAG + backfill script" \
    --body "Phase 1+2: 8 full-sync + 3 incremental tables. Tested on QA."

# 3. After PR approval, merge to main
gh pr merge --squash

# 4. Pull main locally
git checkout main
git pull origin main
```

### Deploying DAG to PROD Server

After merge to main, the PROD server gets files from main branch:

```bash
# Option A: Clone repo on PROD server (recommended — enables future git pull)
ssh -i jasleentung.pem ubuntu@34.223.171.126
cd /home/ubuntu
git clone https://github.com/<org>/Main-s3-redshift-backup-tool.git repo
# Then symlink DAG and scripts into Airflow dirs:
ln -s /home/ubuntu/repo/s3-redshift-backup-tool/airflow_poc/dags/prod_to_dw_mysql_sync_hourly.py \
      /home/ubuntu/airflow/dags/prod_to_dw_mysql_sync_hourly.py
ln -s /home/ubuntu/repo/s3-redshift-backup-tool/airflow_poc/scripts/backfill_large_tables.py \
      /home/ubuntu/airflow/scripts/backfill_large_tables.py

# Option B: SCP files directly (simpler, but manual updates needed)
cd "C:\Users\Jasleen Tung\Downloads\jasleentung_keypair"
scp -i jasleentung.pem \
    "C:\Users\Jasleen Tung\Documents\GitHub\Main-s3-redshift-backup-tool\s3-redshift-backup-tool\airflow_poc\dags\prod_to_dw_mysql_sync_hourly.py" \
    ubuntu@34.223.171.126:/home/ubuntu/airflow/dags/
scp -i jasleentung.pem \
    "C:\Users\Jasleen Tung\Documents\GitHub\Main-s3-redshift-backup-tool\s3-redshift-backup-tool\airflow_poc\scripts\backfill_large_tables.py" \
    ubuntu@34.223.171.126:/home/ubuntu/airflow/scripts/
```

**Recommendation:** Use Option A (git clone + symlinks). Future DAG updates become a simple `git pull` on the server instead of manual SCP.

### QA to PROD Cutover

Both QA and PROD write to the **same DW database** (`db.analysis.uniuni.com.internal`).
To avoid duplicate writes:

| Step | Action |
|---|---|
| 1 | Verify PROD DAG run succeeds (manual trigger) |
| 2 | Verify watermarks and row counts match between QA and PROD runs |
| 3 | **Pause QA DAG** — `airflow dags pause prod_to_dw_mysql_sync_hourly` on QA |
| 4 | **Unpause PROD DAG** — `airflow dags unpause prod_to_dw_mysql_sync_hourly` on PROD |
| 5 | Monitor first 2-3 hourly PROD runs |
| 6 | Once stable, QA DAG can remain paused (keep as fallback) |

---

## 8. Server Setup & Deployment Steps

### Step 1: Install Docker (5 min)

```bash
ssh -i jasleentung.pem ubuntu@34.223.171.126

# Install Docker
sudo apt-get update
sudo apt-get install -y docker.io docker-compose-plugin

# Add ubuntu user to docker group (no sudo needed for docker commands)
sudo usermod -aG docker ubuntu

# Log out and back in for group change to take effect
exit
ssh -i jasleentung.pem ubuntu@34.223.171.126

# Verify
docker --version
docker compose version
```

### Step 2: Create Directory Structure (1 min)

```bash
mkdir -p /home/ubuntu/airflow/{dags,scripts,logs}
cd /home/ubuntu/airflow
```

### Step 3: Copy Files from Local (2 min)

From local machine:
```bash
cd "C:\Users\Jasleen Tung\Downloads\jasleentung_keypair"

# Copy Dockerfile
scp -i jasleentung.pem <local_dockerfile> ubuntu@34.223.171.126:/home/ubuntu/airflow/Dockerfile

# Copy docker-compose.yml
scp -i jasleentung.pem <local_compose> ubuntu@34.223.171.126:/home/ubuntu/airflow/docker-compose.yml

# Copy .env
scp -i jasleentung.pem <local_env> ubuntu@34.223.171.126:/home/ubuntu/airflow/.env

# Copy DAG
scp -i jasleentung.pem <local_dag> ubuntu@34.223.171.126:/home/ubuntu/airflow/dags/prod_to_dw_mysql_sync_hourly.py

# Copy backfill script
scp -i jasleentung.pem <local_backfill> ubuntu@34.223.171.126:/home/ubuntu/airflow/scripts/backfill_large_tables.py
```

### Step 4: Build Custom Docker Image (3 min)

```bash
cd /home/ubuntu/airflow
docker build -t uniuni-airflow:2.8.1 .
```

### Step 5: Set Permissions (1 min)

```bash
chmod 600 .env
```

### Step 6: Initialize Airflow (2 min)

```bash
cd /home/ubuntu/airflow

# Start postgres first
docker compose up -d postgres

# Wait for postgres to be healthy
sleep 10

# Initialize Airflow DB + create admin user
docker compose run --rm airflow-init

# Start all services
docker compose up -d
```

### Step 7: Verify (1 min)

```bash
# Check containers are running
docker compose ps

# Check health
curl http://localhost:8080/health

# Check DAG is visible
docker compose exec airflow-scheduler airflow dags list | grep prod_to_dw_mysql_sync

# Open web UI: http://34.223.171.126:8080
# Login: admin / Pr0d@irfl0w2026
```

### Step 8: Test DAG Run (5 min)

```bash
# Trigger a manual run
docker compose exec airflow-scheduler airflow dags trigger prod_to_dw_mysql_sync_hourly

# Watch task states
docker compose exec airflow-scheduler airflow tasks states-for-dag-run \
    prod_to_dw_mysql_sync_hourly <run_id>
```

Verify:
- All 8 full-sync tasks succeed
- All 3 incremental tasks succeed (will show 0 rows if watermarks are already caught up)
- `print_summary` shows results

### Step 9: Run Backfill for Large Tables (6-10 hours total)

Only needed if the DW tables on PROD are empty or behind.

```bash
# Run all 3 in separate terminals (or sequentially with nohup)

# Terminal 1 (~1-2 hours)
docker compose exec -e PYTHONUNBUFFERED=1 airflow-scheduler \
    python3 -u /opt/airflow/scripts/backfill_large_tables.py uni_tracking_addon_spath

# Terminal 2 (~2-3 hours)
docker compose exec -e PYTHONUNBUFFERED=1 airflow-scheduler \
    python3 -u /opt/airflow/scripts/backfill_large_tables.py uni_prealert_order

# Terminal 3 (~5-7 hours)
docker compose exec -e PYTHONUNBUFFERED=1 airflow-scheduler \
    python3 -u /opt/airflow/scripts/backfill_large_tables.py order_details

# Or use nohup to run in background:
nohup docker compose exec -T -e PYTHONUNBUFFERED=1 airflow-scheduler \
    python3 -u /opt/airflow/scripts/backfill_large_tables.py uni_tracking_addon_spath \
    > /tmp/backfill_spath.log 2>&1 &
```

Monitor progress:
```bash
tail -f /tmp/backfill_spath.log
```

### Step 10: Set Up systemd (2 min)

```bash
sudo tee /etc/systemd/system/airflow.service > /dev/null <<'EOF'
[Unit]
Description=Airflow Docker Compose
Requires=docker.service
After=docker.service

[Service]
Type=oneshot
RemainAfterExit=yes
WorkingDirectory=/home/ubuntu/airflow
ExecStart=/usr/bin/docker compose up -d
ExecStop=/usr/bin/docker compose down
TimeoutStartSec=120

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable airflow
```

### Step 11: Set Up Postgres Backup (2 min)

```bash
mkdir -p /home/ubuntu/backups

sudo tee /etc/cron.d/airflow-pg-backup > /dev/null <<'EOF'
0 2 * * * root docker exec airflow-postgres-1 pg_dump -U airflow airflow | gzip > /home/ubuntu/backups/airflow_pg_$(date +\%Y\%m\%d).sql.gz
EOF
```

### Step 12: Unpause DAG (1 min)

```bash
docker compose exec airflow-scheduler airflow dags unpause prod_to_dw_mysql_sync_hourly
```

---

## 9. Post-Deployment Verification

| Check | Command | Expected |
|---|---|---|
| Containers running | `docker compose ps` | 3 containers (scheduler, webserver, postgres) all healthy |
| Web UI accessible | `curl http://localhost:8080/health` | `{"status": "healthy"}` |
| DAG visible | `airflow dags list` | `prod_to_dw_mysql_sync_hourly` |
| DAG unpaused | `airflow dags list` | `paused: False` |
| Manual run succeeds | Trigger + check states | All 11 tasks succeed |
| Watermarks correct | `SELECT * FROM uniods.sync_watermarks` | 3 rows with recent `last_sync_at` |
| Row counts match | `SELECT COUNT(*) FROM uniods.<table>` | Match source counts |
| Hourly schedule works | Wait 1 hour | New run auto-triggered |
| systemd works | `sudo systemctl status airflow` | Active |
| Logs writable | Check `./logs/` directory | Log files appear |

---

## 10. Future Improvements (after stable deployment)

| Improvement | Priority | Description |
|---|---|---|
| ~~VPC peering~~ | ~~High~~ | ~~Resolved~~ — DevOps provided RDS Proxy endpoints in same subnet |
| Monitoring/alerting | Medium | Add Airflow email on failure (`email_on_failure: True`), or integrate with Slack/PagerDuty |
| Log rotation | Low | Add `AIRFLOW__LOGGING__REMOTE_LOGGING=true` with S3, or local logrotate |
| DAG versioning | Low | Git-sync sidecar to auto-deploy DAGs from Git instead of manual SCP |

---

## 11. Rollback Plan

If deployment fails or causes issues:

```bash
# Stop everything
cd /home/ubuntu/airflow
docker compose down

# Remove systemd service
sudo systemctl stop airflow
sudo systemctl disable airflow
sudo rm /etc/systemd/system/airflow.service
sudo systemctl daemon-reload

# Clean up (optional)
docker system prune -a
rm -rf /home/ubuntu/airflow
```

The QA instance remains running as a fallback. The DW database is shared between
QA and PROD — both can write to it. Ensure only one environment is writing at a time
to avoid conflicts (pause QA DAG before enabling PROD DAG).
