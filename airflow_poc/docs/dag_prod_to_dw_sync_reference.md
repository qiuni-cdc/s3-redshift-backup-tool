# DAG Reference — prod_to_dw_mysql_sync_hourly

Skills/reference file for the PROD-to-DW MySQL sync DAG. Use this as the single source of truth for connections, configuration, deployment, and troubleshooting.

---

## Overview

| Item | Value |
|---|---|
| DAG ID | `prod_to_dw_mysql_sync_hourly` |
| Schedule | Every hour (`0 * * * *`) |
| Source | PROD MySQL (`kuaisong`, `driver_app` schemas) |
| Target | DW MySQL (`uniods` schema) |
| Tables | 7 full-sync + 3 incremental = 10 total |
| Watermarks | `uniods.sync_watermarks` table |
| DAG file | `airflow_poc/dags/prod_to_dw_mysql_sync_hourly.py` |
| Backfill script | `airflow_poc/scripts/backfill_large_tables.py` |

---

## Tables

### Full Sync (7 tables — DELETE + INSERT, parallel)

| Source | Target | Rows | Notes |
|---|---|---|---|
| `kuaisong.uni_pattern_config` | `uniods.uni_pattern_config` | ~68 | |
| `kuaisong.uni_warehouses` | `uniods.parcel_tool_warehouses` | ~76 | Different target name |
| `kuaisong.uni_customer` | `uniods.uni_customer` | ~3K | |
| `kuaisong.uni_zipcodes` | `uniods.parcel_tool_zipcodes` | ~10K | Different target name |
| `kuaisong.uni_mawb_box` | `uniods.uni_mawb_box` | ~31K | |
| `kuaisong.ecs_staff` | `uniods.ecs_staff` | ~158K | |
| `driver_app.uni_common_dict` | `uniods.uni_common_dict` | ~116 | Different source schema |

### Incremental Sync (3 tables — INSERT IGNORE via ID watermark, sequential)

| Source | Target | Rows | ID column |
|---|---|---|---|
| `kuaisong.uni_prealert_info` | `uniods.uni_prealert_info` | ~325K+ | `id` (auto-increment) |
| `kuaisong.uni_tracking_addon_spath` | `uniods.uni_tracking_addon_spath` | ~15M | `id` |
| `kuaisong.order_details` | `uniods.order_details` | ~180M | `id` |

### Task Execution Flow

```
         ┌─ sync_uni_pattern_config ─┐
         ├─ sync_uni_warehouses ──────┤
         ├─ sync_uni_customer ────────┤
         ├─ sync_uni_zipcodes ────────┤  (parallel)
         ├─ sync_uni_mawb_box ────────┤
         ├─ sync_ecs_staff ───────────┤
         └─ sync_uni_common_dict ─────┘
                    │
                    ▼
         incr_uni_prealert_info        (sequential)
                    │
                    ▼
         incr_uni_tracking_addon_spath
                    │
                    ▼
         incr_order_details
                    │
                    ▼
         quality_gate                   (blocks downstream if any sync is not clean)
                    │
                    ▼
         print_summary                  (runs always via trigger_rule=all_done)
```

---

## Features

### Schema Validation
- Compares source and target columns (name, type, order) before every sync
- Returns `(status, detail)` tuple: `"match"`, `"mismatch"`, or `"missing"`

### Auto-Recreate Target (Full-Sync Only)
- On mismatch/missing: drops target table, recreates from source DDL via `SHOW CREATE TABLE`
- Rewrites schema/table name in DDL using regex
- Sends info email with mismatch details (e.g., "Columns in target but not source: {'_test_col'}")
- Safe because full-sync does DELETE + INSERT anyway (no data loss)
- Requires `etl-admin-service` user with CREATE/DROP grants on `uniods.*`

### Incremental Sync — Alert Only
- On mismatch: sends alert email, syncs only shared columns, pushes `status: "partial_sync"` to XCom
- On missing: sends alert email, skips sync
- Does NOT auto-recreate (would lose watermark position)

### Quality Gate
- Runs after all sync tasks, before `print_summary`
- Checks XCom status of every full-sync and incremental task
- **Passes** only if all statuses are `"success"`
- **Fails** (raises `RuntimeError`) on: `count_mismatch`, `partial_sync`, `failed`, `skipped`, `no_result`
- Sends email alert listing all issues when gate fails
- Blocks downstream tasks (future: dbt, customer pipeline)
- `print_summary` still runs regardless (`trigger_rule=all_done`)

### Row Count Verification (Full-Sync)
- After DELETE + INSERT, compares source vs target row count
- On mismatch: pushes `status: "count_mismatch"` to XCom (instead of `"success"`)
- Sends email alert with both counts and difference
- Quality gate will block downstream on this status

### Email Alerts
- SMTP via Office 365 (`smtp.office365.com:587`)
- Sent on: schema recreate (full-sync), schema mismatch (incremental), row count mismatch (full-sync), quality gate failure, errors
- Config: `ALERT_SMTP_*` and `SYNC_ALERT_EMAILS` env vars
- Fails silently if SMTP not configured (no crash)

### Watermarks (Incremental Tables)
- Stored in `uniods.sync_watermarks` table
- Columns: `table_name`, `last_id`, `last_batch_rows`, `last_sync_at`
- Tracks last synced auto-increment `id`
- On first run (no watermark): starts from `id = 0`, uses `INSERT IGNORE` (safe for duplicates)

---

## Connections

### DB Credentials — `etl-admin-service`

| Item | Source (PROD MySQL) | Target (DW MySQL) |
|---|---|---|
| User | `etl-admin-service` | `etl-admin-service` |
| Password | `B8Dv8LxJGMr0y2a7` | `Qe97QJaeD246yHDo` |

### Environment Variables

The DAG reads credentials from environment variables. `os.environ` (docker-compose) takes precedence over the `.env` file at `SYNC_TOOL_PATH/.env`.

| Env Var | Description |
|---|---|
| `DB_USER` | MySQL username (same for source and target) |
| `DB_US_PROD_RO_PASSWORD` | PROD MySQL password |
| `DB_PROD_HOST` | PROD MySQL hostname |
| `DB_PROD_PORT` | PROD MySQL port (default 3306) |
| `DB_DW_WRITE_HOST` | DW MySQL hostname |
| `DB_DW_WRITE_PORT` | DW MySQL port (default 3306) |
| `DB_DW_WRITE_PASSWORD` | DW MySQL password |
| `USE_SSH_TUNNEL` | `true` for QA, `false` for PROD |
| `SSH_BASTION_HOST` | Bastion IP (QA only) |
| `SSH_BASTION_USER` | Bastion SSH user (QA only) |
| `SSH_BASTION_KEY_PATH` | SSH key path inside container (QA only) |
| `ALERT_SMTP_HOST` | SMTP server |
| `ALERT_SMTP_PORT` | SMTP port |
| `ALERT_SMTP_USER` | SMTP login / From address |
| `ALERT_SMTP_PASSWORD` | SMTP password |
| `SYNC_ALERT_EMAILS` | Comma-separated alert recipients |

---

## QA Environment

| Item | Value |
|---|---|
| QA Server | `ubuntu@10.101.1.187` via bastion `35.82.216.244` |
| SSH command | `ssh -o "ProxyCommand=ssh -W %h:%p -i jasleentung.pem jasleentung@35.82.216.244" ubuntu@10.101.1.187 -i "ubuntu@etl.uniuni.pem"` |
| Key directory | `C:\Users\Jasleen Tung\Downloads\jasleentung_keypair\` |
| Repo path | `/home/ubuntu/etl/etl_dw/s3-redshift-backup-tool/` |
| Branch | `jasleen_qa` (merges from `jasleen_prod_dw_sync`) |
| Docker Compose dir | `airflow_poc/` (inside repo — legacy setup) |
| DAGs dir | `airflow_poc/dags/` (volume-mounted to `/opt/airflow/dags`) |
| `USE_SSH_TUNNEL` | `true` (QA can't reach PROD/DW MySQL directly) |
| `DB_USER` | `etl-admin-service` |
| Bastion | `jasleentung@35.82.216.244` |
| SSH key (in container) | `/keys/jasleentung.pem` |

### QA Connection Flow

```
Docker container
  → SSH tunnel via bastion (35.82.216.244)
    → PROD MySQL (us-west-2.ro.db.uniuni.com.internal:3306)
    → DW MySQL (db.analysis.uniuni.com.internal:3306)
```

### QA-Specific Config (docker-compose or .env)

```
USE_SSH_TUNNEL=true
SSH_BASTION_HOST=35.82.216.244
SSH_BASTION_USER=jasleentung
SSH_BASTION_KEY_PATH=/keys/jasleentung.pem
DB_USER=etl-admin-service
DB_US_PROD_RO_PASSWORD=B8Dv8LxJGMr0y2a7
DB_DW_WRITE_PASSWORD=Qe97QJaeD246yHDo
DB_PROD_HOST=us-west-2.ro.db.uniuni.com.internal
DB_DW_WRITE_HOST=db.analysis.uniuni.com.internal
```

### QA Deployment (SCP DAG file)

```bash
cd "C:\Users\Jasleen Tung\Downloads\jasleentung_keypair"
scp -o "ProxyCommand=ssh -W %h:%p -i jasleentung.pem jasleentung@35.82.216.244" \
  -i "ubuntu@etl.uniuni.pem" \
  airflow_poc/dags/prod_to_dw_mysql_sync_hourly.py \
  ubuntu@10.101.1.187:/home/ubuntu/etl/etl_dw/s3-redshift-backup-tool/airflow_poc/dags/
```

### QA Testing Commands

```bash
# SSH to QA
ssh -o "ProxyCommand=ssh -W %h:%p -i jasleentung.pem jasleentung@35.82.216.244" \
  ubuntu@10.101.1.187 -i "ubuntu@etl.uniuni.pem"

# Test a single task (no scheduler, no DAG run — isolated)
docker exec airflow_poc-airflow-scheduler-1 \
  airflow tasks test prod_to_dw_mysql_sync_hourly sync_uni_pattern_config 2026-03-13

# Trigger full DAG run
docker exec airflow_poc-airflow-scheduler-1 \
  airflow dags trigger prod_to_dw_mysql_sync_hourly

# Check task states
docker exec airflow_poc-airflow-scheduler-1 \
  airflow tasks states-for-dag-run prod_to_dw_mysql_sync_hourly '<run_id>' -o table

# View task logs (on host)
cat '/home/ubuntu/etl/etl_dw/s3-redshift-backup-tool/airflow_poc/logs/dag_id=prod_to_dw_mysql_sync_hourly/run_id=<run_id>/task_id=<task>/attempt=1.log'

# Force recreate containers (after env var changes)
cd /home/ubuntu/etl/etl_dw/s3-redshift-backup-tool/airflow_poc
docker compose up -d --force-recreate airflow-scheduler airflow-webserver
```

### QA Critical Gotchas

| Issue | Cause | Fix |
|---|---|---|
| `Connection timed out` to MySQL | Missing `USE_SSH_TUNNEL=true` | Add to docker-compose + recreate |
| `Load key: Permission denied` | Container UID != key owner UID | `user: "1000:0"` in docker-compose |
| `Access denied for user` | Wrong user/password in env | Check `DB_USER` in docker-compose (overrides .env) |
| `DROP command denied` | User lacks DDL grants | Use `etl-admin-service` |
| Tasks stuck at `None` | Scheduler still starting | Wait ~2 min after restart |
| DAGs disappear from UI | Log permission error | `chown -R airflow:root /opt/airflow/logs` |
| `docker compose restart` ignores env changes | Known Docker behavior | Must use `--force-recreate` or `down/up` |

---

## PROD Environment

| Item | Value |
|---|---|
| PROD Server | `ubuntu@34.223.171.126` (direct SSH, no bastion) |
| SSH command | `ssh -i jasleentung.pem ubuntu@34.223.171.126` |
| Key directory | `C:\Users\Jasleen Tung\Downloads\jasleentung_keypair\` |
| Airflow directory | `/home/ubuntu/airflow/` (independent of repo) |
| Repo path | `/home/ubuntu/etl/etl_dw/s3-redshift-backup-tool/` |
| `USE_SSH_TUNNEL` | `false` (RDS Proxy in same subnet) |
| `DB_USER` | `etl-admin-service` |
| PROD MySQL endpoint | `mgt-coc.rds-proxy.us-west-2.ro.db.uniuni.com.internal:3306` |
| DW MySQL endpoint | `mgmt-coc.rds-proxy.db.analysis.uniuni.com.internal:3306` |
| Airflow UI | `http://34.223.171.126:8080` (admin / `Pr0d@irfl0w2026`) |

### PROD Connection Flow (no tunnels)

```
Docker container
  → Direct to PROD MySQL (mgt-coc.rds-proxy...internal:3306)
  → Direct to DW MySQL (mgmt-coc.rds-proxy...internal:3306)
```

### PROD-Specific Config (.env)

```
USE_SSH_TUNNEL=false
DB_USER=etl-admin-service
DB_US_PROD_RO_PASSWORD=B8Dv8LxJGMr0y2a7
DB_DW_WRITE_PASSWORD=Qe97QJaeD246yHDo
DB_PROD_HOST=mgt-coc.rds-proxy.us-west-2.ro.db.uniuni.com.internal
DB_DW_WRITE_HOST=mgmt-coc.rds-proxy.db.analysis.uniuni.com.internal
```

### QA vs PROD Differences

| Setting | QA | PROD |
|---|---|---|
| Airflow | 2.8.1 (Python 3.8) | 2.9.0 (Python 3.10) |
| `USE_SSH_TUNNEL` | `true` | `false` |
| `DB_PROD_HOST` | `us-west-2.ro.db.uniuni.com.internal` | `mgt-coc.rds-proxy.us-west-2.ro.db.uniuni.com.internal` |
| `DB_DW_WRITE_HOST` | `db.analysis.uniuni.com.internal` | `mgmt-coc.rds-proxy.db.analysis.uniuni.com.internal` |
| SSH bastion | `35.82.216.244` | Not needed |
| SSH key mount | `/keys/jasleentung.pem` | Not needed |
| Docker image | `apache/airflow:2.8.1` + pip install | `uniuni-airflow:2.9.0` (custom, pre-built) |
| dbt | 1.9.0b2 (beta, no dbt-mysql) | 1.8.0 (stable, dbt-redshift only — dbt-mysql removed) |
| Airflow location | Inside git repo (`airflow_poc/`) | Independent (`/home/ubuntu/airflow/`) |
| Deployment | Manual `scp` | `deploy_prod.sh` (git pull + rsync) |
| Email alerts | `jasleen.tung@uniuni.com` | `etl@uniuni.com` + `jasleen.tung@uniuni.com` |

### PROD Deployment

```bash
# Deploy everything (git pull + rsync DAGs, scripts, dbt)
ssh -i jasleentung.pem ubuntu@34.223.171.126
cd /home/ubuntu/airflow
./scripts/deploy_prod.sh          # deploy all
./scripts/deploy_prod.sh dags     # deploy only DAGs
./scripts/deploy_prod.sh scripts  # deploy only scripts
./scripts/deploy_prod.sh dbt      # deploy only dbt projects

# Recreate containers (only needed if .env or Dockerfile changed)
docker compose up -d --force-recreate airflow-scheduler airflow-webserver
```

---

## Backfill Script

**File**: `airflow_poc/scripts/backfill_large_tables.py`

For initial load of large incremental tables. The hourly DAG handles ongoing increments, but the first time each table is synced, it needs a bulk load from `id = 0` to current max.

### How It Works

1. Reads watermark from `uniods.sync_watermarks` (resumes from last position if interrupted)
2. Fetches rows in chunks of 50,000 from source (`WHERE id > last_id ORDER BY id LIMIT 50000`)
3. Inserts via multi-row `INSERT IGNORE` in sub-batches of 2,000
4. Updates watermark after each chunk commit
5. Retries up to 5 times on lock timeout / deadlock errors
6. Reports progress: rows/s, percentage, ETA

### Supported Tables

| Table | Estimated Time |
|---|---|
| `uni_prealert_info` | ~30 min |
| `uni_tracking_addon_spath` | ~1-2 hours |
| `order_details` | ~5-7 hours |

**Note**: `uni_prealert_info` was added to the backfill script after being moved from full-sync to incremental.

### Usage

```bash
# On QA (inside container)
docker exec -e PYTHONUNBUFFERED=1 airflow_poc-airflow-scheduler-1 \
  python3 -u /opt/airflow/scripts/backfill_large_tables.py uni_tracking_addon_spath

# On PROD (inside container)
docker compose exec -e PYTHONUNBUFFERED=1 airflow-scheduler \
  python3 -u /opt/airflow/scripts/backfill_large_tables.py uni_tracking_addon_spath

# Background with log
nohup docker compose exec -T -e PYTHONUNBUFFERED=1 airflow-scheduler \
  python3 -u /opt/airflow/scripts/backfill_large_tables.py order_details \
  > /tmp/backfill_order_details.log 2>&1 &

tail -f /tmp/backfill_order_details.log
```

### Configuration

- `CHUNK_SIZE = 50000` — rows per source query
- `SUB_BATCH = 2000` — rows per INSERT statement
- `innodb_lock_wait_timeout = 300` — set at start to avoid lock timeouts during large inserts
- Uses same `_load_env()` and `_get_connections()` as the DAG (SSH tunnels on QA, direct on PROD)

---

## QA-to-PROD Cutover

Both QA and PROD write to the **same DW database**. Only one should be active at a time.

1. Verify PROD DAG run succeeds (manual trigger)
2. Compare row counts between QA and PROD runs
3. Pause QA DAG: `docker exec airflow_poc-airflow-scheduler-1 airflow dags pause prod_to_dw_mysql_sync_hourly`
4. Unpause PROD DAG: `docker compose exec airflow-scheduler airflow dags unpause prod_to_dw_mysql_sync_hourly`
5. Monitor first 2-3 hourly PROD runs
6. QA DAG stays paused (keep as fallback)

---

## DW Grants for etl-admin-service

Verified grants on DW MySQL:
- Full access (SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, ALTER, INDEX, etc.) on: `uniods`, `unidw`, `unidatamart`, `settlement`, `uni_driver_salary`, `uni_report_views`, `unidw_backup`, `unidw_scheduler`
- Granted from `%` (any IP)
- CREATE/DROP on `uniods.*` is required for the auto-recreate feature

PROD MySQL grants needed:
- SELECT on `kuaisong.*` and `driver_app.*` from PROD server IP (`10.103.6.19`)

---

## SMTP / Email Alerts

| Item | Value |
|---|---|
| Host | `smtp.office365.com` |
| Port | 587 (STARTTLS) |
| User / From | `jasleen.tung@uniuni.com` |
| Password | `Guruji@2312ninety` |
| Recipients | `jasleen.tung@uniuni.com` |

Emails sent on:
- Full-sync: target table auto-recreated (with mismatch reason)
- Full-sync: row count mismatch after sync (source != target)
- Incremental: schema mismatch or missing target (alert)
- Quality gate: any sync task not clean (lists all issues)
- Errors: connection failures, sync errors
