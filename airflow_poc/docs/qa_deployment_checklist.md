# QA Deployment Checklist — PROD→DW MySQL Sync DAG

**Last updated**: 2026-03-13

Quick reference for deploying DAG changes to the QA server.

---

## Server Details

| Item | Value |
|---|---|
| QA Server | `ubuntu@10.101.1.187` via bastion `35.82.216.244` |
| SSH command | `ssh -o "ProxyCommand=ssh -W %h:%p -i jasleentung.pem jasleentung@35.82.216.244" ubuntu@10.101.1.187 -i "ubuntu@etl.uniuni.pem"` |
| Key directory | `C:\Users\Jasleen Tung\Downloads\jasleentung_keypair\` |
| Repo path | `/home/ubuntu/etl/etl_dw/s3-redshift-backup-tool/` |
| Branch | `jasleen_qa` (merges from `jasleen_prod_dw_sync`) |
| Docker Compose dir | `airflow_poc/` |
| DAGs dir | `airflow_poc/dags/` (volume-mounted to `/opt/airflow/dags`) |
| .env (SYNC_TOOL_PATH) | Root `.env` at `/home/ubuntu/etl/etl_dw/s3-redshift-backup-tool/.env` |

---

## Critical Config (Learned the Hard Way)

### 1. `user:` directive in docker-compose.yml
```yaml
x-airflow-common: &airflow-common
  image: apache/airflow:2.8.1
  user: "${AIRFLOW_UID:-1000}:0"   # MUST match host ubuntu UID
```
Without this, container runs as UID 50000 (default airflow) and **cannot read the SSH key** at `/keys/jasleentung.pem` (owned by UID 1000).

### 2. `AIRFLOW_UID=1000` in `airflow_poc/.env`
Already set. Must match the `user:` directive.

### 3. SSH key permissions
```bash
chmod 600 ~/etl/etl_dw/ssh/jasleentung.pem
```
SSH refuses keys with permissions > 600. The container user (UID 1000) must own the file.

### 4. `USE_SSH_TUNNEL=true`
QA server **cannot reach PROD/DW MySQL directly** — must go through SSH bastion. Set in both:
- Root `.env`: `USE_SSH_TUNNEL=true`
- `docker-compose.yml` environment section (takes precedence)

### 5. DB credentials for PROD→DW sync
The DAG reads from `SYNC_TOOL_PATH/.env` (root `.env`), with `os.environ` (docker-compose) taking precedence.

**Current credentials** (etl-admin-service — has CREATE/DROP on uniods):
| Var | Value | Where to set |
|---|---|---|
| `DB_USER` | `etl-admin-service` | docker-compose.yml AND root .env |
| `DB_US_PROD_RO_PASSWORD` | `B8Dv8LxJGMr0y2a7` | root .env (docker-compose overrides) |
| `DB_DW_WRITE_PASSWORD` | `Qe97QJaeD246yHDo` | root .env |
| `DB_PROD_HOST` | `us-west-2.ro.db.uniuni.com.internal` | root .env |
| `DB_DW_WRITE_HOST` | `db.analysis.uniuni.com.internal` | root .env |

**Important**: `DB_USER` in docker-compose affects ALL DAGs. If other DAGs need `jasleentung`, add separate user vars.

### 6. `docker compose restart` does NOT pick up new env vars
Must use:
```bash
docker compose up -d --force-recreate airflow-scheduler airflow-webserver
```
Or full down/up:
```bash
docker compose down && docker compose up -d
```

### 7. Log permissions on new date directories
After restart, if scheduler logs show `PermissionError`:
```bash
docker exec -u root airflow_poc-airflow-scheduler-1 chown -R airflow:root /opt/airflow/logs
docker exec -u root airflow_poc-airflow-scheduler-1 chmod -R 775 /opt/airflow/logs
docker restart airflow_poc-airflow-scheduler-1
```

---

## Deployment Steps

### 1. Push code to GitHub
```bash
# Local machine
cd s3-redshift-backup-tool
git add airflow_poc/dags/prod_to_dw_mysql_sync_hourly.py
git commit -m "description"
git push origin jasleen_prod_dw_sync
```

### 2. Merge on QA server
```bash
# SSH to QA
cd /home/ubuntu/etl/etl_dw/s3-redshift-backup-tool
git fetch origin
git stash  # if local changes exist
git merge origin/jasleen_prod_dw_sync --no-edit
```

### 3. Or SCP the DAG file directly
```bash
# From local machine (key directory)
scp -o "ProxyCommand=ssh -W %h:%p -i jasleentung.pem jasleentung@35.82.216.244" \
  -i "ubuntu@etl.uniuni.pem" \
  airflow_poc/dags/prod_to_dw_mysql_sync_hourly.py \
  ubuntu@10.101.1.187:/home/ubuntu/etl/etl_dw/s3-redshift-backup-tool/airflow_poc/dags/
```

### 4. Recreate containers (if env vars changed)
```bash
cd /home/ubuntu/etl/etl_dw/s3-redshift-backup-tool/airflow_poc
docker compose up -d --force-recreate airflow-scheduler airflow-webserver
```

### 5. Trigger DAG run
```bash
docker exec airflow_poc-airflow-scheduler-1 airflow dags trigger prod_to_dw_mysql_sync_hourly
```

### 6. Monitor
```bash
# Task states
docker exec airflow_poc-airflow-scheduler-1 airflow tasks states-for-dag-run \
  prod_to_dw_mysql_sync_hourly '<run_id>' -o table

# Task logs (on host filesystem)
cat '/home/ubuntu/etl/etl_dw/s3-redshift-backup-tool/airflow_poc/logs/dag_id=prod_to_dw_mysql_sync_hourly/run_id=<run_id>/task_id=<task>/attempt=1.log'
```

---

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `Connection timed out` to MySQL | Missing `USE_SSH_TUNNEL=true` | Add to docker-compose + recreate |
| `Load key: Permission denied` | Container UID ≠ key owner UID | Add `user: "1000:0"` to docker-compose |
| `Access denied for user` | Wrong user/password in env | Check `DB_USER` in docker-compose (overrides .env) |
| `DROP command denied` | User lacks DDL grants | Use `etl-admin-service` (has CREATE/DROP on uniods) |
| Tasks stuck at `None` | Scheduler still installing pip packages | Wait ~2 min after restart |
| DAGs disappear from UI | Log permission error | Fix permissions (see step 7 above) |
| Run queued forever | `max_active_runs=1`, previous run still active | Wait or mark failed run as success |
