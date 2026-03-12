# PROD Airflow Setup Checklist

Checklist for deploying Airflow on the new PROD ETL server (`34.223.171.126` / `10.103.6.19`).
Based on lessons learned from QA deployment.

---

## 1. Custom Docker Image (High Priority)

**Problem:** `_PIP_ADDITIONAL_REQUIREMENTS` installs packages on every container restart, adding 3-5 min to startup and using extra memory.

**Fix:** Build a custom image with only the packages we need:

```dockerfile
# Dockerfile
FROM apache/airflow:2.8.1
RUN pip install --no-cache-dir \
    mysql-connector-python \
    boto3
```

Build and use:
```bash
docker build -t uniuni-airflow:2.8.1 .
```

Then in docker-compose.yml, replace:
```yaml
image: apache/airflow:2.8.1
```
with:
```yaml
image: uniuni-airflow:2.8.1
```

And remove `_PIP_ADDITIONAL_REQUIREMENTS` entirely.

**Packages NOT needed for MySQL sync DAG:**
- `dbt-redshift`, `pyarrow`, `pyyaml`, `python-dotenv` — dbt pipeline only
- `apache-airflow-providers-mysql`, `apache-airflow-providers-postgres` — not used (we use mysql-connector-python directly)
- `pydantic-settings`, `pydantic`, `sshtunnel`, `paramiko`, `structlog`, `click`, `tqdm`, `pandas`, `cerberus`, `jsonschema`, `typing-extensions`, `pymysql` — not used by this DAG

If other DAGs need these later, add them to the Dockerfile and rebuild.

---

## 2. Resource Limits (High Priority)

**Problem:** No memory limits means a runaway process can consume all 15GB and crash the server (happened on QA).

**Fix:** Add resource limits to each service in docker-compose.yml:

```yaml
airflow-scheduler:
  <<: *airflow-common
  command: scheduler
  restart: always
  deploy:
    resources:
      limits:
        memory: 4G
        cpus: '2'

airflow-webserver:
  <<: *airflow-common
  command: webserver
  restart: always
  deploy:
    resources:
      limits:
        memory: 2G
        cpus: '1'

postgres:
  image: postgres:13
  deploy:
    resources:
      limits:
        memory: 1G
        cpus: '0.5'
```

Total: ~7GB reserved, leaving 8GB free for OS and backfill scripts.

---

## 3. Passwords in .env File (Medium Priority)

**Problem:** All passwords are hardcoded in docker-compose.yml. Anyone with server access can read them.

**Fix:** Move secrets to a `.env` file in the same directory as docker-compose.yml:

```bash
# airflow_poc/.env (on PROD server)
AIRFLOW_UID=1000
DB_USER=jasleentung
DB_US_PROD_RO_PASSWORD=86a05b5772c55c83c9db74a01c01441f
DB_DW_WRITE_PASSWORD=b33c6fe2b4857c948f818f7260c9c304
ALERT_SMTP_PASSWORD=Guruji@2312ninety
```

Then in docker-compose.yml, reference them:
```yaml
- DB_US_PROD_RO_PASSWORD=${DB_US_PROD_RO_PASSWORD}
- DB_DW_WRITE_PASSWORD=${DB_DW_WRITE_PASSWORD}
```

Lock down permissions:
```bash
chmod 600 .env
```

---

## 4. PROD-Specific Config Changes

DevOps provided RDS Proxy endpoints in the same `10.103.x.x` subnet — **no SSH tunnels needed**.

| Setting | QA | PROD |
|---|---|---|
| `USE_SSH_TUNNEL` | `true` | **`false`** (RDS Proxy in same subnet) |
| `DB_PROD_HOST` | `us-west-2.ro.db.uniuni.com.internal` | **`mgt-coc.rds-proxy.us-west-2.ro.db.uniuni.com.internal`** |
| `DB_DW_WRITE_HOST` | `db.analysis.uniuni.com.internal` | **`mgmt-coc.rds-proxy.db.analysis.uniuni.com.internal`** |
| Port mapping | `8081:8080` | **`8080:8080`** (clean server) |
| SSH bastion | needed | **not needed** |
| SSH key mount | needed | **not needed** |
| Airflow admin password | `admin123` | **change to something stronger** |
| Server SSH | via bastion | **direct** (`ssh -i jasleentung.pem ubuntu@34.223.171.126`) |

---

## 5. Remove Obsolete docker-compose Version (Low Priority)

**Problem:** `version: '3.8'` generates a warning on every command.

**Fix:** Delete the `version: '3.8'` line from docker-compose.yml.

---

## 6. Postgres Backup (Low Priority)

**Problem:** If the postgres volume is lost (server reboot destroyed it on QA), all DAG run history is gone. Requires `airflow db init` + user recreation.

**Fix:** Add a daily cron job on the host:

```bash
# /etc/cron.d/airflow-pg-backup
0 2 * * * root docker exec airflow_poc-postgres-1 pg_dump -U airflow airflow | gzip > /home/ubuntu/backups/airflow_pg_$(date +\%Y\%m\%d).sql.gz
```

Create backup directory:
```bash
mkdir -p /home/ubuntu/backups
```

---

## 7. Single Airflow Instance (Important)

Only run **one** Airflow instance per server. All team members share the same instance by dropping DAG files into the `dags/` folder.

- Do NOT create separate docker-compose setups per person
- Do NOT use different ports for different people
- One Airflow = one scheduler + one webserver + one postgres = all DAGs

---

## 8. Deployment Steps (in order)

```bash
# 1. SSH to PROD server
cd "C:\Users\Jasleen Tung\Downloads\jasleentung_keypair"
ssh -i jasleentung.pem ubuntu@34.223.171.126

# 2. Create directory structure
mkdir -p /home/ubuntu/etl/etl_dw/airflow/{dags,scripts,logs}
cd /home/ubuntu/etl/etl_dw/airflow

# 3. Copy files from local
#    - docker-compose.yml (with fixes from this checklist)
#    - Dockerfile (custom image)
#    - .env (passwords)
#    - dags/prod_to_dw_mysql_sync_hourly.py
#    - scripts/backfill_large_tables.py

# 4. Build custom image
docker build -t uniuni-airflow:2.8.1 .

# 5. Set permissions
chmod 600 .env

# 6. Initialize
docker compose up -d postgres
docker compose run --rm airflow-init
docker compose up -d

# 7. Verify
docker compose ps
curl http://localhost:8080/health

# 8. Run backfill for 3 large tables (if needed)
docker compose exec -e PYTHONUNBUFFERED=1 airflow-scheduler \
    python3 -u /opt/airflow/scripts/backfill_large_tables.py uni_tracking_addon_spath

# 9. Set up systemd service (auto-start on boot)
#    See Section 9 below

# 10. Unpause DAG
docker compose exec airflow-scheduler airflow dags unpause prod_to_dw_mysql_sync_hourly
```

---

## 9. Systemd Service (auto-start on boot)

**Why:** If the server reboots, Airflow should start automatically without anyone logging in manually.

Docker's `restart: always` handles container restarts, but systemd ensures `docker compose up` is called on boot even if Docker Compose state is lost.

**Create the service file:**
```bash
sudo tee /etc/systemd/system/airflow.service > /dev/null <<'EOF'
[Unit]
Description=Airflow Docker Compose
Requires=docker.service
After=docker.service

[Service]
Type=oneshot
RemainAfterExit=yes
WorkingDirectory=/home/ubuntu/etl/etl_dw/airflow
ExecStart=/usr/bin/docker compose up -d
ExecStop=/usr/bin/docker compose down
TimeoutStartSec=120

[Install]
WantedBy=multi-user.target
EOF
```

**Enable and start:**
```bash
sudo systemctl daemon-reload
sudo systemctl enable airflow    # auto-start on boot
sudo systemctl start airflow     # start now
```

**Useful commands:**
```bash
sudo systemctl status airflow    # check status
sudo systemctl restart airflow   # restart all containers
sudo journalctl -u airflow -f    # view logs
```

---

## QA Lessons Learned

1. **Server reboots destroy postgres volume** — Airflow metadata lost, need `airflow db init`
2. **Docker DNS breaks after reboot** — fix with `sudo systemctl restart docker`
3. **Colleague building Docker images caused OOM** — resource limits prevent this
4. **`_PIP_ADDITIONAL_REQUIREMENTS` is slow** — custom image is much better
5. **Log permissions** — `AIRFLOW_UID=1000` in `.env` must match host UID
6. **INSERT IGNORE + multi-row batches** — 10x faster than executemany for INSERT IGNORE
7. **Lock wait timeout** — set `innodb_lock_wait_timeout = 300` for large inserts
