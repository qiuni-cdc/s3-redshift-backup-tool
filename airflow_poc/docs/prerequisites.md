# Order Tracking Pipeline — Server Prerequisites

Steps that must be completed **once on a fresh server** before the pipeline can run.
These are not handled by `docker compose up` — they are host-level setup steps.

---

## 1. Create and chown the logs directory

**Command:**
```bash
mkdir -p /home/ubuntu/etl/etl_dw/s3-redshift-backup-tool/airflow_poc/logs
sudo chown -R 50000:50000 /home/ubuntu/etl/etl_dw/s3-redshift-backup-tool/airflow_poc/logs
```

**Why:**
The `docker-compose.yml` mounts `./logs` on the host into the container at `/opt/airflow/logs`:
```yaml
- ./logs:/opt/airflow/logs
```
The Airflow container runs as UID **50000** (the official Apache Airflow Docker image default).
When `mkdir` creates the directory on the host it is owned by the ubuntu user (UID 1000),
not by UID 50000. On startup the scheduler tries to create task log subdirectories inside
`/opt/airflow/logs` and immediately hits `PermissionError: [Errno 13] Permission denied`.
This crashes the scheduler process. The webserver stays up (it only reads logs) so the
Airflow UI appears healthy — but no tasks ever run because there is no scheduler.

**Symptom if missed:**
- Tasks stuck in `queued` indefinitely
- `docker logs airflow_poc-airflow-scheduler-1` ends with:
  ```
  PermissionError: [Errno 13] Permission denied: '/opt/airflow/logs/dag_id=...'
  ```
- `docker ps` shows scheduler container `Exited (1)`

**Note:** This step is only needed once per server. After the directory exists with correct
ownership, container restarts and `docker compose up --force-recreate` work without issues.

---

## 2. Copy the SSH key to the expected path

**Command:**
```bash
mkdir -p /home/ubuntu/etl/etl_dw/ssh
cp /path/to/jasleentung.pem /home/ubuntu/etl/etl_dw/ssh/jasleentung.pem
chmod 400 /home/ubuntu/etl/etl_dw/ssh/jasleentung.pem
```

**Why:**
The compose file mounts the key into the container at `/keys/jasleentung.pem`:
```yaml
- /home/ubuntu/etl/etl_dw/ssh/jasleentung.pem:/keys/jasleentung.pem:ro
```
This key is used by the extraction tasks to open an SSH tunnel to the MySQL bastion host
(`35.83.114.196`) before connecting to the production MySQL read replica. Without it,
all three extraction tasks (`extract_ecs`, `extract_uti`, `extract_uts`) fail immediately
with an SSH authentication error and the entire pipeline fails.

**Symptom if missed:**
- `extract_*` tasks fail within seconds
- Error in task log: `paramiko.ssh_exception.AuthenticationException`

---

## 3. Set ALERT_SMTP_PASSWORD in the container environment

The SMTP vars are in `docker-compose.yml` **except the password** which must be set
as an environment variable on the host so it is not committed to git.

The current approach: the password is baked directly into `docker-compose.yml`
(acceptable for internal QA server, not for production). If it is ever rotated,
update the value in `docker-compose.yml` under the `ALERT_SMTP_PASSWORD` line.

**Current credentials reference:** `docs/claude_connections.md` — SMTP section.

---

## 4. Redshift hist tables must exist before first run

**Why:**
The dbt mart models run post_hooks that INSERT into hist tables on every cycle
(e.g., `hist_uni_tracking_info_2025_h2`). If a hist table does not exist, the
post_hook fails and the entire mart model run fails on the first cycle that
tries to archive aged-out rows (~6 months after pipeline start).

For QA this is not immediate — the pipeline started Feb 2026 so archiving will
not trigger until ~Aug 2026. But the tables must exist before then.

**Tables to create (DDL mirrors the mart table structure):**
```sql
CREATE TABLE settlement_ods.hist_uni_tracking_info_2025_h2   (LIKE settlement_ods.mart_uni_tracking_info);
CREATE TABLE settlement_ods.hist_uni_tracking_info_2026_h1   (LIKE settlement_ods.mart_uni_tracking_info);
CREATE TABLE settlement_ods.hist_ecs_order_info_2025_h2      (LIKE settlement_ods.mart_ecs_order_info);
CREATE TABLE settlement_ods.hist_ecs_order_info_2026_h1      (LIKE settlement_ods.mart_ecs_order_info);
CREATE TABLE settlement_ods.hist_uni_tracking_spath_2025_h2  (LIKE settlement_ods.mart_uni_tracking_spath);
CREATE TABLE settlement_ods.hist_uni_tracking_spath_2026_h1  (LIKE settlement_ods.mart_uni_tracking_spath);
```

**See also:** `airflow_poc/docs/pipeline_review_issues.md` — Issue 8 (2026_h1 period deadline).

---

## 5. order_tracking_exceptions table must exist

**Why:**
All three mart models write safety-check exceptions to `settlement_ods.order_tracking_exceptions`.
The new `check_extraction_lag` task also writes `EXTRACTION_LAG_*` entries to this table.
If it does not exist, post_hooks fail and the lag check task fails.

**DDL:**
```sql
CREATE TABLE settlement_ods.order_tracking_exceptions (
    order_id      VARCHAR(64),
    exception_type VARCHAR(64)   NOT NULL,
    detected_at   TIMESTAMP     NOT NULL,
    resolved_at   TIMESTAMP,
    notes         VARCHAR(1024)
)
DISTSTYLE ALL
SORTKEY (exception_type, detected_at);
```

---

## 6. git pull on the QA server before docker compose up

**Why:**
The DAG files, dbt models, and `docker-compose.yml` are all version-controlled.
The server must be on the correct branch (`jasleen_qa`) and up to date before
starting the containers, otherwise it runs stale code.

```bash
cd /home/ubuntu/etl/etl_dw/s3-redshift-backup-tool
git checkout jasleen_qa
git pull
```

**If git pull conflicts with local changes** (e.g., files were SCP'd directly):
```bash
git stash          # save local changes
git pull           # pull remote (which likely already has the correct version)
git stash drop     # discard stash — remote version is correct
```

---

## Summary checklist

| # | Step | One-time | Recurring |
|---|---|---|---|
| 1 | `chown 50000:50000 airflow_poc/logs` | Fresh server only | — |
| 2 | Copy SSH key to `/home/ubuntu/etl/etl_dw/ssh/` | Fresh server only | On key rotation |
| 3 | SMTP password in docker-compose.yml | On password rotation | — |
| 4 | Create Redshift hist tables | Before archiving triggers (~Aug 2026) | Every 6 months (period rotation) |
| 5 | Create order_tracking_exceptions table | Before first run | — |
| 6 | `git pull` before `docker compose up` | Every deploy | Every deploy |
