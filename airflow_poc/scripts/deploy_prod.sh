#!/bin/bash
# Deploy DAGs, scripts, and dbt projects from git repo to Airflow on PROD.
# Run this ON the PROD server after git pull.
#
# Usage:
#   ./deploy_prod.sh              # git pull + deploy everything
#   ./deploy_prod.sh dags         # deploy only DAGs (no git pull)
#   ./deploy_prod.sh scripts      # deploy only scripts
#   ./deploy_prod.sh dbt          # deploy only dbt projects
#
# Workflow:
#   ssh to PROD → cd /home/ubuntu/airflow → ./scripts/deploy_prod.sh
#
# What it does:
#   1. git pull main in the project repo
#   2. rsync DAGs, scripts, dbt from repo to /home/ubuntu/airflow/
#   3. --delete ensures removed files get cleaned up on PROD

set -euo pipefail

REPO_DIR="/home/ubuntu/etl/etl_dw/s3-redshift-backup-tool"
AIRFLOW_DIR="/home/ubuntu/airflow"
BRANCH="main"

deploy_dags() {
    echo "=== Deploying DAGs ==="
    rsync -av --delete \
        --include="*.py" --exclude="__pycache__" \
        "$REPO_DIR/airflow_poc/dags/" "$AIRFLOW_DIR/dags/"
    echo ""
}

deploy_scripts() {
    echo "=== Deploying Scripts ==="
    rsync -av --delete \
        --include="*.py" --include="*.sh" --exclude="__pycache__" \
        "$REPO_DIR/airflow_poc/scripts/" "$AIRFLOW_DIR/scripts/"
    echo ""
}

deploy_dbt() {
    if [ -d "$REPO_DIR/airflow_poc/dbt_projects" ]; then
        echo "=== Deploying dbt Projects ==="
        rsync -av --delete \
            --exclude="__pycache__" --exclude="target" \
            --exclude="dbt_packages" --exclude="logs" --exclude=".user.yml" \
            "$REPO_DIR/airflow_poc/dbt_projects/" "$AIRFLOW_DIR/dbt/"
        echo ""
    else
        echo "No dbt_projects/ directory found — skipping"
    fi
}

TARGET="${1:-all}"

# git pull only on full deploy
if [ "$TARGET" = "all" ]; then
    echo "=== Pulling latest $BRANCH ==="
    cd "$REPO_DIR"
    git fetch origin "$BRANCH"
    git checkout "$BRANCH"
    git pull origin "$BRANCH"
    echo ""
fi

case "$TARGET" in
    dags)    deploy_dags ;;
    scripts) deploy_scripts ;;
    dbt)     deploy_dbt ;;
    all)
        deploy_dags
        deploy_scripts
        deploy_dbt
        ;;
    *)
        echo "Usage: $0 [dags|scripts|dbt|all]"
        exit 1
        ;;
esac

# Verify
echo "=== Deployed Files ==="
echo "DAGs:"
ls -1 "$AIRFLOW_DIR/dags/"*.py 2>/dev/null || echo "  (none)"
echo ""
echo "Scripts:"
ls -1 "$AIRFLOW_DIR/scripts/"*.{py,sh} 2>/dev/null || echo "  (none)"
echo ""
echo "dbt:"
ls -1 "$AIRFLOW_DIR/dbt/" 2>/dev/null || echo "  (none)"
echo ""
echo "Deploy complete. Airflow picks up changes within ~30 seconds."
