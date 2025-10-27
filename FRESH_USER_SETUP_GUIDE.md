# Fresh User Setup Guide - S3-Redshift Backup Test Environment

## 🎯 **Complete Setup Guide for New Users**

This guide will help you set up a complete test environment from scratch with the latest performance optimizations and security features.

## 🆕 **Latest Optimizations (September 2025)**

### ⚡ **Performance Enhancements**
- **Sparse Sequence Detection**: Automatically optimizes tables with sparse ID sequences (96% query reduction)
- **Infinite Sync Prevention**: Watermark ceiling protection prevents runaway sync jobs
- **Smart Termination**: Early detection when chunk efficiency drops below 10%

### 🛡️ **Reliability Improvements**  
- **Unified Watermark System**: KISS-based implementation with verified persistence
- **Dual Protection**: Combines sparse detection with watermark ceiling limits
- **Production Tested**: Verified with 19M+ record tables

---

## 📋 **Prerequisites**

### **System Requirements**
- **Python 3.8+** (recommended: Python 3.9-3.12)
- **Git** for repository management
- **SSH keys** configured for database access (if using SSH tunnels)
- **AWS credentials** configured for S3 access

### **Required Access**
- **MySQL Database** - Source database with read access
- **AWS S3 Bucket** - For storing Parquet files
- **Redshift Cluster** - Target warehouse (can be via SSH tunnel)
- **SSH Bastion Host** - If databases are behind firewalls

---

## 🚀 **Step 1: Clone and Basic Setup**

### **1.1 Clone Repository**
```bash
git clone <repository-url>
cd s3-redshift-backup
```

### **1.2 Create Python Virtual Environment**
```bash
# Create virtual environment
python3 -m venv s3_backup_venv

# Activate virtual environment
source s3_backup_venv/bin/activate  # Linux/Mac
# OR
s3_backup_venv\Scripts\activate     # Windows
```

### **1.3 Install Dependencies**
```bash
# Install required packages
pip install --upgrade pip
pip install -r requirements.txt

# If requirements.txt doesn't exist, install core dependencies:
pip install pydantic mysql-connector-python boto3 psycopg2-binary click pandas pyarrow paramiko
```

---

## 🔧 **Step 2: Configuration Setup**

### **2.1 Check System Status**
```bash
# First, check if system is properly configured
python -m src.cli.main status

# View system information
python -m src.cli.main info

# Check available commands
python -m src.cli.main --help
```

**Note**: The system uses `.env` file configuration rather than separate config directories. The CLI will guide you through any missing configuration.

### **2.2 Configure Environment Variables (.env file)**

**CRITICAL**: The `.env` file contains **only credentials** (passwords, SSH keys, AWS keys). All other settings (S3 bucket, performance tuning) are in YAML configuration files.

```bash
# Copy from template
cp .env.template .env

# Edit with your actual credentials
nano .env
```

**Example `.env` structure (credentials only):**
```bash
# ============================================
# Database Credentials (Multiple Databases)
# ============================================
DB_USER=your_db_username

# US Data Warehouse passwords
DB_US_DW_PASSWORD=your_us_dw_password
DB_US_DW_RO_PASSWORD=your_us_dw_ro_password

# US Production passwords
DB_US_PROD_RO_PASSWORD=your_us_prod_password

# Canada Data Warehouse passwords
DB_CA_DW_RO_PASSWORD=your_ca_dw_password

# QA Environment passwords
DB_US_QA_PASSWORD=your_qa_password

# ============================================
# SSH Credentials (Bastion Host)
# ============================================
SSH_BASTION_USER=your_ssh_username
SSH_BASTION_KEY_PATH=/path/to/your/ssh/key.pem

# ============================================
# Redshift Credentials
# ============================================
REDSHIFT_USER=your_redshift_username
REDSHIFT_PASSWORD=your_redshift_password

# Redshift SSH Tunnel (if behind bastion)
REDSHIFT_SSH_BASTION_USER=your_redshift_ssh_user
REDSHIFT_SSH_BASTION_KEY_PATH=/path/to/redshift/ssh/key.pem

# ============================================
# AWS Credentials
# ============================================
AWS_ACCESS_KEY_ID=your_aws_access_key
AWS_SECRET_ACCESS_KEY=your_aws_secret_key
```

**Important Notes:**
- **Only add passwords/credentials you actually need** for your databases
- The system uses `config/connections.yml` for connection details (hosts, ports, database names)
- S3 bucket names and regions are defined in pipeline YAML files (e.g., `config/pipelines/*.yml`)
- Performance settings (batch size, workers) are also in pipeline YAML files

**Security:**
```bash
# Set proper SSH key permissions
chmod 600 /path/to/your/ssh/key.pem

# Verify .env is in .gitignore (already configured)
grep "^\.env" .gitignore
```

### **2.3 Understanding YAML Configuration Files**

The system uses YAML files for non-sensitive configuration. There are two main types:

#### **A. Connections Configuration** (`config/connections.yml`)

Defines database connections, SSH tunnels, and S3 settings. Uses environment variable substitution for security.

```yaml
connections:
  sources:
    # Source MySQL connections
    US_DW_UNIDW_SSH:
      host: us-west-2.ro.db.analysis.uniuni.com.internal
      port: 3306
      database: unidw
      username: "${DB_USER}"              # References .env
      password: "${DB_US_DW_PASSWORD}"    # References .env

      ssh_tunnel:
        enabled: true
        host: 35.83.114.196
        username: "${SSH_BASTION_USER}"   # References .env
        private_key_path: "${SSH_BASTION_KEY_PATH}"  # References .env
        local_port: 0  # Auto-assign

  targets:
    # Redshift target connection
    redshift_default:
      host: your-cluster.redshift.amazonaws.com
      port: 5439
      database: dw
      username: "${REDSHIFT_USER}"        # References .env
      password: "${REDSHIFT_PASSWORD}"    # References .env

  s3:
    # S3 configuration (NOT in .env)
    bucket: your-s3-bucket-name
    region: us-east-1
    access_key_id: "${AWS_ACCESS_KEY_ID}"      # References .env
    secret_access_key: "${AWS_SECRET_ACCESS_KEY}"  # References .env
```

#### **B. Pipeline Configuration** (`config/pipelines/*.yml`)

Defines sync pipelines with table-specific settings, S3 paths, and performance tuning.

```yaml
pipeline:
  name: "your_pipeline_name"
  description: "Your pipeline description"
  source: "US_DW_UNIDW_SSH"     # References connections.yml
  target: "redshift_default"     # References connections.yml
  version: "1.2.0"

  processing:
    strategy: "sequential"       # or "inter-table"
    batch_size: 10000           # Performance tuning (NOT in .env)
    timeout_minutes: 60
    max_parallel_tables: 2

  s3:
    isolation_prefix: "your_pipeline/"  # S3 path prefix
    partition_strategy: "table"
    compression: "snappy"

tables:
  # Table-specific configurations
  unidw.your_table:
    cdc_strategy: "id_and_timestamp"
    cdc_id_column: "id"
    cdc_timestamp_column: "updated_at"
    description: "Your table description"
```

**Key Points:**
- `${VARIABLE}` syntax references variables from `.env` file
- Connection names (e.g., `US_DW_UNIDW_SSH`) are defined in `connections.yml`
- Pipeline files reference connection names, NOT direct credentials
- S3 bucket, batch_size, and other non-sensitive configs are directly in YAML

### **2.4 Verify Configuration**

```bash
# Test system with your .env configuration
python -m src.cli.main status

# This will show:
# - Database connectivity (MySQL via SSH tunnel)
# - S3 bucket access
# - Redshift connectivity (if configured)
# - Any configuration issues
```

**Expected Output:**
```
✅ Database Connection: Connected via SSH tunnel
✅ S3 Bucket Access: Accessible
✅ Redshift Connection: Connected via SSH tunnel
✅ System Status: Ready for backup operations
```

**If you see errors**, check your `.env` file settings and network connectivity.

---

## 🧪 **Step 3: Create Test Pipeline**

### **3.1 Create Test Pipeline Configuration**

Create `config/pipelines/test_pipeline.yml`:
```yaml
pipeline:
  name: "test_pipeline"
  description: "Test pipeline for new users"
  source: "US_DW_UNIDW_SSH"
  target: "redshift_default"
  version: "1.2.0"
  
  processing:
    strategy: "sequential"
    batch_size: 10000
    timeout_minutes: 60
    max_parallel_tables: 2
  
  s3:
    isolation_prefix: "test_pipeline/"
    partition_strategy: "table"
    compression: "snappy"

tables:
  # Start with a small test table
  unidw.your_small_test_table:
    cdc_strategy: "full_sync"
    full_sync_mode: "replace"
    description: "Small test table for validation"
    processing:
      batch_size: 5000
    validation:
      enable_data_quality_checks: true
      max_null_percentage: 10.0
```

---

## 🔍 **Step 4: Test Connections**

### **4.1 Test Individual Connections**
```bash
# Test MySQL connection
python -m src.cli.main connections test US_DW_UNIDW_SSH

# Test Redshift connection  
python -m src.cli.main connections test redshift_default

# List all connections
python -m src.cli.main connections list
```

### **4.2 Test SSH Tunnels (if applicable)**
```bash
# Test SSH connectivity manually
ssh -i /path/to/your/ssh/key your_ssh_user@your-bastion-host.com

# Test database through tunnel
mysql -h your-mysql-host.internal -u your_mysql_user -p
```

---

## 📊 **Step 5: Validate Setup**

### **5.1 Check Pipeline Configuration**
```bash
# List available pipelines
python -m src.cli.main config list-pipelines

# Validate your test pipeline
python -m src.cli.main config show-pipeline test_pipeline
```

### **5.2 Test Small Sync (Dry Run)**
```bash
# Test with dry run first
python -m src.cli.main sync pipeline \
  --pipeline test_pipeline \
  -t unidw.your_small_test_table \
  --dry-run \
  --limit 100
```

---

## 🚀 **Step 6: Execute First Test**

### **6.1 Small Scale Test**
```bash
# Test with limited rows
python -m src.cli.main sync pipeline \
  --pipeline test_pipeline \
  -t unidw.your_small_test_table \
  --limit 1000
```

### **6.2 Verify Results**
```sql
-- Check Redshift table (CRITICAL: Always verify actual data)
SELECT COUNT(*) FROM public.your_small_test_table;
SELECT * FROM public.your_small_test_table LIMIT 5;

-- IMPORTANT: Don't rely on log messages alone for success verification
-- Always check actual row counts in Redshift
```

### **6.3 Check S3 Files**
```bash
# List S3 files (adjust bucket name)
aws s3 ls s3://your-s3-bucket-name/test_pipeline/ --recursive
```

---

## 🐛 **Step 7: Troubleshooting Common Issues**

### **7.1 Connection Issues**

**System Status Shows Connection Errors:**
```bash
# Check system status first
python -m src.cli.main status

# Common issues:
# 1. SSH key permissions (must be 600)
chmod 600 /path/to/your/ssh/key.pem

# 2. Test SSH tunnel manually
ssh -i /path/to/your/ssh/key your_ssh_user@your-bastion-host.com

# 3. Check .env file settings
grep -E "DB_|SSH_|S3_" .env
```

**Dependency Issues:**
```bash
# If you get "ModuleNotFoundError: No module named 'pydantic'"
pip install --upgrade pip
pip install -r requirements.txt

# Or install individual dependencies:
pip install pydantic mysql-connector-python boto3 psycopg2-binary click pandas pyarrow paramiko
```

### **7.2 AWS/S3 Issues**

**S3 Access Denied:**
```bash
# Test AWS credentials
aws s3 ls s3://your-s3-bucket-name/

# Check AWS credentials
aws configure list
```

**Missing AWS Credentials:**
```bash
# Configure AWS credentials
aws configure
# OR set environment variables
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
export AWS_DEFAULT_REGION=us-west-2
```

### **7.3 Permission Issues**

**Table Access Denied:**
```sql
-- Check table permissions (MySQL)
SHOW GRANTS FOR 'your_mysql_user'@'%';

-- Check table exists
USE unidw;
SHOW TABLES LIKE 'your_test_table%';
```

**Redshift Schema Permissions:**
```sql
-- Check Redshift permissions
SELECT HAS_TABLE_PRIVILEGE('your_redshift_user', 'public.test_table', 'INSERT');
```

---

## ✅ **Step 8: Validation Checklist**

### **Before Going Live:**

- [ ] **Connections work** - All database connections successful
- [ ] **SSH tunnels stable** - No connection drops during testing
- [ ] **S3 access confirmed** - Can read/write to designated bucket
- [ ] **Small table sync successful** - End-to-end pipeline works
- [ ] **Data verification passed** - Row counts match between source/target
- [ ] **Proper table truncation** - TRUNCATE works for full_sync replace mode
- [ ] **Error handling tested** - System recovers from connection failures
- [ ] **Watermark system working** - No infinite loops or data duplication
- [ ] **Sparse sequence optimization** - Verified early termination for inefficient tables
- [ ] **Ceiling protection tested** - Confirmed sync stops for continuous data injection

### **Production Readiness:**

- [ ] **Scale testing completed** - Tested with larger tables (10K+ rows)
- [ ] **Performance acceptable** - Sync times meet requirements (96% improvement for sparse tables)
- [ ] **Optimization validation** - Sparse sequence detection working correctly
- [ ] **Monitoring setup** - Logs and alerts configured
- [ ] **Backup procedures** - Recovery plans documented
- [ ] **Team training** - Other users can operate the system

---

## 📚 **Next Steps**

### **For Advanced Usage:**
1. **Review** `USER_MANUAL.md` for comprehensive CLI commands
2. **Check** `WATERMARK_Interpretation.md` for watermark management

### **For Performance Tuning:**
1. **Review** `REDSHIFT_OPTIMIZATION_GUIDE.md` for table optimization strategies
2. **Configure** `redshift_keys.json` for optimal query performance
3. **Understand** dimension vs fact table optimization patterns
4. **Learn** AUTO optimization features for adaptive performance

### **For Production:**
1. **Scale up** with production tables
2. **Configure** monitoring and alerting
3. **Setup** automated scheduling (cron/Airflow)
4. **Establish** operational procedures

---

## 🆘 **Getting Help**

### **Common Commands for Debugging:**
```bash
# Check system status
python -m src.cli.main status

# Get watermarks
python -m src.cli.main watermark get -t your_table -p your_pipeline

# Check S3 files
python -m src.cli.main s3clean list -t your_table -p your_pipeline

# View detailed logs
tail -f logs/backup.log
```

### **Support Resources:**
- **Documentation**: All `.md` files in repository
- **Configuration Examples**: `config_examples/` directory  
- **Test Scripts**: Various `test_*.py` and `check_*.py` files
- **Issue Reporting**: Follow repository issue guidelines

**Remember**: Start small, validate each step, and gradually scale up to production workloads.

---

**Last Updated**: October 21, 2025  