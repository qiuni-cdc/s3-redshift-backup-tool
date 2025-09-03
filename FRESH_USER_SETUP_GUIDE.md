# Fresh User Setup Guide - S3-Redshift Backup Test Environment

## 🎯 **Complete Setup Guide for New Users**

This guide will help you set up a complete test environment from scratch.

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

### **2.1 Initialize Configuration**
```bash
# Create default configuration structure
python -m src.cli.main config setup
```

This creates:
```
config/
├── connections.yml       # Database connections
├── pipelines/
│   └── default.yml      # Default pipeline
└── environments/
    ├── development.yml   # Dev settings
    └── production.yml    # Prod settings
```

### **2.2 Configure Environment Variables**

Create `.env` file in project root:
```bash
# Copy from template (if exists)
cp .env.template .env

# OR create manually
cat > .env << 'EOF'
# Environment
ENVIRONMENT=development

# AWS Configuration
AWS_REGION=us-west-2
S3_BUCKET_NAME=your-s3-bucket-name

# Redshift Configuration (via SSH tunnel)
REDSHIFT_HOST=localhost
REDSHIFT_PORT=5439
REDSHIFT_DATABASE=your_redshift_db
REDSHIFT_USERNAME=your_redshift_user
REDSHIFT_PASSWORD=your_redshift_password
REDSHIFT_SCHEMA=public

# SSH Tunnel for Redshift (if needed)
REDSHIFT_SSH_HOST=your-bastion-host.com
REDSHIFT_SSH_PORT=22
REDSHIFT_SSH_USERNAME=your_ssh_user
REDSHIFT_SSH_KEY_PATH=/path/to/your/ssh/key

# Logging
LOG_LEVEL=INFO
LOG_FILE=logs/backup.log
EOF
```

### **2.3 Configure Database Connections**

Edit `config/connections.yml`:
```yaml
connections:
  # Source MySQL Database
  US_DW_UNIDW_SSH:
    type: "mysql"
    host: "your-mysql-host.com"
    port: 3306
    database: "unidw"
    username: "your_mysql_user"
    password: "your_mysql_password"
    
    # SSH tunnel configuration (if needed)
    ssh_tunnel:
      enabled: true
      host: "your-bastion-host.com"
      port: 22
      username: "your_ssh_user"
      key_path: "/path/to/your/ssh/key"
      remote_bind_address: "your-mysql-host.internal"
      remote_bind_port: 3306
    
    connection_params:
      charset: "utf8mb4"
      sql_mode: "TRADITIONAL"
      autocommit: false

  # Target Redshift
  redshift_default:
    type: "redshift"
    host: "your-redshift-cluster.redshift.amazonaws.com"
    port: 5439
    database: "your_redshift_db"
    schema: "public"
    username: "your_redshift_user"
    password: "your_redshift_password"
    
    # SSH tunnel for Redshift (if needed)
    ssh_tunnel:
      enabled: true
      host: "your-redshift-bastion.com"
      port: 22
      username: "your_ssh_user"
      key_path: "/path/to/your/ssh/key"
```

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
-- Check Redshift table
SELECT COUNT(*) FROM public.your_small_test_table;
SELECT * FROM public.your_small_test_table LIMIT 5;
```

### **6.3 Check S3 Files**
```bash
# List S3 files (adjust bucket name)
aws s3 ls s3://your-s3-bucket-name/test_pipeline/ --recursive
```

---

## 🐛 **Step 7: Troubleshooting Common Issues**

### **7.1 Connection Issues**

**MySQL Connection Failed:**
```bash
# Check SSH tunnel
ssh -v -i /path/to/your/ssh/key your_ssh_user@your-bastion-host.com

# Test MySQL connection manually
mysql -h your-mysql-host -u your_mysql_user -p -e "SELECT 1"
```

**Redshift Connection Failed:**
```bash
# Test Redshift connection manually
psql -h your-redshift-cluster.redshift.amazonaws.com \
     -p 5439 \
     -U your_redshift_user \
     -d your_redshift_db \
     -c "SELECT 1"
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

### **Production Readiness:**

- [ ] **Scale testing completed** - Tested with larger tables (10K+ rows)
- [ ] **Performance acceptable** - Sync times meet requirements  
- [ ] **Monitoring setup** - Logs and alerts configured
- [ ] **Backup procedures** - Recovery plans documented
- [ ] **Team training** - Other users can operate the system

---

## 📚 **Next Steps**

### **For Advanced Usage:**
1. **Review** `USER_MANUAL.md` for comprehensive CLI commands
2. **Check** `WATERMARK_CLI_GUIDE.md` for watermark management
3. **Read** `WATERMARK_BUG_PREVENTION_CHECKLIST.md` for best practices

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

# List watermarks
python -m src.cli.main watermark list

# Check S3 files
python -m src.cli.main s3clean list -t your_table

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

**Last Updated**: September 3, 2025  
**Compatible with**: v1.2.0 multi-schema architecture