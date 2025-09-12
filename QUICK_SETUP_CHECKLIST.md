# Quick Setup Checklist for New Team Members

## 📋 Complete this checklist to get started with the S3-Redshift backup tool

### Prerequisites
- [ ] Python 3.8+ installed
- [ ] Git access to the repository
- [ ] Access credentials for MySQL, S3, and Redshift

---

## 🚀 Step-by-Step Setup

### 1. Repository Setup
```bash
# Clone repository
git clone https://github.com/your-org/s3-redshift-backup-tool.git
cd s3-redshift-backup-tool

# Install dependencies
pip install -r requirements.txt
```

### 2. Environment Configuration

Create `.env` file in project root:

```bash
# Copy template
cp .env.example .env

# Edit with your credentials
nano .env
```

Required variables:
```bash
# MySQL Database
DB_HOST=your-mysql-host
DB_USER=your-username
DB_PASSWORD=your-password
DB_DATABASE=your-database-name

# SSH Tunnel for MySQL
SSH_BASTION_HOST=bastion.example.com
SSH_BASTION_USER=your-ssh-user
SSH_BASTION_KEY_PATH=/path/to/ssh/key

# S3 Storage
S3_BUCKET_NAME=your-s3-bucket
S3_ACCESS_KEY=your-aws-access-key
S3_SECRET_KEY=your-aws-secret-key
S3_REGION=us-east-1

# Redshift Data Warehouse
REDSHIFT_HOST=your-cluster.redshift.amazonaws.com
REDSHIFT_DATABASE=your-database
REDSHIFT_USER=your-user
REDSHIFT_PASSWORD=your-password
REDSHIFT_SCHEMA=public

# Redshift SSH (if needed)
REDSHIFT_SSH_BASTION_HOST=redshift-bastion.example.com
REDSHIFT_SSH_BASTION_USER=your-user
REDSHIFT_SSH_BASTION_KEY_PATH=/path/to/redshift/key
```

### 3. Test Connections

```bash
# Verify all connections work
python -m src.cli.main status
```

Expected output:
```
✅ System Status: Operational
🔌 Database: Connected (your_database)
📦 S3: Connected (your-bucket) 
🏢 Redshift: Connected (your_schema)
```

### 4. Your First Sync

#### Choose a Small Test Table
```bash
# List available tables (if needed)
python -c "
from src.core.flexible_schema_manager import FlexibleSchemaManager
from src.core.connections import ConnectionManager
from src.config.settings import AppConfig

config = AppConfig.load()
conn_mgr = ConnectionManager(config)
schema_mgr = FlexibleSchemaManager(conn_mgr)
tables = schema_mgr.list_tables('your_schema')
print('Available tables:', tables[:10])
"
```

#### Test Sync with Limits
```bash
# Sync first 1000 rows only
python -m src.cli.main sync -t your_schema.test_table --limit 1000
```

#### Verify Results
```sql
-- Connect to Redshift and verify
SELECT COUNT(*) FROM your_schema.test_table;
SELECT * FROM your_schema.test_table LIMIT 5;
```

### 5. Production Table Setup (Optional)

If you have tables requiring optimization:

#### Create `redshift_keys.json`
```json
{
  "your_schema.your_main_table": {
    "distkey": "customer_id",
    "sortkey": ["created_date", "customer_id"],
    "table_type": "fact"
  },
  "your_schema.lookup_table": {
    "diststyle": "ALL",
    "sortkey": ["lookup_id"],
    "table_type": "dimension"
  }
}
```

---

## ✅ Verification Checklist

Complete all items before using in production:

### Connection Tests
- [ ] MySQL connection works (via SSH tunnel)
- [ ] S3 bucket accessible
- [ ] Redshift connection established
- [ ] All status checks pass

### Basic Functionality
- [ ] Small table sync successful (< 1000 rows)
- [ ] Data appears correctly in Redshift
- [ ] Watermark tracking works
- [ ] Second sync only processes new data

### Error Handling
- [ ] Failed sync can be resumed
- [ ] Error messages are clear
- [ ] Watermark shows correct status

### Production Readiness
- [ ] Tested with your actual tables
- [ ] S3 cleanup commands work
- [ ] Performance optimization configured
- [ ] Team knows schema change procedure

---

## 🆘 If Something Goes Wrong

### First Steps (Always Try These)
1. **Check watermark status**: `python -m src.cli.main watermark get -t your_table`
2. **Review error logs**: Look at console output
3. **Verify connections**: `python -m src.cli.main status`

### Common Solutions
```bash
# Reset if watermark is stuck
python -m src.cli.main watermark reset -t table_name

# Clear schema cache if structure changed
# (Need to do this programmatically currently)

# Clean up S3 if too many files
python -m src.cli.main s3clean clean -t table_name --dry-run
```

### When to Ask for Help
- SSH connection issues → Ask infrastructure team
- Schema errors → Ask database team  
- Performance issues → Ask data engineering team
- General usage → Check documentation first

---

## 📞 Team Contacts

- **Tool Owner**: [Your Name] - General questions
- **Infrastructure**: [Team] - SSH/connection issues
- **Database**: [Team] - Schema changes
- **Data Engineering**: [Team] - Performance optimization

---

## 📚 Next Reading

After completing this checklist:
1. **Read**: `TEAM_INTRODUCTION_GUIDE.md` for detailed concepts
2. **Reference**: `USER_MANUAL.md` for complete command reference
3. **Advanced**: `REDSHIFT_OPTIMIZATION_GUIDE.md` for performance tuning

---

## 🎉 Welcome to Efficient Data Syncing!

Once you complete this checklist, you'll have:
- Reliable automated data sync capabilities
- Understanding of incremental processing
- Tools to handle large-scale data operations
- Confidence to sync production tables

**Remember**: Start small, test thoroughly, then scale up!