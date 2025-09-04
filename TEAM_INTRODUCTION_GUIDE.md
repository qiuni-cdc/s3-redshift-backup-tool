# S3-Redshift Backup Tool - Team Introduction Guide

## 🚀 Quick Start for New Team Members

Welcome to our production-ready MySQL → S3 → Redshift incremental backup system! This guide will help you understand and start using the tool quickly.

## 📋 Table of Contents
- [Why This Tool?](#-why-this-tool)
- [Key Features](#-key-features)
- [Basic Concepts](#-basic-concepts)
- [System Architecture](#-system-architecture)
- [Common Usage Scenarios](#-common-usage-scenarios)
- [Quick Command Reference](#-quick-command-reference)
- [Best Practices](#-best-practices)
- [Troubleshooting](#-troubleshooting)

## 🎯 Why This Tool?

### The Problem It Solves
- **Manual Data Syncing**: No more manual exports/imports
- **Data Loss Prevention**: Incremental backups with watermark tracking
- **Performance**: Optimized for large tables (tested with 385M+ rows)
- **Reliability**: Automatic retry, error recovery, and progress tracking

### Perfect For
- Regular MySQL → Redshift data synchronization
- Large table migrations (millions of rows)
- Incremental data updates without full table reloads
- Multi-environment data pipelines

## 🌟 Key Features

### 1. **Incremental Backup with Watermarks**
- Only syncs new/changed data since last run
- Prevents data duplication and loss
- Automatic progress tracking

### 2. **Production-Grade Reliability**
- SSH tunnel support for secure connections
- Automatic retry on failures
- Comprehensive error logging
- Memory-efficient chunking for huge tables

### 3. **Flexible Schema Management**
- Automatic schema discovery from MySQL
- Column name sanitization (e.g., `190_time` → `col_190_time`)
- VARCHAR safety buffer (auto-doubles lengths)
- Handles schema differences gracefully

### 4. **Redshift Optimization**
- Configurable DISTKEY/SORTKEY for performance
- Direct Parquet loading (no CSV conversion)
- Support for dimension tables with `DISTSTYLE ALL`
- Automatic table creation

### 5. **Advanced Features**
- CDC (Change Data Capture) strategies
- S3 storage management with cleanup tools
- Multi-database support (v1.2.0)
- Manual watermark control for custom scenarios

## 📚 Basic Concepts

### Watermarks
Think of watermarks as "bookmarks" that track where the last sync ended:
- **MySQL Watermark**: Last data timestamp processed
- **S3 Files**: Parquet files stored between syncs
- **Redshift Status**: Success/failed status of loading

### Backup Strategies
1. **Sequential** (Default): Tables processed one by one
2. **Inter-table Parallel**: Multiple tables simultaneously

### Data Flow
```
MySQL → Extract Data → S3 (Parquet) → COPY → Redshift
   ↑                         ↓
   └──── Watermark ←─────────┘
```

## 🏗️ System Architecture

### High-Level Architecture
```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│                 │     │                 │     │                 │
│  MySQL Source   │────▶│   S3 Storage    │────▶│    Redshift     │
│  (Via SSH)      │     │  (Parquet)      │     │  (Via SSH)      │
│                 │     │                 │     │                 │
└─────────────────┘     └─────────────────┘     └─────────────────┘
         │                       │                        │
         └───────────────────────┴────────────────────────┘
                          Watermark System
                         (Tracks Progress)
```

### Key Components

1. **FlexibleSchemaManager**
   - Discovers table schemas dynamically
   - Maps MySQL types → Redshift types
   - Handles column name issues

2. **S3WatermarkManager**
   - Stores sync progress in S3
   - Prevents data loss/duplication
   - Enables incremental processing

3. **GeminiRedshiftLoader**
   - Direct Parquet COPY to Redshift
   - Automatic table creation
   - Row count verification

4. **Connection Manager**
   - SSH tunnel management
   - Connection pooling
   - Retry logic

## 💻 Common Usage Scenarios

### 1. First Time Setup

#### Configure Environment (.env file)
```bash
# MySQL Configuration
DB_HOST=your-mysql-host
DB_USER=your-mysql-user
DB_PASSWORD=your-mysql-password
DB_DATABASE=your-database

# SSH Tunnel for MySQL
SSH_BASTION_HOST=your-bastion-host
SSH_BASTION_USER=your-ssh-user
SSH_BASTION_KEY_PATH=/path/to/key

# S3 Configuration
S3_BUCKET_NAME=your-s3-bucket
S3_ACCESS_KEY=your-access-key
S3_SECRET_KEY=your-secret-key

# Redshift Configuration
REDSHIFT_HOST=your-redshift-cluster
REDSHIFT_DATABASE=your-database
REDSHIFT_USER=your-user
REDSHIFT_PASSWORD=your-password
REDSHIFT_SCHEMA=public
```

#### Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Daily Sync Operations

#### Basic Table Sync
```bash
# Sync a single table (incremental)
python -m src.cli.main sync -t settlement.orders

# Sync multiple tables
python -m src.cli.main sync -t settlement.orders,settlement.customers
```

#### Check Sync Status
```bash
# View current system status
python -m src.cli.main status

# Check specific table watermark
python -m src.cli.main watermark get -t settlement.orders
```

### 3. Fresh/Full Sync

#### Reset and Sync from Specific Date
```bash
# Reset watermark
python -m src.cli.main watermark reset -t settlement.orders

# Set starting point
python -m src.cli.main watermark set -t settlement.orders --timestamp '2025-01-01 00:00:00'

# Run sync
python -m src.cli.main sync -t settlement.orders
```

### 4. Large Table Best Practices

#### For Tables with 100M+ Rows
```bash
# Use row limits for testing first
python -m src.cli.main sync -t large_table --limit 10000

# Monitor memory usage
python -m src.cli.main sync -t large_table --batch-size 5000

# Run during off-peak hours
nohup python -m src.cli.main sync -t large_table > sync.log 2>&1 &
```

### 5. Schema Changes Workflow

#### When MySQL Adds New Column
```sql
-- Step 1: MySQL automatically has new column
-- Step 2: Add to Redshift manually
ALTER TABLE your_schema.your_table ADD COLUMN new_column VARCHAR(200);

-- Step 3: Run sync normally
```

```bash
python -m src.cli.main sync -t your_schema.your_table
```

## 📖 Quick Command Reference

### Essential Commands

| Task | Command |
|------|---------|
| **Sync Table** | `python -m src.cli.main sync -t schema.table` |
| **Check Status** | `python -m src.cli.main status` |
| **View Watermark** | `python -m src.cli.main watermark get -t table` |
| **Reset Watermark** | `python -m src.cli.main watermark reset -t table` |
| **Set Start Date** | `python -m src.cli.main watermark set -t table --timestamp 'YYYY-MM-DD HH:MM:SS'` |
| **S3 Cleanup** | `python -m src.cli.main s3clean clean -t table --older-than 7d` |
| **Test Sync** | `python -m src.cli.main sync -t table --limit 1000` |

### Advanced Commands

| Task | Command |
|------|---------|
| **Backup Only** | `python -m src.cli.main sync -t table --backup-only` |
| **Load Only** | `python -m src.cli.main sync -t table --redshift-only` |
| **List S3 Files** | `python -m src.cli.main s3clean list -t table` |
| **Fix Row Counts** | `python -m src.cli.main watermark-count set-count -t table --count N --mode absolute` |

## ✅ Best Practices

### 1. **Regular Monitoring**
```bash
# Check sync status daily
python -m src.cli.main status

# Verify row counts match
python -m src.cli.main watermark get -t your_table
```

### 2. **Performance Optimization**

Create `redshift_keys.json` for better query performance:
```json
{
  "schema.your_table": {
    "distkey": "customer_id",
    "sortkey": ["created_date", "customer_id"]
  }
}
```

### 3. **S3 Storage Management**
```bash
# Clean old files weekly
python -m src.cli.main s3clean clean -t table --older-than 7d

# Check storage usage
python -m src.cli.main s3clean list -t table --show-timestamps
```

### 4. **Error Recovery**
```bash
# If sync fails, check watermark
python -m src.cli.main watermark get -t table

# Resume from where it stopped
python -m src.cli.main sync -t table

# For stuck syncs, reset and retry
python -m src.cli.main watermark reset -t table
```

## 🔧 Troubleshooting

### Common Issues

#### 1. "ModuleNotFoundError"
```bash
# Solution: Install dependencies
pip install -r requirements.txt
```

#### 2. "SSH Connection Failed"
```bash
# Check SSH key permissions
chmod 600 /path/to/ssh/key

# Test SSH connection
ssh -i /path/to/key user@bastion-host
```

#### 3. "Redshift COPY Failed"
```bash
# Check for schema mismatches
# Manually add missing columns to Redshift
ALTER TABLE schema.table ADD COLUMN missing_column VARCHAR(100);
```

#### 4. "Memory Error on Large Table"
```bash
# Reduce batch size
export BACKUP__BATCH_SIZE=5000
python -m src.cli.main sync -t large_table
```

### Getting Help

1. **Check Logs**: Detailed logs in console output
2. **Watermark Status**: `watermark get` shows current state
3. **Dry Run**: Use `--dry-run` to preview actions
4. **Documentation**: See `README.md` for detailed docs

## 🎓 Training Scenarios

### Scenario 1: Your First Sync
```bash
# 1. Check system status
python -m src.cli.main status

# 2. Sync a small table first
python -m src.cli.main sync -t settlement.small_table --limit 1000

# 3. Verify in Redshift
SELECT COUNT(*) FROM settlement.small_table;
```

### Scenario 2: Handling Failed Sync
```bash
# 1. Check what happened
python -m src.cli.main watermark get -t problem_table

# 2. Check S3 files
python -m src.cli.main s3clean list -t problem_table

# 3. Resume sync
python -m src.cli.main sync -t problem_table
```

### Scenario 3: Monthly Fresh Sync
```bash
# 1. Reset watermark
python -m src.cli.main watermark reset -t monthly_table

# 2. Set to first of month
python -m src.cli.main watermark set -t monthly_table --timestamp '2025-01-01 00:00:00'

# 3. Run full sync
python -m src.cli.main sync -t monthly_table
```

## 🚦 Getting Started Checklist

- [ ] Get `.env` file from team lead
- [ ] Install Python dependencies
- [ ] Test SSH connections
- [ ] Run `python -m src.cli.main status`
- [ ] Try syncing a small test table
- [ ] Verify data in Redshift
- [ ] Read about watermarks in detail
- [ ] Practice with s3clean commands

## 📞 Support

- **Documentation**: `/docs` folder and `*.md` files
- **Common Issues**: `TROUBLESHOOTING.md`
- **Architecture Details**: `ARCHITECTURE.md`

Welcome to the team! This tool will make your data syncing tasks much easier and more reliable. Start with small tables to get familiar, then work your way up to larger production tables.

---
*Remember: When in doubt, check the watermark status first!*