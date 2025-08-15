# 📖 S3 to Redshift Backup System - User Manual

## Table of Contents
- [🎯 System Overview](#-system-overview)
- [🚀 Quick Start Guide](#-quick-start-guide)
- [⚙️ System Configuration](#️-system-configuration)
- [🔧 Command Reference](#-command-reference)
- [📊 Backup Strategies](#-backup-strategies)
- [🎛️ Monitoring & Status](#️-monitoring--status)
- [🔍 Data Exploration](#-data-exploration)
- [🛠️ Troubleshooting](#️-troubleshooting)
- [📈 Advanced Usage](#-advanced-usage)
- [🔒 Security & Best Practices](#-security--best-practices)

---

## 🎯 System Overview

### What This System Does
The S3 to Redshift Backup System is a production-grade data pipeline that:
- **Incrementally backs up** MySQL settlement data to S3
- **Converts data** to analytics-optimized Parquet format
- **Loads data into Redshift** for business intelligence
- **Handles parcel status deduplication** to show latest status only
- **Partitions by time** for efficient querying
- **Tracks watermarks** to process only new/changed records
- **Provides multiple strategies** for different data sizes and requirements

### Key Features
- ✅ **Two backup strategies** (Sequential, Inter-table Parallel)
- ✅ **Complete S3 to Redshift pipeline** with CSV conversion method
- ✅ **Latest status views** for parcel tracking deduplication
- ✅ **Production-ready table structure** with performance optimizations
- ✅ **Incremental processing** with watermark management
- ✅ **Production monitoring** with structured logging
- ✅ **Error handling** with retry mechanisms
- ✅ **Data validation** and quality checks
- ✅ **Secure connections** via SSH tunneling
- ✅ **Analytics-ready output** (Parquet + time partitioning)

### Architecture Overview
```
┌─────────────────┐    SSH Tunnel    ┌─────────────────┐    CSV Files     ┌─────────────────┐
│   MySQL DB      │ ◄─────────────── │  Backup System  │ ──────────────► │   S3 Storage    │
│   (Settlement)  │                  │   - Sequential  │                 │   (Parquet/CSV) │
└─────────────────┘                  │   - Parallel    │                 └─────────────────┘
         │                           │   - Chunked     │                           │
         │                           └─────────────────┘                           │
         │                                     │                       CSV COPY   │
         │                                     ▼                                   ▼
         │                            ┌─────────────────┐ SSH Tunnel    ┌─────────────────┐
         └───────────────────────────►│   Monitoring    │◄──────────────│   Redshift DW   │
                                      │   & Logging     │               │ Latest Status   │
                                      └─────────────────┘               │     Views       │
                                                                        └─────────────────┘
```

---

## 🎊 **PRODUCTION READY - COMPLETE PIPELINE OPERATIONAL**

### 🚀 **Current System Status**
- **✅ S3 Backup Pipeline**: Fully operational with 3 backup strategies
- **✅ Redshift Data Warehouse**: 2.1+ million records loaded and verified
- **✅ Latest Status Views**: Parcel deduplication solution deployed
- **✅ Business Intelligence**: Ready for analytics, reporting, and dashboards
- **✅ Performance Optimized**: DISTKEY/SORTKEY applied for fast queries

### 📊 **Production Redshift Access**
- **Database**: `dw`
- **Table**: `public.settlement_normal_delivery_detail`
- **Views**: `public.settlement_latest_delivery_status` (use this for parcel queries)
- **Records**: 2,131,906 settlement delivery transactions
- **Query Performance**: Sub-second response for parcel lookups

---

## 🚀 Quick Start Guide

### Prerequisites
- Python 3.12+ environment
- Access to settlement database via SSH bastion
- AWS S3 bucket with read/write permissions
- SSH private key for bastion host access

### 1. Environment Setup

```bash
# Navigate to the system directory
cd /home/qi_chen/s3-redshift-backup

# Activate the virtual environment
source test_env/bin/activate

# Verify configuration
cat .env
```

### 2. System Health Check

```bash
# Check overall system status
python -m src.cli.main status

# Display backup information
python -m src.cli.main info
```

### 3. Your First Backup

```bash
# Start with a dry run (safe)
python -m src.cli.main backup \
  -t settlement.settlement_claim_detail \
  -s sequential \
  --dry-run

# Run the actual backup
python -m src.cli.main backup \
  -t settlement.settlement_claim_detail \
  -s sequential
```

### 4. Explore Your Data

```bash
# View S3 backup data
python display_s3_data.py

# Comprehensive dashboard
python backup_dashboard.py

# Detailed data inspection
python inspect_backup_data.py
```

---

## ⚙️ System Configuration

### Environment Variables (.env file)

The system uses environment variables for configuration. Here's your current production setup:

```bash
# Database Configuration
DB_HOST=your-database-host.example.com
DB_PORT=3306
DB_USER=your_db_user
DB_PASSWORD=your_db_password
DB_DATABASE=settlement

# SSH Configuration (for bastion host access)
SSH_BASTION_HOST=your.mysql.bastion.host
SSH_BASTION_USER=your_ssh_user
SSH_BASTION_KEY_PATH=/path/to/your/ssh/key.pem
SSH_LOCAL_PORT=0

# S3 Configuration
S3_BUCKET_NAME=your-s3-bucket-name
S3_ACCESS_KEY=YOUR_AWS_ACCESS_KEY_ID
S3_SECRET_KEY=YOUR_AWS_SECRET_ACCESS_KEY
S3_REGION=us-east-1
S3_INCREMENTAL_PATH=incremental/
S3_HIGH_WATERMARK_KEY=watermark/last_run_timestamp.txt

# Backup Performance Settings
BACKUP_BATCH_SIZE=10000
BACKUP_MAX_WORKERS=4
BACKUP_NUM_CHUNKS=4
BACKUP_RETRY_ATTEMPTS=3
BACKUP_TIMEOUT_SECONDS=300

# Logging Configuration
LOG_LEVEL=INFO
DEBUG=false
```

### Configuration Validation

```bash
# Validate all configuration settings
python -m src.cli.main config

# Test specific connections
python test_exact_colab.py  # Database connectivity
python display_s3_data.py   # S3 connectivity
```

---

## 🔧 Command Reference

### Main CLI Commands

#### Backup Command
```bash
python -m src.cli.main backup [OPTIONS]
```

**Options:**
- `-t, --tables` (required): Table names to backup
- `-s, --strategy` (required): Backup strategy (sequential|inter-table)
- `--dry-run`: Show what would be backed up without actually doing it
- `--estimate`: Show time estimates before starting
- `--config-file`: Use custom configuration file
- `--log-file`: Custom log file path
- `--debug`: Enable debug logging

**Examples:**
```bash
# Single table, sequential processing
python -m src.cli.main backup \
  -t settlement.settlement_claim_detail \
  -s sequential

# Multiple tables, parallel processing
python -m src.cli.main backup \
  -t settlement.partner_info \
  -t settlement.settlement_claim_detail \
  -s inter-table

# Large table processing
python -m src.cli.main backup \
  -t settlement.settlement_normal_delivery_detail \
  -s sequential

# Dry run with time estimation
python -m src.cli.main backup \
  -t settlement.settlement_claim_detail \
  -s sequential \
  --dry-run \
  --estimate

# Debug mode with custom log file
python -m src.cli.main backup \
  -t settlement.settlement_claim_detail \
  -s sequential \
  --debug \
  --log-file backup_debug.log
```

#### Status Command
```bash
python -m src.cli.main status
```
Shows system health, connectivity status, and recent backup activity.

#### Sync Command (Production Pipeline)
```bash
python -m src.cli.main sync [OPTIONS]
```

**Complete MySQL → S3 → Redshift synchronization with flexible schema discovery.**

**Options:**
- `-t, --tables` (required): Table names to sync
- `-s, --strategy`: Backup strategy (sequential|inter-table)
- `--backup-only`: Only run backup (MySQL → S3), skip Redshift loading
- `--redshift-only`: Only run Redshift loading (S3 → Redshift), skip backup
- `--limit`: Limit rows per query (for testing/development)
- `--verify-data`: Verify row counts after sync
- `--dry-run`: Test run without execution
- `--max-workers`: Override number of worker threads
- `--batch-size`: Override batch size

**Examples:**
```bash
# Full sync (MySQL → S3 → Redshift)
python -m src.cli.main sync -t settlement.settlement_claim_detail

# Multiple tables with parallel strategy  
python -m src.cli.main sync \
  -t settlement.settlement_claim_detail \
  -t settlement.settlement_normal_delivery_detail \
  -s inter-table

# Backup only (MySQL → S3)
python -m src.cli.main sync -t settlement.table_name --backup-only

# Redshift loading only (S3 → Redshift) - preserves manual watermarks
python -m src.cli.main sync -t settlement.table_name --redshift-only

# Test run without execution
python -m src.cli.main sync -t settlement.table_name --dry-run

# Testing with row limits (development)
python -m src.cli.main sync -t settlement.table_name --limit 1000

# Quick test with logging
python -m src.cli.main --log-file test.log sync -t settlement.table_name --limit 5000
```

#### Watermark Management Commands
```bash
python -m src.cli.main watermark [OPERATION] [OPTIONS]
```

**Operations:**
- `get`: Get current table watermark
- `set`: Set new watermark timestamp for table  
- `reset`: Delete watermark completely (fresh start)
- `list`: List all table watermarks

**Options:**
- `-t, --table`: Table name for table-specific watermark operations
- `--timestamp`: Timestamp for set operation (YYYY-MM-DD HH:MM:SS)

**Examples:**
```bash
# View current watermark
python -m src.cli.main watermark get -t settlement.settlement_claim_detail

# Set manual starting timestamp for incremental sync
python -m src.cli.main watermark set \
  -t settlement.settlement_claim_detail \
  --timestamp "2025-08-09 20:00:01"

# Reset watermark completely (fresh start)
python -m src.cli.main watermark reset -t settlement.settlement_claim_detail

# List all table watermarks
python -m src.cli.main watermark list
```

**Watermark Use Cases:**

1. **Load Existing S3 Files After Specific Timestamp:**
```bash
python -m src.cli.main watermark reset -t settlement.table_name
python -m src.cli.main watermark set -t settlement.table_name --timestamp '2025-08-09 20:00:01'
python -m src.cli.main sync -t settlement.table_name --redshift-only
```

2. **Complete Fresh Sync from Timestamp:**
```bash
# Clean S3 files (optional)
aws s3 rm s3://bucket/incremental/ --recursive --exclude "*" --include "*table_name*"

# Set starting point and run full sync
python -m src.cli.main watermark reset -t settlement.table_name
python -m src.cli.main watermark set -t settlement.table_name --timestamp '2025-08-09 20:00:01'
python -m src.cli.main sync -t settlement.table_name
```

#### S3 Storage Management (s3clean)
```bash
python -m src.cli.main s3clean [OPERATION] [OPTIONS]
```

**PRODUCTION-READY S3 CLEANUP SYSTEM** - Enterprise-grade storage management with comprehensive safety features.

**Operations:**
- `list` - List S3 files for a table or all tables with detailed metadata
- `clean` - Clean S3 files for a specific table with safety confirmations
- `clean-all` - Clean S3 files for all tables (use with extreme caution)

**Options:**
- `-t, --table` - Table name to clean (required for clean operation)
- `--older-than` - Delete files older than X time units (e.g., "7d", "24h", "30m")
- `--pattern` - File pattern to match (e.g., "batch_*", "*.parquet")
- `--dry-run` - Show what would be deleted without actually deleting
- `--force` - Skip confirmation prompts for automated workflows

**Examples:**

**Safe Exploration and Preview:**
```bash
# List files for specific table with sizes and dates
python -m src.cli.main s3clean list -t settlement.settlement_return_detail

# Preview what would be deleted (recommended first step)
python -m src.cli.main s3clean clean -t settlement.settlement_return_detail --dry-run
```

**Targeted Cleanup Operations:**
```bash
# Clean all files for a table (with confirmation prompt)
python -m src.cli.main s3clean clean -t settlement.settlement_return_detail

# Clean files older than 7 days (recommended approach)
python -m src.cli.main s3clean clean -t settlement.settlement_return_detail --older-than "7d"

# Clean with pattern matching for specific batches
python -m src.cli.main s3clean clean -t settlement.settlement_return_detail --pattern "batch_*"

# Automated cleanup without prompts (for scripts)
python -m src.cli.main s3clean clean -t settlement.settlement_return_detail --force

# System-wide cleanup (DANGEROUS - requires double confirmation)
python -m src.cli.main s3clean clean-all --older-than "30d"
```

**Enterprise Safety Features:**
- ✅ **Multi-layer Protection** - Dry-run preview, confirmation prompts, table validation
- ✅ **Time-based Filtering** - Only clean files older than specified age (`7d`, `24h`, `30m`)
- ✅ **Pattern Matching** - Target specific file patterns for selective cleanup
- ✅ **Table Isolation** - Prevents accidental deletion across wrong tables
- ✅ **Size Reporting** - Shows exactly how much space will be freed
- ✅ **File Type Validation** - Only processes `.parquet` backup files
- ✅ **Error Prevention** - Cannot accidentally clean wrong table or recent files

**Production-Ready Workflow:**
1. **Explore**: `s3clean list -t table_name` - Review current storage usage
2. **Preview**: `s3clean clean -t table_name --dry-run` - Validate cleanup plan
3. **Execute**: `s3clean clean -t table_name --older-than "7d"` - Perform safe cleanup
4. **Verify**: `s3clean list -t table_name` - Confirm results and space freed

**Maintenance Schedule Recommendations:**
- **Weekly**: Clean files older than 7 days for active tables
- **Monthly**: Clean files older than 30 days for archived tables  
- **Quarterly**: Review overall storage usage patterns

#### Info Command
```bash
python -m src.cli.main info
```
Displays available backup strategies and their use cases.

#### Clean Command
```bash
python -m src.cli.main clean [OPTIONS]
```

**Options:**
- `--bucket`: S3 bucket name
- `--prefix`: S3 prefix to clean
- `--days`: Keep files newer than N days
- `--confirm`: Confirm deletion

**Examples:**
```bash
# Clean old backup files (older than 30 days)
python -m src.cli.main clean \
  --bucket your-s3-bucket-name \
  --prefix incremental/ \
  --days 30 \
  --confirm

# Dry run of cleanup (see what would be deleted)
python -m src.cli.main clean \
  --bucket your-s3-bucket-name \
  --prefix incremental/ \
  --days 7
```

---

## 📊 Backup Strategies

### 1. Sequential Strategy
**Best for:** Small to medium number of tables, critical data consistency

```bash
python -m src.cli.main backup \
  -t settlement.settlement_claim_detail \
  -s sequential
```

**Characteristics:**
- ✅ Processes one table at a time
- ✅ Maximum reliability and error isolation
- ✅ Lower resource usage
- ⏱️ Slower for multiple tables
- 🎯 Use when: Data consistency is critical, limited resources

### 2. Inter-table Parallel Strategy
**Best for:** Many small to medium tables, good I/O capacity

```bash
python -m src.cli.main backup \
  -t settlement.partner_info \
  -t settlement.settlement_claim_detail \
  -t settlement.billing_summary \
  -s inter-table
```

**Characteristics:**
- ✅ Processes multiple tables simultaneously
- ✅ Faster overall completion time
- ✅ Good resource utilization
- ⚠️ Higher memory and connection usage
- 🎯 Use when: Multiple tables need backup, system can handle parallelism

### Strategy Recommendation

**For most use cases, use `sequential` strategy** - it's reliable, fast, and handles all table sizes efficiently.

**Use `inter-table` only when:**
- You have multiple tables to backup simultaneously
- Your system can handle parallel connections
- You need to optimize total processing time across multiple tables

### Strategy Selection Guide

| Table Size | Number of Tables | Recommended Strategy | Example |
|------------|------------------|---------------------|---------|
| Small-Medium | 1 | Sequential | Single claim table |
| Small-Medium | 2-10 | Inter-table | Multiple settlement tables |
| Large | 1 | Sequential | Delivery detail table |
| Mixed | Mixed | Sequential (safe) | Production environments |

---

## 🎛️ Monitoring & Status

### System Dashboard

```bash
# Comprehensive system status
python backup_dashboard.py
```

**Output includes:**
- S3 bucket overview
- Backup file statistics
- Per-table breakdown
- Watermark status
- System health checks
- Recent activity summary

### S3 Data Explorer

```bash
# Browse S3 backup structure
python display_s3_data.py
```

**Shows:**
- File counts and sizes
- Table organization
- Time coverage
- Backup activity by date

### Detailed Data Inspection

```bash
# Inspect parquet file contents
python inspect_backup_data.py
```

**Provides:**
- Schema analysis
- Sample data preview
- Column statistics
- Data quality assessment

### Log Analysis

**Structured JSON Logs:**
```json
{
  "strategy": "sequential",
  "event": "Backup operation completed",
  "success": true,
  "duration_seconds": 2.59,
  "successful_tables": ["settlement.settlement_claim_detail"],
  "failed_tables": [],
  "watermark_updated": true,
  "logger": "backup",
  "level": "info",
  "timestamp": "2025-08-06T17:37:55.352744Z"
}
```

**Key Log Events:**
- `CLI initialized successfully`: System startup
- `Connection established`: Database/S3 connectivity
- `Table processing started`: Individual table backup begins
- `Backup operation completed`: Final results
- `Watermark updated successfully`: Incremental tracking

---

## 🔍 Data Exploration

### S3 Data Structure

Your backups are stored in S3 with this structure:
```
s3://your-s3-bucket-name/
├── incremental/
│   └── settlement.settlement_normal_delivery_detail/
│       └── year=2025/month=07/day=28/hour=20/
│           ├── 2025-07-28_20-12-03_batch_0001.parquet (1.4 MB)
│           ├── 2025-07-28_20-12-03_batch_0002.parquet (1.4 MB)
│           └── ... (105 more files)
└── watermark/
    └── last_run_timestamp.txt (current watermark)
```

### Redshift Data Warehouse Integration

#### Production Redshift Setup
The system now includes complete S3-to-Redshift data loading:

**Connection Details:**
- **Database**: `dw`
- **Schema**: `public` 
- **Table**: `settlement_normal_delivery_detail`
- **Access**: Via SSH tunnel through bastion host

**Table Structure:**
```sql
-- Production table with performance optimizations
CREATE TABLE public.settlement_normal_delivery_detail (
    -- All 51 columns from MySQL source
    ID BIGINT,
    billing_num VARCHAR(500),
    partner_id BIGINT,
    -- ... (complete column list)
)
-- Performance optimizations for parcel tracking
DISTKEY(ant_parcel_no)           -- Even distribution + join optimization
SORTKEY(create_at, billing_num); -- Time-based queries optimized
```

**Data Loading Status:**
- **Rows Loaded**: 2.1+ million settlement delivery records
- **Method**: Parquet → CSV → Redshift COPY
- **Update Frequency**: Incremental (based on backup schedule)
- **Data Quality**: All columns preserved, proper data types

#### Latest Status Views (Parcel Deduplication)

**Problem**: Parcels can have multiple status updates, but users need only the latest status.

**Solution**: Three intelligent views handle deduplication:

1. **`public.settlement_latest_delivery_status`** (Primary View)
```sql
-- Get latest status for specific parcel
SELECT * FROM public.settlement_latest_delivery_status 
WHERE ant_parcel_no = 'BAUNI000300014750782';

-- Count parcels by current status
SELECT latest_status, COUNT(*) as parcel_count
FROM public.settlement_latest_delivery_status
GROUP BY latest_status
ORDER BY parcel_count DESC;
```

2. **`public.settlement_partner_latest_status`** (Partner Focus)
```sql
-- Partner performance analysis
SELECT partner_id, COUNT(*) as total_parcels,
       COUNT(CASE WHEN latest_status = 'DELIVERED' THEN 1 END) as delivered
FROM public.settlement_partner_latest_status
GROUP BY partner_id;
```

3. **`public.settlement_status_summary`** (Analytics Dashboard)
```sql
-- Status distribution for dashboards
SELECT * FROM public.settlement_status_summary;
```

**Important**: Always use the views above, never query the base table directly for status lookups.

#### Business Intelligence Queries

**Delivery Performance Analysis:**
```sql
SELECT 
    partner_id,
    COUNT(*) as total_parcels,
    COUNT(CASE WHEN latest_status = 'DELIVERED' THEN 1 END) as delivered_count,
    ROUND(COUNT(CASE WHEN latest_status = 'DELIVERED' THEN 1 END) * 100.0 / COUNT(*), 2) as delivery_rate
FROM public.settlement_latest_delivery_status
WHERE partner_id IS NOT NULL
GROUP BY partner_id
ORDER BY total_parcels DESC;
```

**Recent Activity Monitoring:**
```sql
SELECT ant_parcel_no, partner_id, latest_status, last_status_update_at
FROM public.settlement_latest_delivery_status
WHERE last_status_update_at >= CURRENT_DATE - 7
ORDER BY last_status_update_at DESC
LIMIT 100;
```

**Revenue Analysis:**
```sql
SELECT 
    DATE_TRUNC('day', create_at) as delivery_date,
    COUNT(*) as parcel_count,
    SUM(CASE WHEN net_price ~ '^[0-9.]+$' THEN net_price::DECIMAL(10,2) ELSE 0 END) as daily_revenue
FROM public.settlement_latest_delivery_status
WHERE create_at >= CURRENT_DATE - 30
GROUP BY DATE_TRUNC('day', create_at)
ORDER BY delivery_date DESC;
```

### Querying Your Data

#### Using Amazon Athena (S3 Direct)
```sql
-- Create external table
CREATE EXTERNAL TABLE settlement_delivery_detail (
  ID bigint,
  billing_num string,
  partner_id bigint,
  customer_id bigint,
  ant_parcel_no string,
  parcel_scan_time timestamp,
  actual_weight decimal(5,3),
  net_price decimal(4,2),
  latest_status string,
  create_at timestamp,
  update_at timestamp
  -- ... other columns
)
PARTITIONED BY (
  year string,
  month string,
  day string,
  hour string
)
LOCATION 's3://your-s3-bucket-name/incremental/settlement.settlement_normal_delivery_detail/'
TBLPROPERTIES ('has_encrypted_data'='false')

-- Query recent deliveries
SELECT 
  partner_id,
  COUNT(*) as delivery_count,
  AVG(net_price) as avg_price
FROM settlement_delivery_detail
WHERE year='2025' AND month='07' AND day='28'
GROUP BY partner_id
ORDER BY delivery_count DESC
```

#### Using AWS CLI
```bash
# List backup files
aws s3 ls s3://your-s3-bucket-name/incremental/ --recursive

# Download a specific file
aws s3 cp s3://your-s3-bucket-name/incremental/settlement.settlement_normal_delivery_detail/year=2025/month=07/day=28/hour=20/2025-07-28_20-12-03_batch_0001.parquet ./

# Sync entire backup directory
aws s3 sync s3://your-s3-bucket-name/incremental/ ./local_backup/
```

#### Using Python (Pandas/PyArrow)
```python
import pandas as pd
import boto3

# Read directly from S3
s3_path = 's3://your-s3-bucket-name/incremental/settlement.settlement_normal_delivery_detail/'
df = pd.read_parquet(s3_path)

# Analyze data
print(f"Total records: {len(df):,}")
print(f"Date range: {df['parcel_scan_time'].min()} to {df['parcel_scan_time'].max()}")
print(f"Partners: {df['partner_id'].nunique()}")

# Partner analysis
partner_stats = df.groupby('partner_id').agg({
    'ID': 'count',
    'net_price': 'mean',
    'actual_weight': 'sum'
}).round(2)
print(partner_stats)
```

---

## 🛠️ Troubleshooting

### Common Issues and Solutions

#### 1. SSH Connection Problems

**Symptoms:**
- "SSH tunnel failed to start"
- "Authentication failed"
- Permission denied errors

**Solutions:**
```bash
# Check SSH key permissions
ls -la /path/to/your/ssh/key.pem
# Should show: -rw------- (600 permissions)

# Fix permissions if needed
chmod 600 /path/to/your/ssh/key.pem

# Test SSH connection manually
ssh -i /path/to/your/ssh/key.pem chenqi@your.mysql.bastion.host

# Check paramiko version (should be <3.0)
pip list | grep paramiko
```

#### 2. Database Connection Issues

**Symptoms:**
- "Database connection failed"
- "Access denied for user"
- Table validation errors

**Solutions:**
```bash
# Test database connectivity
python test_exact_colab.py

# Check credentials in .env file
grep DB_ .env

# Verify table exists
python check_claim_simple.py
```

#### 3. S3 Access Problems

**Symptoms:**
- "S3 bucket not accessible"
- "Access Denied" errors
- Upload failures

**Solutions:**
```bash
# Test S3 connectivity
python display_s3_data.py

# Check AWS credentials
grep S3_ .env

# Verify bucket permissions
aws s3 ls s3://your-s3-bucket-name/
```

#### 4. No Data to Backup

**Symptoms:**
- "No new data to backup"
- Empty backup runs
- Zero files created

**Expected Behavior:**
This is often **correct behavior** for incremental systems!

**Verification:**
```bash
# Check what data would be backed up
python -m src.cli.main backup -t settlement.settlement_claim_detail -s sequential --dry-run

# View current watermark
python backup_dashboard.py | grep -A5 "Watermark"

# Check table contents (if accessible)
python check_claim_simple.py
```

#### 5. Watermark and Sync Issues

**Symptoms:**
- "Found X files, filtered to 0 files for loading"
- "No S3 parquet files found"
- Manual watermarks not working
- All files being loaded when expecting incremental

**Solutions:**

**Check Watermark State:**
```bash
# View current watermark details
python -m src.cli.main watermark get -t settlement.table_name

# Look for:
# - Last Data Timestamp: Should match your expected date
# - Last Extraction Time: Should be None for manual watermarks
# - Backup Strategy: Should be 'manual_cli' for manual watermarks
```

**Reset and Set Fresh Watermark:**
```bash
# Complete reset and fresh start
python -m src.cli.main watermark reset -t settlement.table_name
python -m src.cli.main watermark set -t settlement.table_name --timestamp '2025-08-09 20:00:01'

# Verify the watermark was set correctly
python -m src.cli.main watermark get -t settlement.table_name
```

**Check S3 File Timestamps:**
```bash
# List S3 files with creation times
aws s3 ls s3://your-bucket/incremental/ --recursive | grep table_name

# Files created before your watermark timestamp will be filtered out
```

**Use Correct Sync Commands:**
```bash
# For loading existing S3 files with manual watermark (preserves watermark)
python -m src.cli.main sync -t settlement.table_name --redshift-only

# For fresh end-to-end sync (backup will overwrite manual watermark)
python -m src.cli.main sync -t settlement.table_name
```

**Debug Filtering Logic:**
Look for these log messages to understand what's happening:
- ✅ `"Using manual watermark-based incremental Redshift loading: files after YYYY-MM-DD"`
- ❌ `"Using session-based incremental Redshift loading: files from X to Y"` (means manual watermark was overwritten)
- ✅ `"backup_strategy = 'manual_cli'"` in debug output
- ❌ `"backup_strategy = 'sequential'"` (means backup process ran and overwrote manual watermark)

#### 6. S3 Clean Issues

**Symptoms:**
- "S3 clean operation failed"
- "Failed to list S3 files"
- "No files found to delete" when files should exist

**Solutions:**

**Check S3 Connectivity:**
```bash
# Test basic S3 access
python -m src.cli.main s3clean list

# Check specific table files
python -m src.cli.main s3clean list -t settlement.table_name
```

**Verify File Existence:**
```bash
# List with AWS CLI to compare
aws s3 ls s3://your-bucket/incremental/ --recursive | grep table_name

# Check file ages
python -m src.cli.main s3clean list -t settlement.table_name
```

**Safe Cleanup Process:**
```bash
# 1. Always start with dry run
python -m src.cli.main s3clean clean -t settlement.table_name --dry-run

# 2. Use time filters for safety
python -m src.cli.main s3clean clean -t settlement.table_name --older-than "7d" --dry-run

# 3. Execute only after verification
python -m src.cli.main s3clean clean -t settlement.table_name --older-than "7d"
```

**Common Issues:**
- **"No files found"**: Table name might not match S3 file patterns
- **"Invalid time format"**: Use formats like "7d", "24h", "30m"
- **Large file counts**: Command processes up to 1000 files per operation

#### 7. Performance Issues

**Symptoms:**
- Slow backup performance
- Memory usage issues
- Timeouts

**Solutions:**
```bash
# Adjust batch size (in .env)
BACKUP_BATCH_SIZE=5000  # Smaller batches

# Reduce workers
BACKUP_MAX_WORKERS=2    # Fewer parallel workers

# Use sequential strategy for large tables
python -m src.cli.main backup -t large_table -s sequential

# Monitor with debug logging
python -m src.cli.main backup -t table_name -s sequential --debug
```

### Log Analysis for Debugging

**Enable Debug Mode:**
```bash
# Set in .env file
DEBUG=true
LOG_LEVEL=DEBUG

# Or use command-line flag
python -m src.cli.main backup -t table_name -s sequential --debug
```

**Key Log Entries to Check:**
```json
// Connection establishment
{"event": "SSH tunnel established", "local_port": 12345}
{"event": "Database connection established", "database": "settlement"}

// Table processing  
{"event": "Table validation successful", "table_name": "settlement.table"}
{"event": "Processing backup window", "start_time": "...", "end_time": "..."}

// Results
{"event": "Backup operation completed", "success": true}
{"event": "Watermark updated successfully", "new_watermark": "..."}
```

---

## 📈 Advanced Usage

### Custom Backup Schedules

#### Cron Job Setup
```bash
# Edit crontab
crontab -e

# Daily backup at 2 AM
0 2 * * * cd /home/qi_chen/s3-redshift-backup && source test_env/bin/activate && python -m src.cli.main backup -t settlement.settlement_claim_detail -s sequential >> /var/log/backup.log 2>&1

# Hourly incremental backup
0 * * * * cd /home/qi_chen/s3-redshift-backup && source test_env/bin/activate && python -m src.cli.main backup -t settlement.settlement_normal_delivery_detail -s sequential >> /var/log/backup_hourly.log 2>&1
```

#### Systemd Service
Create `/etc/systemd/system/settlement-backup.service`:
```ini
[Unit]
Description=Settlement Data Backup Service
After=network.target

[Service]
Type=oneshot
User=qi_chen
WorkingDirectory=/home/qi_chen/s3-redshift-backup
Environment=PATH=/home/qi_chen/s3-redshift-backup/test_env/bin
ExecStart=/home/qi_chen/s3-redshift-backup/test_env/bin/python -m src.cli.main backup -t settlement.settlement_claim_detail -s sequential
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

Enable and test:
```bash
sudo systemctl enable settlement-backup.service
sudo systemctl start settlement-backup.service
sudo systemctl status settlement-backup.service
```

### Batch Processing Multiple Tables

```bash
# Create a backup script
cat > backup_all_settlement.sh << 'EOF'
#!/bin/bash
cd /home/qi_chen/s3-redshift-backup
source test_env/bin/activate

echo "Starting settlement backup: $(date)"

# Backup all settlement tables
tables=(
  "settlement.partner_info"
  "settlement.settlement_claim_detail" 
  "settlement.billing_summary"
  "settlement.delivery_status"
)

for table in "${tables[@]}"; do
  echo "Backing up $table..."
  python -m src.cli.main backup -t "$table" -s sequential
  if [ $? -eq 0 ]; then
    echo "✅ $table backup completed"
  else
    echo "❌ $table backup failed"
  fi
done

echo "Settlement backup completed: $(date)"
EOF

chmod +x backup_all_settlement.sh
./backup_all_settlement.sh
```

### Monitoring and Alerting

#### Basic Health Check Script
```bash
cat > health_check.sh << 'EOF'
#!/bin/bash
cd /home/qi_chen/s3-redshift-backup
source test_env/bin/activate

# Check system status
python -m src.cli.main status > /tmp/backup_status.txt

if grep -q "ERROR" /tmp/backup_status.txt; then
  echo "ALERT: Backup system health check failed"
  cat /tmp/backup_status.txt
  # Send alert (email, Slack, etc.)
  exit 1
else
  echo "✅ Backup system healthy"
  exit 0
fi
EOF
```

#### Integration with Monitoring Tools
```python
# Example: Send metrics to CloudWatch
import boto3
from datetime import datetime

def send_backup_metrics(success, duration, rows_processed):
    cloudwatch = boto3.client('cloudwatch')
    
    metrics = [
        {
            'MetricName': 'BackupSuccess',
            'Value': 1 if success else 0,
            'Unit': 'Count'
        },
        {
            'MetricName': 'BackupDuration',
            'Value': duration,
            'Unit': 'Seconds'  
        },
        {
            'MetricName': 'RowsProcessed',
            'Value': rows_processed,
            'Unit': 'Count'
        }
    ]
    
    cloudwatch.put_metric_data(
        Namespace='SettlementBackup',
        MetricData=metrics
    )
```

### Performance Tuning

#### Optimal Settings by Data Size

**Small Tables (< 100K rows):**
```bash
BACKUP_BATCH_SIZE=10000
BACKUP_MAX_WORKERS=2
# Strategy: sequential
```

**Medium Tables (100K - 1M rows):**
```bash
BACKUP_BATCH_SIZE=20000
BACKUP_MAX_WORKERS=4
# Strategy: sequential or inter-table
```

**Large Tables (> 1M rows):**
```bash
BACKUP_BATCH_SIZE=50000
BACKUP_MAX_WORKERS=4
BACKUP_NUM_CHUNKS=8
# Strategy: sequential (recommended)
```

#### Network Optimization
```bash
# For slow connections
BACKUP_BATCH_SIZE=5000
BACKUP_TIMEOUT_SECONDS=600

# For fast connections
BACKUP_BATCH_SIZE=25000
BACKUP_MAX_WORKERS=8
```

---

## 🔒 Security & Best Practices

### Security Configuration

#### SSH Key Management
```bash
# Secure SSH key storage
chmod 600 /path/to/your/ssh/key.pem
chown qi_chen:qi_chen /path/to/your/ssh/key.pem

# Key rotation (when needed)
# 1. Generate new key
ssh-keygen -t rsa -b 4096 -f new_chenqi.pem
# 2. Add to bastion host
# 3. Update SSH_BASTION_KEY_PATH in .env
# 4. Test connectivity
# 5. Remove old key from bastion
```

#### Environment Variables Protection
```bash
# Secure .env file permissions
chmod 600 .env
chown qi_chen:qi_chen .env

# Never commit .env to version control
echo ".env" >> .gitignore

# Use environment-specific configs
cp .env .env.production
cp .env .env.staging
```

#### S3 Security
```bash
# Use IAM roles instead of access keys (when possible)
# Limit S3 permissions to specific bucket and paths
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject"
      ],
      "Resource": [
        "arn:aws:s3:::your-s3-bucket-name/incremental/*",
        "arn:aws:s3:::your-s3-bucket-name/watermark/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:ListBucket"
      ],
      "Resource": "arn:aws:s3:::your-s3-bucket-name"
    }
  ]
}
```

### Operational Best Practices

#### 1. Testing Strategy
```bash
# Always test with dry-run first
python -m src.cli.main backup -t new_table -s sequential --dry-run

# Test in staging environment
cp .env .env.backup
# Update .env with staging credentials
python -m src.cli.main backup -t test_table -s sequential
# Restore production .env
mv .env.backup .env
```

#### 2. Backup Validation
```bash
# Verify backup completion
python backup_dashboard.py | grep -A5 "Recent Activity"

# Check data integrity
python inspect_backup_data.py | grep -A10 "Data Shape"

# Validate S3 files
aws s3 ls s3://your-s3-bucket-name/incremental/ --recursive --human-readable
```

#### 3. Disaster Recovery
```bash
# Backup configuration files
cp .env ./backups/.env.$(date +%Y%m%d)
cp src/config/schemas.py ./backups/schemas.py.$(date +%Y%m%d)

# Document recovery procedures
cat > RECOVERY.md << 'EOF'
# Disaster Recovery Procedures

## System Recovery
1. Restore virtual environment: `python -m venv test_env`
2. Install dependencies: `pip install -r requirements.txt`
3. Restore .env configuration
4. Test connectivity: `python test_exact_colab.py`
5. Verify S3 access: `python display_s3_data.py`

## Data Recovery
1. List available S3 backups: `aws s3 ls s3://bucket/incremental/`
2. Download specific backup: `aws s3 cp s3://bucket/path ./restore/`
3. Restore to database (if needed)
EOF
```

#### 4. Monitoring and Maintenance
```bash
# Weekly system health check
python -m src.cli.main status > weekly_health_$(date +%Y%m%d).log

# Monthly cleanup of old backups
python -m src.cli.main clean --bucket your-s3-bucket-name --prefix incremental/ --days 90 --confirm

# Quarterly performance review
python backup_dashboard.py > quarterly_report_$(date +%Y%m%d).txt
```

### Compliance and Auditing

#### Audit Trail
```bash
# Enable detailed logging
LOG_LEVEL=INFO
DEBUG=false

# Centralized log collection
# Send logs to ELK stack, Splunk, or CloudWatch Logs

# Example log entry for compliance
{
  "event": "Backup operation completed",
  "user": "qi_chen", 
  "timestamp": "2025-08-06T17:37:55.352744Z",
  "tables": ["settlement.settlement_claim_detail"],
  "records_processed": 0,
  "data_size_mb": 0.0,
  "success": true,
  "duration_seconds": 2.59
}
```

#### Data Retention
```bash
# Define retention policies in .env
BACKUP_RETENTION_DAYS=90
ARCHIVE_RETENTION_DAYS=2555  # 7 years

# Automated cleanup script
cat > cleanup_retention.sh << 'EOF'
#!/bin/bash
python -m src.cli.main clean \
  --bucket your-s3-bucket-name \
  --prefix incremental/ \
  --days $BACKUP_RETENTION_DAYS \
  --confirm
EOF
```

---

## 🎯 Summary

This user manual provides comprehensive guidance for operating your S3 to Redshift backup system. The system is production-ready and has successfully migrated from your Google Colab prototype to a robust, enterprise-grade solution.

### Key Takeaways
1. **Start with dry runs** for safety
2. **Monitor regularly** using provided dashboards  
3. **Choose appropriate strategies** based on data size
4. **Follow security best practices** for production use
5. **Test thoroughly** before production deployment

### Getting Help
- Check logs for detailed error information
- Use `--debug` flag for troubleshooting
- Run health checks with `python -m src.cli.main status`
- Refer to this manual for specific scenarios

**Your backup system is ready for production use!** 🚀