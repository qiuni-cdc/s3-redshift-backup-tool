# Watermark Interpretation Guide

## Overview

The watermark system tracks the **state and progress** of data synchronization from MySQL → S3 → Redshift. Each table has its own watermark that stores critical metadata about sync operations.

**Key Concept**: Watermarks enable **incremental syncs** by remembering:
- What data has been processed (timestamps, IDs)
- How much data was transferred (row counts, file counts)
- Current operation status (success, in_progress, failed)
- Which files are awaiting processing vs already loaded

## Watermark Structure

### Core Components

A watermark consists of **three main sections**:

1. **MySQL State** - Tracks data extraction from source database
2. **S3 State** - Tracks intermediate storage (unprocessed files)
3. **Redshift State** - Tracks data loading to target warehouse

## Viewing Watermark Data

### Basic Command
```bash
python -m src.cli.main watermark get -t <table_name> -p <pipeline_name>
```

### Example Output
```
Watermark for table: unidw.dw_parcel_pricing_temp
Pipeline: us_dw_unidw_2_public_pipeline

MySQL → S3 Backup Stage:
  Last Timestamp: 2025-01-15 14:30:00
  Last ID: 1234567
  Status: success
  Rows Extracted (Cumulative): 5,000,000
  Rows Extracted This Session: 50,000

S3 Storage:
  S3 Files Created: 46
  📦 Unprocessed S3 Files (awaiting Redshift load): 0 files
  ✅ Processed S3 Files (loaded to Redshift): 46 files

S3 → Redshift Loading Stage:
  Status: success
  Rows Loaded: 5,000,000
  Load Timestamp: 2025-01-15 15:00:00
```

## Field-by-Field Explanation

### MySQL State Fields

#### `last_timestamp` (MySQL → S3)
**What it means**: The most recent `updated_at` value from the last processed row.

**Purpose**: Starting point for next incremental sync.

**Example**: `2025-01-15 14:30:00`
- Next sync will fetch records WHERE `updated_at > '2025-01-15 14:30:00'` OR (`updated_at = '2025-01-15 14:30:00'` AND `id > last_id`)

**Important**: Used with `last_id` in hybrid CDC strategy to prevent data loss.

#### `last_id` (MySQL → S3)
**What it means**: The ID of the last processed row.

**Purpose**: Handles records with identical timestamps.

**Example**: `1234567`
- Next sync will fetch records with `id > 1234567` when timestamp equals watermark

**Why it matters**: Without this, records with the same timestamp as the watermark could be skipped.

#### `mysql_status`
**What it means**: Current state of MySQL → S3 backup operation.

**Possible Values**:
- `success` - Backup completed successfully
- `in_progress` - Backup is currently running or was interrupted
- `failed` - Backup encountered errors
- `not_started` - No backup attempted yet

**Interpretation**:
```
success → Safe to load to Redshift
in_progress → May have files ready, check S3 state
failed → Investigation needed, check error logs
```

#### `total_rows` (Cumulative)
**What it means**: **Lifetime total** of all rows extracted from MySQL across all sync sessions.

**Purpose**: Historical tracking and capacity planning.

**Example**: `5,000,000`
- This is the sum of ALL rows ever extracted for this table
- Grows with each sync session
- **Never resets** unless watermark is manually reset

**Calculation**:
```
Initial: 0
Session 1: +100,000 → 100,000
Session 2: +50,000 → 150,000
Session 3: +30,000 → 180,000
Total: 180,000 (cumulative)
```

#### `last_session_rows` (Session-Specific)
**What it means**: Rows extracted in the **most recent sync session only**.

**Purpose**: Show what the current/last execution accomplished.

**Example**: `50,000`
- This session processed 50,000 rows
- Previous sessions' row counts not included
- **Resets with each new session**

**Use Cases**:
- Airflow task metrics: "This task processed 50,000 rows"
- Performance monitoring: "Last sync took 10 minutes for 50,000 rows"
- Troubleshooting: "Why did last session only process 1,000 rows?"

**Critical Difference**:
```
total_rows (cumulative): 5,000,000 ← All sessions combined
last_session_rows: 50,000 ← Just the last session
```

**Interruption Handling** (NEW):
```
Session starts, processes 3 batches:
  Batch 1 (100 rows): last_session_rows = 100
  Batch 2 (100 rows): last_session_rows = 200
  💥 Crash!

Watermark shows: last_session_rows = 200 ✅
(Not just 100 from the last batch - shows total session progress!)
```

### S3 State Fields

#### `s3_file_count`
**What it means**: **Total S3 files created** for this table (unprocessed + processed).

**Calculation**: `len(backup_s3_files) + len(processed_s3_files)`

**Example**: `46`
- Could be 46 unprocessed files + 0 processed
- Or 0 unprocessed files + 46 processed
- Or 20 unprocessed + 26 processed = 46 total

#### `backup_s3_files` (Array)
**What it means**: S3 parquet files that have been **created but NOT yet loaded to Redshift** (unprocessed files).

**Purpose**: Track files awaiting Redshift load.

**Example**:
```python
[
  "incremental/table=us_dw_unidw_ssh_unidw_dw_parcel_pricing/20250115_143000_chunk_1_batch_1.parquet",
  "incremental/table=us_dw_unidw_ssh_unidw_dw_parcel_pricing/20250115_143000_chunk_1_batch_2.parquet"
]
```

**Interpretation**:
- **Count > 0**: Files ready for `--redshift-only` load
- **Count = 0**: No pending files, either all loaded or none created

**File Movement**: When loaded to Redshift, files move from `backup_s3_files` → `processed_s3_files`

#### `processed_s3_files` (Array)
**What it means**: S3 parquet files that have been **successfully loaded to Redshift**.

**Purpose**: Track completed files, prevent reloading.

**Example**:
```python
[
  "incremental/table=us_dw_unidw_ssh_unidw_dw_parcel_pricing/20250114_120000_chunk_1_batch_1.parquet",
  "incremental/table=us_dw_unidw_ssh_unidw_dw_parcel_pricing/20250114_120000_chunk_1_batch_2.parquet"
]
```

**Interpretation**:
- **Count > 0**: These files have been loaded, won't be reprocessed
- **Count = 0**: No files loaded yet (new table or reset watermark)

**Important**: `--redshift-only` skips files in this list.

### Redshift State Fields

#### `redshift_status`
**What it means**: Current state of S3 → Redshift load operation.

**Possible Values**:
- `success` - Load completed successfully
- `in_progress` - Load is currently running or was interrupted
- `failed` - Load encountered errors
- `not_started` - No load attempted yet

#### `redshift_rows_loaded`
**What it means**: Total rows loaded to Redshift from S3 files.

**Purpose**: Verify data consistency between MySQL and Redshift.

**Validation Check**:
```
mysql_rows_extracted (5,000,000) == redshift_rows_loaded (5,000,000) ✅
mysql_rows_extracted (5,000,000) != redshift_rows_loaded (4,950,000) ⚠️ Data loss!
```

#### `redshift_last_load_timestamp`
**What it means**: When the most recent Redshift load completed.

**Purpose**: Track load freshness and schedules.

**Example**: `2025-01-15 15:00:00`

## Common Scenarios & Interpretation

### Scenario 1: Successful Complete Sync

**Watermark State**:
```
MySQL State:
  Status: success
  Total Rows: 1,000,000
  Last Session Rows: 1,000,000

S3 State:
  Files Created: 20
  Unprocessed Files: 0
  Processed Files: 20

Redshift State:
  Status: success
  Rows Loaded: 1,000,000
```

**Interpretation**:
✅ Full pipeline completed successfully
- MySQL → S3: 1M rows extracted → 20 files
- S3 → Redshift: All 20 files loaded
- Data consistent end-to-end

**Next Action**: Ready for next incremental sync

---

### Scenario 2: Backup Complete, Redshift Load Pending

**Watermark State**:
```
MySQL State:
  Status: success
  Total Rows: 1,000,000
  Last Session Rows: 50,000

S3 State:
  Files Created: 5
  Unprocessed Files: 5  ← Files awaiting load
  Processed Files: 0

Redshift State:
  Status: not_started
  Rows Loaded: 950,000  ← Previous total
```

**Interpretation**:
⏳ Backup complete, load pending
- MySQL → S3: Successfully created 5 new files with 50K rows
- S3 → Redshift: Files ready but not loaded yet
- Gap: 50K rows in S3 not yet in Redshift

**Next Action**: Run `--redshift-only` to load pending files

**Command**:
```bash
python -m src.cli.main sync pipeline -p <pipeline> -t <table> --redshift-only
```

---

### Scenario 3: Interrupted Backup (In Progress)

**Watermark State**:
```
MySQL State:
  Status: in_progress  ← Interrupted!
  Total Rows: 800,000
  Last Session Rows: 30,000  ← Partial session progress
  Last Timestamp: 2025-01-15 14:20:00
  Last ID: 1150000

S3 State:
  Files Created: 3
  Unprocessed Files: 3
  Processed Files: 0

Redshift State:
  Status: not_started
  Rows Loaded: 770,000  ← Previous total
```

**Interpretation**:
⚠️ Backup was interrupted mid-session
- MySQL → S3: Processed 3 batches before crash
- Progress saved: Can resume from ID 1150000, timestamp 2025-01-15 14:20:00
- Gap: 30K rows in S3, not in Redshift yet
- **Key Insight**: `last_session_rows = 30,000` shows cumulative session progress before crash

**Next Action**: Resume backup (will continue from last watermark)

**Command**:
```bash
# Resume backup from interruption point
python -m src.cli.main sync pipeline -p <pipeline> -t <table>

# Then load to Redshift
python -m src.cli.main sync pipeline -p <pipeline> -t <table> --redshift-only
```

---

### Scenario 4: Failed Sync with Error

**Watermark State**:
```
MySQL State:
  Status: failed
  Error: "Connection timeout after 300s"
  Total Rows: 500,000
  Last Session Rows: 0

S3 State:
  Files Created: 0
  Unprocessed Files: 0
  Processed Files: 10

Redshift State:
  Status: success
  Rows Loaded: 500,000
```

**Interpretation**:
❌ Sync failed before creating any files
- MySQL → S3: Connection failed, no files created
- Previous state: 500K rows already in Redshift (from earlier runs)
- No data loss, but new data not extracted

**Next Action**: Investigate error, retry backup

**Troubleshooting**:
1. Check network connectivity
2. Check database load
3. Increase timeout settings
4. Retry sync

---

### Scenario 5: Partial Redshift Load

**Watermark State**:
```
MySQL State:
  Status: success
  Total Rows: 1,000,000
  Last Session Rows: 100,000

S3 State:
  Files Created: 10
  Unprocessed Files: 3  ← Remaining files
  Processed Files: 7  ← Already loaded

Redshift State:
  Status: in_progress  ← Interrupted during load
  Rows Loaded: 970,000  ← 7 files worth
```

**Interpretation**:
⏸️ Redshift load interrupted mid-process
- MySQL → S3: All 10 files created successfully
- S3 → Redshift: 7 of 10 files loaded (70% complete)
- Remaining: 3 files (30K rows) still need loading

**Next Action**: Resume Redshift load

**Command**:
```bash
python -m src.cli.main sync pipeline -p <pipeline> -t <table> --redshift-only
```

**Result**: Will load only the 3 remaining unprocessed files

---

### Scenario 6: Data Consistency Issue

**Watermark State**:
```
MySQL State:
  Status: success
  Total Rows: 1,000,000
  Last Session Rows: 100,000

S3 State:
  Files Created: 10
  Unprocessed Files: 0
  Processed Files: 10

Redshift State:
  Status: success
  Rows Loaded: 950,000  ← MISMATCH!
```

**Interpretation**:
🚨 Data inconsistency detected!
- MySQL extracted: 1,000,000 rows
- Redshift loaded: 950,000 rows
- Missing: 50,000 rows (5% data loss)

**Possible Causes**:
1. Files corrupted during transfer
2. Redshift COPY command failed silently
3. Watermark tracking bug (should be fixed in current version)

**Next Action**: Data validation and recovery

**Recovery Steps**:
```bash
# 1. List S3 files and verify contents
python -m src.cli.main s3clean list -t <table> -p <pipeline>

# 2. Check actual Redshift row count
SELECT COUNT(*) FROM schema.table_name;

# 3. If mismatch confirmed, reset and resync
python -m src.cli.main watermark reset -t <table> -p <pipeline>
python -m src.cli.main sync pipeline -p <pipeline> -t <table>
```

## Watermark Display Format

### CLI Output Structure

```
Watermark for table: <full_table_name>
Pipeline: <pipeline_name>

MySQL → S3 Backup Stage:
  Last Timestamp: <last_timestamp>         # Incremental sync starting point
  Last ID: <last_id>                       # For hybrid CDC strategy
  Status: <mysql_status>                   # success/in_progress/failed
  Rows Extracted (Cumulative): <total_rows>      # Lifetime total
  Rows Extracted This Session: <last_session_rows>  # Current session only

S3 Storage:
  S3 Files Created: <s3_file_count>        # Total files (unprocessed + processed)
  📦 Unprocessed S3 Files (awaiting Redshift load): <backup_count> files
  ✅ Processed S3 Files (loaded to Redshift): <processed_count> files

S3 → Redshift Loading Stage:
  Status: <redshift_status>                # success/in_progress/failed
  Rows Loaded: <redshift_rows_loaded>      # Total rows in Redshift
  Load Timestamp: <redshift_last_load_timestamp>
```

### File List Display (with `--show-files`)

```bash
python -m src.cli.main watermark get -t <table> -p <pipeline> --show-files
```

**Output**:
```
📦 Unprocessed S3 Files (awaiting Redshift load): 3 files
  1. incremental/table=us_dw_unidw_ssh_unidw_dw_parcel_pricing/20250115_143000_chunk_1_batch_1.parquet
  2. incremental/table=us_dw_unidw_ssh_unidw_dw_parcel_pricing/20250115_143000_chunk_1_batch_2.parquet
  3. incremental/table=us_dw_unidw_ssh_unidw_dw_parcel_pricing/20250115_143000_chunk_1_batch_3.parquet

✅ Processed S3 Files (loaded to Redshift): 2 files
  1. incremental/table=us_dw_unidw_ssh_unidw_dw_parcel_pricing/20250114_120000_chunk_1_batch_1.parquet
  2. incremental/table=us_dw_unidw_ssh_unidw_dw_parcel_pricing/20250114_120000_chunk_1_batch_2.parquet
```

## Watermark Operations

### View Watermark
```bash
# Basic view
python -m src.cli.main watermark get -t <table_name> -p <pipeline_name>

# With file details
python -m src.cli.main watermark get -t <table_name> -p <pipeline_name> --show-files
```

### Set Watermark (Manual Resume Point)
```bash
# Set starting ID for incremental sync
python -m src.cli.main watermark set -t <table_name> -p <pipeline_name> --id 1234567

# Set starting timestamp
python -m src.cli.main watermark set -t <table_name> -p <pipeline_name> --timestamp "2025-01-15 14:30:00"

# Set both (for hybrid CDC)
python -m src.cli.main watermark set -t <table_name> -p <pipeline_name> --id 1234567 --timestamp "2025-01-15 14:30:00"
```

### Reset Watermark (Full Resync)
```bash
# Reset all watermark data
python -m src.cli.main watermark reset -t <table_name> -p <pipeline_name>

# This will:
# - Clear last_timestamp and last_id
# - Reset row counts to 0
# - Clear file lists
# - Next sync will be a full table sync
```

**⚠️ Warning**: Resetting watermark triggers full table resync. Use only when necessary.

### Update Watermark (Internal Use)
```bash
# Update specific fields (used internally by sync operations)
# Not typically used manually
```

## Troubleshooting Guide

### Problem: Files exist but not being loaded

**Symptoms**:
```
Unprocessed Files: 10 files
Processed Files: 0
Redshift Status: not_started
```

**Solution**:
```bash
# Load files to Redshift
python -m src.cli.main sync pipeline -p <pipeline> -t <table> --redshift-only
```

---

### Problem: Row count mismatch

**Symptoms**:
```
Total Rows (MySQL): 1,000,000
Rows Loaded (Redshift): 950,000
```

**Solution**:
```bash
# 1. Verify Redshift actual count
SELECT COUNT(*) FROM schema.table_name;

# 2. List S3 files
python -m src.cli.main s3clean list -t <table> -p <pipeline>

# 3. If confirmed mismatch, reset and resync
python -m src.cli.main watermark reset -t <table> -p <pipeline>
python -m src.cli.main sync pipeline -p <pipeline> -t <table>
```

---

### Problem: Sync stuck in "in_progress"

**Symptoms**:
```
MySQL Status: in_progress (for hours)
Last Session Rows: 50,000
```

**Diagnosis**:
```bash
# Check if process is actually running
ps aux | grep "src.cli.main"

# Check S3 files created
python -m src.cli.main watermark get -t <table> -p <pipeline> --show-files
```

**Solution**:
```bash
# If no process running and files exist:
# 1. Resume backup (will continue from watermark)
python -m src.cli.main sync pipeline -p <pipeline> -t <table>

# 2. Or if backup complete, load to Redshift
python -m src.cli.main sync pipeline -p <pipeline> -t <table> --redshift-only
```

---

### Problem: Session rows show 0 after interruption

**Symptoms** (OLD BUG - Fixed):
```
Total Rows: 800,000
Last Session Rows: 0  ← Should show partial progress
Unprocessed Files: 3  ← Files exist!
```

**Status**: **Fixed in current version**
- Watermark now tracks `session_rows_total` during each batch
- Shows accurate session progress even after interruption
- Example: 3 batches processed → `last_session_rows = 300` ✅

**If you see this**: Update to latest version

---

### Problem: Duplicate files in both lists

**Symptoms** (OLD BUG - Fixed):
```
Unprocessed Files: 5 files
  - file1.parquet
  - file2.parquet
Processed Files: 5 files
  - file1.parquet  ← Duplicate!
  - file2.parquet  ← Duplicate!
```

**Status**: **Fixed in current version**
- Files now properly move from `backup_s3_files` → `processed_s3_files`
- No more duplicates

**If you see this**: Update to latest version

## Best Practices

### 1. Regular Watermark Checks
```bash
# Before sync - check current state
python -m src.cli.main watermark get -t <table> -p <pipeline>

# After sync - verify completion
python -m src.cli.main watermark get -t <table> -p <pipeline> --show-files
```

### 2. Validate Data Consistency
```bash
# Compare row counts
# MySQL total_rows should equal Redshift rows_loaded
```

### 3. Monitor File States
```bash
# Check for pending loads
# Unprocessed files > 0 → Run --redshift-only
```

### 4. Handle Interruptions Safely
```bash
# Don't reset watermark after interruption
# Resume sync - it will continue from last watermark
python -m src.cli.main sync pipeline -p <pipeline> -t <table>
```

### 5. Clean Up Old Files
```bash
# List S3 files for table
python -m src.cli.main s3clean list -t <table> -p <pipeline>

# Clean up after successful load
python -m src.cli.main s3clean clean -t <table> -p <pipeline>
```

## Integration with Airflow

### Metrics Available

```json
{
  "table_metrics": {
    "settlement.orders": {
      "rows_processed": 50000,          // From last_session_rows
      "rows_cumulative": 5000000,       // From total_rows
      "files_created": 5,               // From s3_file_count
      "mysql_status": "success",
      "redshift_status": "success",
      "duration_seconds": 120.5
    }
  }
}
```

### JSON Output Command
```bash
python -m src.cli.main sync pipeline \
  -p <pipeline> \
  -t <table> \
  --json-output sync_result.json
```

**Key Insight**: Airflow tasks use `last_session_rows`, not `total_rows`, to report what the current task accomplished.

## Summary

### Watermark Tracks Three Things:

1. **Where to start next** → `last_timestamp`, `last_id`
2. **What's been done** → `total_rows`, `last_session_rows`, file counts
3. **Current state** → `mysql_status`, `redshift_status`

### Key Metrics:

- **`total_rows`** → Lifetime cumulative (historical tracking)
- **`last_session_rows`** → Current session only (operational visibility)
- **`backup_s3_files`** → Unprocessed files awaiting Redshift load
- **`processed_s3_files`** → Files already loaded to Redshift

### Critical Commands:

```bash
# View watermark
python -m src.cli.main watermark get -t <table> -p <pipeline>

# Resume interrupted sync
python -m src.cli.main sync pipeline -p <pipeline> -t <table>

# Load pending files
python -m src.cli.main sync pipeline -p <pipeline> -t <table> --redshift-only

# Reset for full resync
python -m src.cli.main watermark reset -t <table> -p <pipeline>
```

### Data Validation:

Always verify: `mysql_rows_extracted == redshift_rows_loaded`

If mismatch → investigate and potentially resync.

---

**Related Documentation**:
- `Sophie_Branch_Development.md` (lines 784-1036) - Watermark Logic Architecture
- `Codes_Explain.md` - Technical implementation details
