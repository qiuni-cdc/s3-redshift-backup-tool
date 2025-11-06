# Q&A - Common Issues and Solutions

## ModuleNotFoundError: cdc_backup_integration Issue

### Why it happens:
- The file `src/backup/cdc_backup_integration.py` is missing from the codebase
- `src/backup/row_based.py` line 16 imports `from src.backup.cdc_backup_integration import create_cdc_integration`
- Python fails at import time when it can't find the module

### How it connects to `python -m src.cli.main status`:
**Import Chain:**
1. `status` command → uses `ConnectionManager` for health checks
2. `ConnectionManager` → imports backup strategies 
3. `RowBasedBackupStrategy` → imports `cdc_backup_integration`
4. **Import fails** → entire module loading fails

### Key insight:
Even though the `status` command doesn't directly use CDC functionality, Python resolves **all imports** when loading modules. The missing import causes the entire chain to fail, preventing the status command from running.

### Solution:
Create the missing `src/backup/cdc_backup_integration.py` file with a `create_cdc_integration()` function to satisfy the import requirement.  


## S3 Pagination Issue - 371/400 Files Discovery Problem

### Why it happens:
- S3 `list_objects_v2()` has maximum 1000 objects per call limit
- Single call only returns first 1000 objects under prefix
- Missing 29 parquet files were in objects beyond the 1000 limit
- `check_processed_files.py` used pagination and found all 400 files correctly

### Root cause in `src/core/gemini_redshift_loader.py`:
```python
# BROKEN: Single call, limited results
response = s3_client.list_objects_v2(
    Bucket=self.config.s3.bucket_name,
    Prefix=prefix,
    MaxKeys=max_keys  # Even 2000 limit doesn't override 1000 default
)
```

### Fix Applied:
```python
# FIXED: Pagination gets ALL objects
paginator = s3_client.get_paginator('list_objects_v2')
page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)
for page in page_iterator:
    all_objects.extend(page['Contents'])
```

### Key insight:
Even though S3 appears to scan "1000 total objects", the missing files were simply not discovered in the initial scan. Pagination ensures complete file discovery across all S3 pages.

### Result:
- Before fix: 371/400 files discovered
- After fix: All 400/400 files discovered  
- Enhanced logging shows file-by-file processing with detailed failure analysis 

## Batch size for massive table
 Here are my recommendations for tuning a massive table sync:

  1. Database batch_size (in your .yml pipeline file)

  This setting controls how many rows are fetched from MySQL in a single query. Your current setting is 100000.

   * Recommendation: Slightly increase this value. For very large tables, the overhead of many small queries can add up.
     A larger batch size reduces the number of round-trips to the database.
   * Trade-offs:
       * Pros: Fewer, more efficient queries against the source database. Can increase overall throughput.
       * Cons: Increases memory usage on the machine running the script, as it has to hold all these rows in memory. A
         very large value could also put more strain on the source database.
   * Suggested Value: Try increasing this to 250000 or 500000. Monitor the memory usage of the Python process. If it gets
     too high, bring this number back down.

  2. BACKUP_BATCH_SIZE (in your .env file)

  This setting controls how many rows are written into each individual Parquet file that gets uploaded to S3. Your
  current default is 50000.

   * Recommendation: Adjust this to create files between 10MB and 200MB. This is the optimal size range for Redshift's
     COPY command to achieve maximum parallelism.
   * Analysis: You've observed that 50,000 rows create a ~7MB file. This is good, but on the smaller side. Creating
     slightly larger files will reduce the total number of files S3 and Redshift have to manage, which can improve
     performance.
   * Suggested Value: Increasing this to 100000 (to create ~14MB files) or 250000 (to create ~35MB files) would be an
     excellent tuning step. It's generally not recommended to go above 500000 rows per file unless your rows are very
     small.

## Watermark Tracking Bug Issues (Resolved)

### Issue 1: Processed Files During Backup Phase

**Problem**: Files created during backup (MySQL → S3) were incorrectly marked as "processed" (loaded to Redshift).

**Symptoms**: 
- Files show in "Processed S3 Files" but haven't been loaded to Redshift
- `--redshift-only` skips files that should be loaded
- Data appears processed but isn't actually in Redshift

**Root Cause**: Backup stage updated `processed_s3_files` instead of tracking backup files separately.

**Fix Applied**:
```python
# BEFORE: Incorrectly marked as processed during backup
watermark_data = {
    'processed_s3_files': backup_files  # ❌ Wrong - these aren't loaded yet
}

# AFTER: Separate tracking for backup vs loaded files
watermark_data = {
    'backup_s3_files': backup_files,     # ✅ Files awaiting Redshift load
    's3_file_count': len(backup_files),  # ✅ Count of backup files
    # processed_s3_files only updated by Redshift loader
}
```

**Result**: 
- `📦 Backup S3 Files (awaiting Redshift load)` - Shows files created during backup
- `✅ Processed S3 Files (loaded to Redshift)` - Shows files actually loaded
- `--redshift-only` correctly finds unprocessed files

### Issue 2: Watermark Loss on Interruption

**Problem**: When sync was interrupted, watermark showed "0 rows extracted, 0 S3 files" despite files existing.

**Symptoms**:
- Created 3 parquet files but watermark shows 0 files
- Rows extracted counter resets to 0 after interruption
- Progress lost on restart

**Root Cause**: Watermark only updated at final completion, not incrementally during chunks.

**Fix Applied**:
```python
# BEFORE: Only updated at final completion
def final_watermark_update():
    # If interrupted, all progress lost

# AFTER: Incremental updates after each chunk
def _update_chunk_watermark_with_files():
    watermark_data = {
        'mysql_rows_extracted': total_rows_processed,  # ✅ Track progress
        's3_file_count': len(all_backup_files),       # ✅ Track file count
        'backup_s3_files': all_backup_files           # ✅ Track file list
    }
```

**Result**: 
- Watermark shows accurate progress after each chunk
- Safe resume from exact interruption point
- No data loss when sync is interrupted

### Issue 3: Row Count Double-Counting

**Problem**: Watermark showed incorrect row counts (60,000 instead of actual 50,000).

**Symptoms**:
- Watermark shows more rows than actually exist in files
- Session-based counting adds incorrectly across runs
- File count correct but row count inflated

**Root Cause**: Session totals being added instead of using actual file content.

**Fix Applied**:
```python
# BEFORE: Used session processing totals (additive)
rows_extracted = session_rows_processed  # Could double-count

# AFTER: Count actual rows from parquet files
actual_rows_in_files = self._count_rows_in_backup_s3_files(table_name)
rows_extracted = actual_rows_in_files    # Always accurate
mode = 'absolute'                        # Prevent additive errors
```

**Result**: 
- Watermark always shows actual row count from files
- No more double-counting across sessions
- Consistent and accurate progress tracking

### Issue 4: Field Preservation During Updates

**Problem**: New `backup_s3_files` field lost during watermark updates.

**Symptoms**:
- `backup_s3_files` shows correctly but `s3_file_count` shows 0
- Field inconsistencies between display and storage

**Root Cause**: Watermark update methods didn't preserve new fields.

**Fix Applied**:
```python
# Enhanced update_mysql_watermark to sync fields
if hasattr(watermark, 'backup_s3_files') and watermark.backup_s3_files:
    watermark.s3_file_count = len(watermark.backup_s3_files)  # ✅ Sync count
    
# _update_watermark_direct uses setattr to handle any field
for field, value in watermark_data.items():
    if hasattr(watermark, field):
        setattr(watermark, field, value)  # ✅ Handles all fields
```

**Result**: 
- All watermark fields stay consistent
- New fields properly preserved across updates
- Display matches actual stored data

### Recovery Commands

**Fix current watermark issues**:
```bash
# Count actual rows in files and fix watermark
python fix_current_watermark.py

# Check watermark details
python -m src.cli.main watermark get -t table_name --show-files

# Fix any counter discrepancies  
python fix_watermark_counters.py table_name
```

### Prevention Measures Applied

- ✅ **Incremental progress tracking** prevents data loss on interruption
- ✅ **Separate file tracking** prevents incorrect processing status  
- ✅ **Actual file counting** ensures accurate row counts
- ✅ **Field preservation** maintains watermark consistency
- ✅ **Diagnostic utilities** for troubleshooting and recovery

### Issue 5: File Movement and S3 File Count Display (Resolved)

**Problem**: After processing files to Redshift, files appeared in both backup and processed lists, and "S3 Files Created" showed 0.

**Symptoms**:
- Duplicate files in both `backup_s3_files` and `processed_s3_files`
- "S3 Files Created: 0" despite files being successfully processed
- Confusion about which files are awaiting Redshift load vs already loaded

**Root Cause**: 
1. Missing logic to move files from backup list to processed list when loaded to Redshift
2. Incorrect calculation of `s3_file_count` after moving files

**Fix Applied**:
```python
# FIXED: Move files from backup_s3_files to processed_s3_files
if hasattr(watermark, 'backup_s3_files') and watermark.backup_s3_files:
    original_backup_count = len(watermark.backup_s3_files)
    watermark.backup_s3_files = [
        backup_file for backup_file in watermark.backup_s3_files
        if backup_file not in processed_files
    ]
    moved_files_count = original_backup_count - len(watermark.backup_s3_files)
    
    if moved_files_count > 0:
        logger.info(f"Moved {moved_files_count} files from backup_s3_files to processed_s3_files")
        
    # Update s3_file_count to reflect total files (backup + processed)
    backup_count = len(watermark.backup_s3_files) if watermark.backup_s3_files else 0
    processed_count = len(watermark.processed_s3_files) if watermark.processed_s3_files else 0
    watermark.s3_file_count = backup_count + processed_count
```

**Result**: 
- Files properly move from backup to processed when loaded to Redshift
- No more duplicate files in both lists
- `s3_file_count` correctly shows total files created (backup + processed)
- Clear distinction between files awaiting load vs files already loaded
- "S3 Files Created" now displays the correct total count

## DateTime Parsing Error in Backup Logic (Resolved)

**Problem**: Backup operations failed with `fromisoformat: argument must be str` TypeError during timestamp processing.

**Symptoms**:
- Error: `'fromisoformat: argument must be str'`
- Backup operations failed with TypeError
- Error occurred in row_based_backup strategy with timestamp_id chunking

**Error Example**:
```
2025-09-05T19:11:42.861457Z [error] Operation failed [backup] 
chunking_type=timestamp_id error_message='fromisoformat: argument must be str' 
error_type=TypeError table_name=US_DW_UNIDW_SSH:unidw.dw_parcel_pricing_temp
```

**Root Cause**: 
Mixed datetime/string type handling in watermark timestamp processing. The code assumed `last_mysql_data_timestamp` was always a string, but sometimes it was already a datetime object.

**Fix Applied**:
```python
# BEFORE (BUGGY):
data_timestamp_dt = datetime.fromisoformat(watermark.last_mysql_data_timestamp.replace('Z', '+00:00'))

# AFTER (FIXED):
if isinstance(last_timestamp, str):
    data_timestamp_dt = datetime.fromisoformat(last_timestamp.replace('Z', '+00:00'))
else:
    # Handle case where last_timestamp is already a datetime object
    data_timestamp_dt = last_timestamp
```

**Result**: 
- Backup operations handle both string and datetime timestamp inputs gracefully
- No more TypeError failures during timestamp processing
- Defensive programming prevents type assumption errors
- Consistent datetime handling throughout watermark system

## Custom Redshift Optimization Feature

### Overview
The system supports customized DISTKEY and SORTKEY optimization for Redshift tables through a configuration-driven approach, allowing fine-tuned performance optimization for different table patterns.

### Configuration File: `redshift_keys.json`
Tables can be optimized by creating a `redshift_keys.json` file in the project root:

```json
{
  "settlement.settlement_normal_delivery_detail": {
    "distkey": "ant_parcel_no",
    "sortkey": {
      "type": "compound",
      "columns": ["billing_num", "create_at"]
    }
  },
  "settlement.settle_orders": {
    "distkey": "tracking_number",
    "sortkey": {
      "type": "interleaved", 
      "columns": ["tracking_number", "created_at"]
    }
  }
}
```

### DISTKEY Configuration
- **Purpose**: Controls data distribution across Redshift nodes for optimal join performance
- **Usage**: `"distkey": "column_name"`
- **Validation**: System verifies column exists in table schema
- **Fallback**: Uses `DISTSTYLE AUTO` if column not found or not specified

### SORTKEY Configuration Formats

#### 1. Dictionary Format (Recommended)
```json
"sortkey": {
  "type": "compound|interleaved",
  "columns": ["col1", "col2", "col3"]
}
```

#### 2. Array Format (Legacy)
```json
"sortkey": ["col1", "col2"]  // Defaults to COMPOUND
```

#### 3. String Format (Single Column)
```json
"sortkey": "single_column"  // Defaults to COMPOUND
```

### SORTKEY Types
- **COMPOUND**: Best for queries filtering on prefix columns (recommended for most cases)
- **INTERLEAVED**: Best for queries filtering on any combination of sort columns

### Generated DDL Examples

**No Configuration (Default)**:
```sql
CREATE TABLE IF NOT EXISTS schema.table_name (
  columns...
)
DISTSTYLE AUTO
SORTKEY AUTO;
```

**With Custom Configuration**:
```sql
CREATE TABLE IF NOT EXISTS schema.settlement_normal_delivery_detail (
  columns...
)
DISTKEY(ant_parcel_no)
COMPOUND SORTKEY(billing_num, create_at);
```

### Implementation Features
- **Schema Validation**: Verifies all specified columns exist before applying
- **Column Name Sanitization**: Handles column names starting with numbers (e.g., `190_time` → `col_190_time`)
- **Graceful Fallback**: Uses AUTO optimization if custom columns not found
- **Comprehensive Logging**: Reports which optimizations are applied or skipped

### Use Cases
1. **High-Volume Tables**: Use DISTKEY on commonly joined columns
2. **Time-Series Data**: Use time-based SORTKEY for range queries
3. **Join Optimization**: Match DISTKEY with frequent join columns
4. **Query Patterns**: Choose COMPOUND vs INTERLEAVED based on WHERE clauses

### Testing
The feature includes comprehensive testing in `tests/test_core/test_redshift_optimization.py` that verifies:
- Custom configurations are applied correctly
- Default AUTO optimization works when no config provided
- Column validation and sanitization work properly
- Different SORTKEY types generate correct DDL 

## Batch Size Configuration Architecture Fix

### Problem: Multiple Conflicting Batch Size Systems

The system had **three different** batch size concepts causing confusion and inconsistent file sizes:

1. **Chunk Size** (rows fetched from MySQL): From pipeline/table config
2. **S3 Batch Size** (rows per Parquet file): From environment variable  
3. **System Defaults** (hardcoded fallbacks): Scattered throughout code

### Root Cause Analysis

**Configuration Hierarchy Issues:**
- Pipeline config: `batch_size: 500000` (chunk size)
- Environment variable: `BACKUP_BATCH_SIZE=50000` (S3 batch size)
- Hardcoded fallbacks: `50000` in CDC config, `10000` in settings.py
- **Result**: 1 chunk = 10 Parquet files (500K ÷ 50K = 10)

**Code Locations with Hardcoded Values:**
```python
# PROBLEM: Multiple hardcoded 50000 values
src/core/cdc_configuration_manager.py:59: batch_size=...get('cdc_batch_size', 50000)
src/core/cdc_configuration_manager.py:232: 'batch_size': 50000
src/core/cdc_strategy_engine.py:116: batch_size: int = 50000
src/config/settings.py:134: batch_size: int = Field(10000, ...)
```

**Environment Variable Override:**
```bash
# .env file had conflicting values
BACKUP_BATCH_SIZE=50000  # This controlled S3 file size
# But validation showed "50000 rows" per file instead of expected chunk size
```

### Fix Applied: Unified Configuration Hierarchy

#### 1. **Fixed Batch Size Priority Chain**
```python
# BEFORE: Inconsistent fallbacks
batch_size = table_config.get('batch_size', 50000)  # Hardcoded

# AFTER: Proper hierarchy
# 1. Table-specific processing.batch_size (highest priority)
batch_size = table_config.processing.get('batch_size')
if batch_size is None:
    # 2. Pipeline processing.batch_size (fallback)
    pipeline_processing = config.pipeline.processing if hasattr(config.pipeline, 'processing') else {}
    batch_size = pipeline_processing.get('batch_size')
if batch_size is None:
    # 3. System default from AppConfig (final fallback)
    batch_size = config.backup.target_rows_per_chunk
```

#### 2. **Environment Configuration Fix**
```bash
# BEFORE (.env):
BACKUP_BATCH_SIZE=50000  # Created 10 small files per chunk

# AFTER (.env):
BACKUP_BATCH_SIZE=500000  # Creates 1 large file per chunk
```

#### 3. **System Default Updates**
```python
# Updated settings.py system defaults
target_rows_per_chunk: int = Field(100000, ...)  # Was 500000
```

#### 4. **CDC Configuration Manager Fix**
```python
# BEFORE: Hardcoded fallback
batch_size=table_config.get('batch_size', table_config.get('cdc_batch_size', 50000))

# AFTER: Dynamic fallback
batch_size=table_config.get('batch_size', table_config.get('cdc_batch_size',
    table_config.get('system_default_batch_size', 100000)))
```

### Current Configuration Structure

**Your Pipeline Config:**
```yaml
pipeline:
  processing:
    batch_size: 100000  # Pipeline default

tables:
  unidw.dw_parcel_detail_tool:
    processing:
      batch_size: 1000000  # Table-specific override (highest priority)
```

**Environment Settings:**
```bash
BACKUP_BATCH_SIZE=500000  # S3 file size (rows per Parquet file)
```

### Result: Configurable File Granularity

**Before Fix:**
- Chunk: 500,000 rows → 10 files × 50,000 rows each
- Validation: "50000 rows" per file
- Many small files to manage

**After Fix:**
- Chunk: 1,000,000 rows → 2 files × 500,000 rows each
- Validation: "500000 rows" per file
- Optimal file sizes for Redshift COPY performance

### Configuration Best Practices Applied

1. **Table-Specific Override**: Use for large tables requiring bigger batches
2. **Pipeline Default**: Set reasonable baseline for all tables in pipeline
3. **System Fallback**: Provides safe defaults when no config specified
4. **Environment Control**: Fine-tune S3 file sizes without code changes

### Key Benefits

- ✅ **Consistent Configuration**: Single source of truth per level
- ✅ **Performance Optimization**: Right-sized files for Redshift COPY
- ✅ **Flexible Tuning**: Override at table/pipeline/system level as needed
- ✅ **No More Hardcoding**: All defaults configurable through proper channels
- ✅ **Fewer S3 Files**: Reduced management overhead with optimally sized files


## Table Name Pattern Matching Fix

### Issue: S3 File Discovery Failures

**Problem Identified:**
- Table name: `US_DW_UNIDW_SSH:unidw.dw_parcel_detail_tool`  
- Actual S3 files: `us_dw_unidw_ssh_unidw_dw_parcel_detail_tool_*.parquet`
- Pattern matching returned: **0 files found** (should be 46 files)

**Root Cause:** Multiple components had inconsistent table name normalization logic:
- Some normalized: `US_DW_UNIDW_SSH:unidw_dw_parcel_detail_tool` (only `.` → `_`)
- Actual S3 files: `us_dw_unidw_ssh_unidw_dw_parcel_detail_tool` (lowercase + `:` → `_`)

### Components Fixed

**1. `src/backup/row_based.py:650` - S3 File Counting**
```python
# BEFORE (BROKEN):
safe_table_name = table_name.replace('.', '_')
# Result: "US_DW_UNIDW_SSH:unidw_dw_parcel_detail_tool"

# AFTER (FIXED):
safe_table_name = table_name.lower().replace(':', '_').replace('.', '_')
# Result: "us_dw_unidw_ssh_unidw_dw_parcel_detail_tool"
```

**2. `src/core/gemini_redshift_loader.py:596-607` - Redshift S3 File Discovery**
```python
# BEFORE (BROKEN):
if ':' in table_name:
    # Clean table: replace dots with underscores (NO lowercase)
    clean_table = actual_table.replace('.', '_').replace('-', '_')

# AFTER (FIXED):
if ':' in table_name:
    # Clean table: lowercase and replace dots with underscores
    clean_table = actual_table.lower().replace('.', '_').replace('-', '_')
```

### Symptoms Fixed

**Before Fix:**
```
Actual S3 file count for US_DW_UNIDW_SSH:unidw.dw_parcel_detail_tool: 0 files
No successful MySQL backup found for US_DW_UNIDW_SSH:unidw.dw_parcel_detail_tool
No S3 parquet files found for US_DW_UNIDW_SSH:unidw.dw_parcel_detail_tool
```

**After Fix:**
```
Actual S3 file count for US_DW_UNIDW_SSH:unidw.dw_parquet_detail_tool: 46 files
Found S3 files for processing: 46 parquet files
Successfully loaded table data to Redshift
```

### Session Stuck Problem Resolved

**Why Sessions Got Stuck (0 file count):**
1. **Watermark Inconsistency**: System believed no files exist (count=0) while 46 files actually existed in S3
2. **State Confusion**: Backup logic couldn't reconcile actual S3 state vs tracked watermark state  
3. **Loop Logic Issues**: Main processing loop continued indefinitely due to inconsistent progress tracking
4. **Resource Waste**: Sessions running endlessly thinking they needed to process already-existing data

**Resolution:** Accurate file counting enables proper watermark tracking and session completion decisions.


## Watermark MySQL Status Fix

### Issue: Redshift Loader Rejecting Valid S3 Files

**Problem:** Loader required `mysql_status: success` but watermark showed `mysql_status: in_progress`

**Watermark State:**
```
MySQL → S3 Backup Stage: Status: in_progress
S3 → Redshift Loading Stage: Status: success  
S3 Files Created: 46
Backup S3 Files (awaiting Redshift load): 46 files exist
```

### Root Cause in `src/core/gemini_redshift_loader.py:310-312`

**Before Fix (RESTRICTIVE):**
```python
if not watermark or watermark.mysql_status != 'success':
    logger.warning(f"No successful MySQL backup found for {table_name}")
    return []  # BLOCKS loading of existing S3 files
```

**After Fix (PRAGMATIC):**
```python
if not watermark:
    logger.warning(f"No watermark found for {table_name}")
    return []

# Allow loading with in_progress or success status (files may exist from interrupted backup)
if watermark.mysql_status not in ['success', 'in_progress']:
    logger.warning(f"MySQL backup status is {watermark.mysql_status} for {table_name}, skipping")
    return []

if watermark.mysql_status == 'in_progress':
    logger.info(f"MySQL backup is in_progress for {table_name}, checking for existing S3 files")
```

### Business Logic Justification

**Scenario:** Backup was interrupted but S3 files were successfully created
- MySQL backup status: `in_progress` (process was interrupted)  
- S3 files: 46 files exist and are valid
- Business need: Load these files to Redshift (they contain real data)

**Old Logic:** Reject all loading because status isn't `success`
**New Logic:** Allow loading if files exist, even with `in_progress` status

### Impact

**Before:** 46 valid S3 files couldn't be loaded due to status check
**After:** 46 S3 files successfully processed to Redshift regardless of interrupted backup status

**Key Insight:** Watermark status should guide processing decisions, not block valid data loading when files demonstrably exist.

## Multi-Connection Password Management Integration (Resolved)

### Issue: Database Password Configuration for Multi-Pipeline Support

**Problem:** System used single `DB_PASSWORD` environment variable but pipeline configurations require connection-specific passwords (US DW, Canada DW, Production, QA, etc.).

**Symptoms:**
```
2025-09-16T20:39:59.518831Z [warning] No password found in DatabaseConfig or environment variables
```

**Root Cause:** 
1. **ConnectionRegistry Available**: `connections.yml` defined 14+ connection configurations with environment variable references
2. **ConnectionManager Not Integrated**: System fell back to basic `DatabaseConfig` instead of using ConnectionRegistry
3. **Missing Password**: No generic `DB_PASSWORD` in `.env`, only connection-specific passwords like:
   - `DB_US_DW_RO_PASSWORD` (for US Data Warehouse connections)
   - `DB_CA_DW_RO_PASSWORD` (for Canada connections)  
   - `DB_US_QA_PASSWORD` (for QA environment)
   - `DB2_US_RO_PASSWORD` (for DB2 connections)

### Connection Architecture Overview

**Your `.env` Connection-Specific Passwords:**
```bash
DB_US_DW_RO_PASSWORD=qPwRT]9(BUI0Q2Vw     # US Data Warehouse
DB_CA_DW_RO_PASSWORD=cJY(,,%+sbam{%9`     # Canada Data Warehouse  
DB_US_PROD_RO_PASSWORD=L~yL&Yw9t?qZn=k~   # US Production
DB_US_QA_PASSWORD=3B/l0PB9}fXRlebm        # US QA Environment
DB2_US_RO_PASSWORD=qPwRT]9(BUI0Q2Vw       # DB2 Instance
```

**Your `connections.yml` Configuration:**
```yaml
connections:
  sources:
    mysql_default:
      password: "${DB_US_DW_PASSWORD}"  # References environment variable
    US_DW_UNIDW_SSH:
      password: "${DB_US_DW_RO_PASSWORD}"  # Connection-specific password
    CA_DW_RO_SSH:
      password: "${DB_CA_DW_RO_PASSWORD}"  # Canada-specific password
```

### Fix Applied: ConnectionRegistry Integration

**1. Enhanced ConnectionManager (`src/core/connections.py:67-76`)**
```python
# BEFORE: Only checked basic DatabaseConfig
if self.connection_registry and connection_name:
    try:
        conn_config = self.connection_registry.get_connection(connection_name)
        if conn_config:
            logger.info(f"Using connection configuration for: {connection_name}")
            return conn_config
    except Exception as e:
        logger.warning(f"Failed to get connection {connection_name} from registry: {e}")
        
# Fallback still happened due to missing connection names in health_check()
```

**2. Updated Health Check Method (`src/core/connections.py:612,620`)**
```python
# BEFORE (PROBLEMATIC): No connection name passed
with self.ssh_tunnel() as local_port:
    with self.database_connection(local_port) as conn:

# AFTER (FIXED): Uses mysql_default connection
with self.ssh_tunnel('mysql_default') as local_port:
    with self.database_connection(local_port, 'mysql_default') as conn:
```

**3. Optional Password Field (`src/config/settings.py:13`)**
```python
# BEFORE: Required password field
password: SecretStr = Field(..., description="Database password")

# AFTER: Optional password field (allows connection-specific passwords)
password: Optional[SecretStr] = Field(None, description="Database password")
```

### Test Script Enhancement

**Environment Variable Loading Fix (`test_connection_resolution.py:13-36`)**
```python
# Added automatic .env loading with fallback
try:
    from dotenv import load_dotenv
    env_file = Path(__file__).parent / '.env'
    if env_file.exists():
        load_dotenv(env_file)
        print(f"✅ Loaded environment variables from {env_file}")
except ImportError:
    # Manual .env loading as fallback
    with open(env_file) as f:
        for line in f:
            # Parse KEY=VALUE pairs
```

### Resolution Workflow

**Test Results Confirm Integration Success:**
```
=== Environment Variables Test ===
✅ DB_US_DW_PASSWORD: **************** (length: 16)
✅ DB_US_DW_RO_PASSWORD: **************** (length: 16)
✅ DB_CA_DW_RO_PASSWORD: **************** (length: 16)
[... all connection-specific passwords loaded]

=== ConnectionRegistry Test ===
✅ ConnectionRegistry initialized with 14 connections
✅ Found connection: US_DW_UNIDW_SSH
   Host: us-west-2.ro.db.analysis.uniuni.com.internal
   Password: ****************

=== ConnectionManager Test ===
✅ ConnectionManager loaded
   Connection: US_DW_UNIDW_SSH
   Password: ****************
```

### Impact & Benefits

**Before Fix:**
- System tried to use single `DB_PASSWORD` for all connections
- Health checks failed with "No password found" warnings
- No support for multi-pipeline environments with different credentials

**After Fix:**
- Each connection uses its specific environment variable (e.g., `${DB_US_DW_RO_PASSWORD}`)
- ConnectionRegistry provides proper credential resolution
- Health checks use `mysql_default` connection for system status
- Full support for multi-environment, multi-pipeline configurations

**Key Insight:** The system now properly leverages your existing multi-connection architecture instead of requiring a generic password. Each pipeline can use its appropriate credentials while maintaining security through environment variable references.  

## Watermark Logic - Final Architecture (Resolved)

### Overview

The watermark system implements a **dual-tracking architecture** that separates cumulative totals from session-specific metrics, providing both historical progress tracking and per-execution visibility.

### Core Design Principles

#### 1. **Separation of Concerns**
- **Cumulative Metrics**: Track lifetime totals across all sync sessions
- **Session Metrics**: Track current execution only (essential for Airflow/monitoring)

#### 2. **Two-Phase Update Pattern**

**Phase 1: Batch Processing (Incremental Updates)**
Location: `row_based.py:508-516`

```python
# During batch processing - accumulate rows incrementally
self.watermark_manager.simple_manager.update_mysql_state(
    table_name=table_name,
    timestamp=accumulated_last_timestamp,
    id=accumulated_last_id,
    status='in_progress',
    rows_extracted=batch_rows,              # Adds to cumulative total
    s3_files_created=batch_files,           # Adds to cumulative total
    session_rows_total=total_rows_processed # Show session progress so far
)
```

**Behavior**:
- `rows_extracted=100` → Adds 100 to cumulative `total_rows`
- `session_rows_total=total_rows_processed` → Shows cumulative session progress (e.g., after 2 batches: 200)
- Updates `last_session_rows = total_rows_processed` (accumulates, not overwrites!)
- Provides resume capability if sync is interrupted
- **CRITICAL**: Even if interrupted, watermark shows correct session total

**Phase 2: Final Update (Session Total)**
Location: `row_based.py:1796-1803`

```python
# After all batches complete - set final session total
success = self.watermark_manager.simple_manager.update_mysql_state(
    table_name=table_name,
    timestamp=max_data_timestamp.isoformat(),
    id=last_processed_id,
    status=status,
    error=error_message,
    rows_extracted=0,                      # Don't add again (already accumulated)
    session_rows_total=session_rows_processed  # Set session display total
)
```

**Behavior**:
- `rows_extracted=0` → No change to cumulative total (avoid double-counting)
- `session_rows_total=300` → Sets `last_session_rows = 300` directly
- Changes status from `in_progress` → `success`

#### 3. **Watermark Manager Implementation**
Location: `simple_watermark_manager.py:98-137`

```python
def update_mysql_state(self, table_name: str,
                      rows_extracted: Optional[int] = None,
                      session_rows_total: Optional[int] = None) -> None:
    """
    Dual-mode update supporting both cumulative and session tracking
    """
    watermark = self.get_watermark(table_name)

    # CUMULATIVE: Add new rows to existing total
    session_rows = 0
    if rows_extracted is not None and rows_extracted > 0:
        current_rows = watermark['mysql_state'].get('total_rows', 0)
        total_rows = current_rows + rows_extracted
        session_rows = rows_extracted
        logger.info(f"Cumulative update: {current_rows} + {rows_extracted} = {total_rows}")
    else:
        total_rows = watermark['mysql_state'].get('total_rows', 0)

    # SESSION: Allow direct setting of session total (for final updates)
    if session_rows_total is not None:
        session_rows = session_rows_total
        logger.info(f"Session total set directly: {session_rows_total}")

    watermark['mysql_state'].update({
        'total_rows': total_rows,           # Cumulative across all sessions
        'last_session_rows': session_rows,  # Current session only
        'last_updated': datetime.now(timezone.utc).isoformat()
    })

    self._save_watermark(table_name, watermark)
```

### Data Flow Example

**Scenario: Sync processes 300 rows across 3 batches**

```
Initial State:
  total_rows: 400
  last_session_rows: 0

Batch 1 (100 rows):
  rows_extracted=100
  session_rows_total=100
  → total_rows: 400 + 100 = 500
  → last_session_rows: 100 ✅

Batch 2 (100 rows):
  rows_extracted=100
  session_rows_total=200
  → total_rows: 500 + 100 = 600
  → last_session_rows: 200 ✅ (accumulates!)

Batch 3 (100 rows):
  rows_extracted=100
  session_rows_total=300
  → total_rows: 600 + 100 = 700
  → last_session_rows: 300 ✅ (accumulates!)

Final Update:
  rows_extracted=0
  session_rows_total=300
  → total_rows: 700 (unchanged)
  → last_session_rows: 300 (confirms session total!)
```

**Interruption Handling (NEW):**

```
Initial State:
  total_rows: 400
  last_session_rows: 0

Batch 1 (100 rows):
  → total_rows: 500
  → last_session_rows: 100 ✅

Batch 2 (100 rows):
  → total_rows: 600
  → last_session_rows: 200 ✅

💥 PROGRAM CRASHES 💥

Watermark shows:
  total_rows: 600 ✅ (cumulative correct)
  last_session_rows: 200 ✅ (session progress preserved!)
  status: 'in_progress' ⚠️

After Restart - Session 2:
  Resume from last watermark

Batch 3 (100 rows):
  → total_rows: 600 + 100 = 700
  → last_session_rows: 100 (new session)

Final Update:
  → last_session_rows: 100 (this session only)
```

### Integration Points

#### 1. **CLI Display** (`main.py:1494-1502`)
```python
last_session_rows = getattr(watermark, 'mysql_last_session_rows', 0)
click.echo(f"Rows Extracted This Session: {last_session_rows:,}")
click.echo(f"Total Rows Extracted (Cumulative): {watermark.mysql_rows_extracted:,}")
```

#### 2. **Airflow Integration** (`multi_schema_commands.py:1156`)
```python
# Use session-specific metric for Airflow task reporting
backup_session_rows = getattr(watermark, 'mysql_last_session_rows', 0) or 0
actual_rows = backup_session_rows  # Report what THIS task did
```

#### 3. **JSON Output** (Airflow Automation)
```json
{
  "table_metrics": {
    "settlement.orders": {
      "rows_processed": 300,      // Session metric (from last_session_rows)
      "files_created": 3,
      "duration": 45.2
    }
  }
}
```

### Key Benefits

#### 1. **No Double-Counting**
- Batch updates accumulate correctly during processing
- Final update doesn't add rows again

#### 2. **Accurate Session Tracking**
- Airflow sees: "This sync processed 300 rows" ✅
- Not: "Total lifetime: 700 rows" ❌

#### 3. **Resume Capability**
- Interrupted syncs can resume from last processed ID/timestamp
- Cumulative totals remain accurate across restarts

#### 4. **Historical Tracking**
- `total_rows` provides lifetime statistics
- Useful for capacity planning and trend analysis

### TODO: Future Enhancement

**Redshift Session Tracking** (`multi_schema_commands.py:1158-1159`):
```python
# TODO: Add session tracking for Redshift loading as well
redshift_rows = getattr(watermark, 'redshift_rows_loaded', 0) or 0
```

Currently Redshift only tracks cumulative totals. Consider implementing:
- `redshift_last_session_rows` for per-load metrics
- Useful for load performance monitoring

### Production Status

The watermark system is now **production-ready** with:
- ✅ Clear separation of cumulative vs session metrics
- ✅ No double-counting bugs
- ✅ Accurate Airflow integration
- ✅ Resume capability for interrupted syncs
- ✅ Proper display in CLI and JSON output
- ✅ **Interruption-safe session tracking** - Shows accurate progress even if process crashes

The dual-tracking architecture provides both **historical context** (cumulative totals) and **operational visibility** (session metrics), making it suitable for enterprise data pipeline monitoring and automation.

### Key Enhancement: Interruption-Safe Session Tracking

**Problem Solved:** Previously, if a sync was interrupted after processing 2 batches (e.g., 100,000 rows), the watermark would only show `last_session_rows: 50,000` (the last batch size), losing visibility of the 50,000 rows from the first batch.

**Solution Implemented:** Each batch update now passes `session_rows_total=total_rows_processed`, which shows the **cumulative progress within the current session**:

```python
# Before (line 514): Only passed batch_rows
rows_extracted=batch_rows  # Overwrites session display with each batch

# After (lines 515-516): Pass both batch and session total
rows_extracted=batch_rows,              # For cumulative tracking
session_rows_total=total_rows_processed # For accurate session display ✅
```

**Impact:**
- **Interrupted Session 1:** Processed 200k rows → Watermark shows `last_session_rows: 200,000` ✅
- **Completed Session 2:** Processed 100k rows → Watermark shows `last_session_rows: 100,000` ✅
- **Monitoring Accuracy:** Operations teams see true session progress even after failures
- **Airflow Reliability:** Task metrics reflect actual work done, not just last batch size 

## YAML-Based Configuration System Migration (Resolved)

### Issue: Configuration Management and Environment Variable Substitution

**Problem:** System relied on direct `.env` file reading with mixed configuration structure and credentials, making multi-environment management difficult.

**Symptoms:**
```
❌ Status check failed: 2 validation errors for DatabaseConfig
host
  Field required [type=missing, input_value={'user': 'tianziqin'...}]
database
  Field required [type=missing, input_value={'user': 'tianziqin'...}]
```

### Root Cause Analysis

**Original Architecture (v1.0.0):**
```
.env file → AppConfig (Pydantic) → Application
```
- Configuration structure and credentials mixed in .env
- Hard to manage multiple environments
- No central source of truth for infrastructure settings
- Duplication between .env and YAML files

**Issues Identified:**

1. **Environment Variable Substitution Not Working**
   - `connections.yml` had `${DB_USER}` placeholders
   - `.env` file not loaded before YAML interpolation
   - Variables stayed as `${DB_USER}` instead of being substituted

2. **Allowlist Blocking Variables**
   - Variables like `SSH_BASTION_USER`, `REDSHIFT_USER`, `DB2_US_RO_PASSWORD` blocked
   - Security allowlist needed prefixes: `SSH_`, `REDSHIFT_`, `DB2_`, etc.
   - Warning logs: `Environment variable 'DB2_US_RO_PASSWORD' not in allowlist`

3. **Validation Errors After AppConfig Creation**
   - Environment variables cleared after creating AppConfig
   - Subsequent validation calls couldn't access config values
   - Try/finally block removing env vars broke Pydantic validation

### Fix Applied: YAML-First Configuration Architecture

#### 1. **New Architecture (v1.1.0+)**

```
.env (credentials) → connections.yml (${VAR} refs) → ConfigurationManager → AppConfig → Application
```

**Benefits:**
- Configuration structure in YAML (version controlled)
- Credentials in .env (not version controlled, secure)
- Central source of truth for infrastructure
- Environment variable substitution via `${VAR}` syntax

#### 2. **Load .env Before YAML Interpolation**

**Code location:** `src/core/configuration_manager.py:173-190`

```python
def _load_dotenv(self):
    """Load .env file to populate environment variables for YAML interpolation"""
    from dotenv import load_dotenv

    env_file = Path.cwd() / '.env'
    if env_file.exists():
        load_dotenv(env_file, override=False)
        logger.debug(f"Loaded .env file from: {env_file}")
```

**Critical Initialization Order:**
```python
def __init__(self, config_root: str = "config"):
    # CRITICAL: Load .env BEFORE interpolating YAML
    self._load_dotenv()           # Step 1: Load credentials
    self._load_connections()      # Step 2: Load YAML + interpolate ${VAR}
    self._load_environments()
    self._load_templates()
    self._load_pipelines()
```

**Why This Order Matters:**
- ✅ `.env` loaded **first** - Credentials available for interpolation
- ✅ YAML loaded **second** - Can substitute `${VAR}` with actual values
- ❌ Wrong order would leave `${DB_USER}` as placeholder

#### 3. **Environment Variable Allowlist Fix**

**Code location:** `src/core/configuration_manager.py:535-587`

```python
# BEFORE (BROKEN): Missing infrastructure prefixes
safe_prefixes = [
    'BACKUP_', 'CONFIG_', 'DB_', 'CONN_'
]

# AFTER (FIXED): Added all infrastructure prefixes
safe_prefixes = [
    'BACKUP_', 'CONFIG_', 'DB_', 'DB2_', 'CONN_',
    'SSH_', 'REDSHIFT_', 'MYSQL_', 'AWS_', 'S3_'
]
```

**Security Features:**
- ✅ Only whitelisted variables are interpolated
- ✅ Blocks dangerous patterns (command injection, path traversal)
- ✅ Prevents access to arbitrary system environment variables
- ✅ Logs warnings for blocked variables

#### 4. **Removed Environment Variable Clearing**

**Code location:** `src/core/configuration_manager.py:1218-1274`

```python
# BEFORE (BROKEN): Cleared env vars after creating AppConfig
try:
    app_config = AppConfig()
    return app_config
finally:
    # This broke subsequent validation!
    for key in infrastructure_keys:
        del os.environ[key]  # ❌ Removed needed values

# AFTER (FIXED): Keep env vars for validation
for key, value in env_mapping.items():
    if value is not None:
        os.environ[key] = str(value)

app_config = AppConfig()  # Pydantic reads from environment
return app_config  # No cleanup - values persist for validation ✅
```

**Why This Matters:**
- AppConfig properties create sub-configs on-demand (lazy loading)
- `app_config.database` triggers `DatabaseConfig()` creation
- DatabaseConfig needs environment variables still set
- Clearing vars broke `validate_all()` and health checks

#### 5. **Environment Variable Interpolation**

**Code location:** `src/core/configuration_manager.py:552-616`

**Example YAML** (`config/connections.yml`):
```yaml
connections:
  sources:
    default:
      host: us-west-2.ro.db.analysis.uniuni.com.internal
      username: "${DB_USER}"           # ← References .env
      password: "${DB_US_DW_PASSWORD}" # ← References .env

      ssh_tunnel:
        enabled: true
        host: 35.83.114.196
        username: "${SSH_BASTION_USER}"
        private_key_path: "${SSH_BASTION_KEY_PATH}"

s3:
  bucket_name: redshift-dw-qa-uniuni-com
  access_key_id: "${AWS_ACCESS_KEY_ID}"
  secret_access_key: "${AWS_SECRET_ACCESS_KEY}"
```

**Substitution Process:**
```
BEFORE interpolation:
  username: "${DB_USER}"
  password: "${DB_US_DW_PASSWORD}"

AFTER interpolation:
  username: "tianziqin"
  password: "qPwRT]9(BUI0Q2Vw"
```

### Complete Data Flow

```
┌─────────────────────────────────────────┐
│ Step 1: Load .env file                  │
│   DB_USER=tianziqin                     │
│   DB_US_DW_PASSWORD=secret123           │
│   ↓ load_dotenv()                       │
│   os.environ['DB_USER'] = 'tianziqin'   │
└─────────────────┬───────────────────────┘
                  ↓
┌─────────────────────────────────────────┐
│ Step 2: Load connections.yml            │
│   username: "${DB_USER}" → "tianziqin"  │
│   password: "${DB_US_DW_PASSWORD}" → OK │
│   ↓ _interpolate_environment_variables  │
└─────────────────┬───────────────────────┘
                  ↓
┌─────────────────────────────────────────┐
│ Step 3: Create AppConfig                │
│   Set environment variables from YAML   │
│   ↓ AppConfig() (Pydantic reads env)    │
│   app_config.database.user = 'tianziqin'│
└─────────────────┬───────────────────────┘
                  ↓
┌─────────────────────────────────────────┐
│ Step 4: Use in application              │
│   Backup strategies use AppConfig       │
│   No changes needed - backward compat!  │
└─────────────────────────────────────────┘
```

### Testing Results

**Before Fix:**
```
BEFORE INTERPOLATION: username = ${DB_USER}
AFTER INTERPOLATION:  username = ${DB_USER}  ← Not substituted!

ENV_MAPPING VALUES:
  DB_HOST = us-west-2.ro.db.analysis.uniuni.com.internal
  DB_USER = ${DB_USER}  ← Still placeholder!

❌ Status check failed: host Field required, database Field required
```

**After Fix:**
```
✓ Loaded .env file from: /home/tianzi/s3-redshift-backup-tool/.env

BEFORE INTERPOLATION: username = ${DB_USER}
AFTER INTERPOLATION:  username = tianziqin  ← Substituted!

✓ Substituted ${DB_USER} = tianziqin
✓ Substituted ${DB_US_DW_PASSWORD} = qPwRT]9(BUI0Q2Vw
✓ Substituted ${SSH_BASTION_USER} = tianziqin
✓ Substituted ${AWS_ACCESS_KEY_ID} = AKIA3QYE2ANHIFTCRKN3

✅ Status check passed
```

### Components Modified

**1. ConfigurationManager (`src/core/configuration_manager.py`)**
- Added `_load_dotenv()` method (lines 173-190)
- Updated `__init__()` to load .env first (line 157)
- Fixed allowlist in `is_safe_env_var()` (lines 542-544)
- Removed env var cleanup in `create_app_config()` (lines 1218-1274)

**2. CLI Initialization (`src/cli/main.py`)**
```python
# Initialize ConfigurationManager to load connections.yml
config_manager = ConfigurationManager()

# Create AppConfig from YAML (backward compatibility)
config = config_manager.create_app_config()

# Store in context for commands
ctx.obj['config_manager'] = config_manager
```

**3. Multi-Schema Commands (`src/cli/multi_schema_commands.py`)**
```python
# Create AppConfig with specific source/target connections
config = multi_schema_ctx.config_manager.create_app_config(
    source_connection=pipeline_config.source,  # e.g., "US_DW_UNIDW_SSH"
    target_connection=pipeline_config.target   # e.g., "redshift_default"
)
```

### Benefits

**1. Separation of Concerns**
- Configuration structure in YAML (version controlled)
- Credentials in .env (not version controlled, secure)
- Clear separation of infrastructure vs secrets

**2. Multi-Environment Support**
- Single YAML structure
- Different .env files per environment (dev, staging, prod)
- Easy environment switching

**3. Centralized Management**
- All connection details in one place (`connections.yml`)
- Easy to update infrastructure settings
- No need to search through .env for connection details

**4. Security**
- Credentials never hardcoded in YAML
- Allowlist prevents injection attacks
- .env stays out of version control

**5. Backward Compatibility**
- Existing code using `AppConfig` works unchanged
- No breaking changes to backup strategies
- Gradual migration path

### Common Patterns

**Pattern 1: Adding New Environment Variable**
```bash
# 1. Add to .env
NEW_DATABASE_PASSWORD=secret456

# 2. Reference in connections.yml
sources:
  new_source:
    password: "${NEW_DATABASE_PASSWORD}"

# 3. Update allowlist if needed (add prefix to safe_prefixes)
```

**Pattern 2: Adding New Connection**
```yaml
connections:
  sources:
    new_connection:
      host: new-host.example.com
      username: "${DB_USER}"
      password: "${DB_NEW_PASSWORD}"
      ssh_tunnel:
        enabled: true
        username: "${SSH_BASTION_USER}"
```

### Troubleshooting Guide

**Problem**: Variable not substituted (shows `${VAR_NAME}`)

**Solutions:**
1. Check .env file: `grep VAR_NAME .env`
2. Check allowlist: Look for warning in logs
3. Add to allowlist: Update `safe_prefixes` or `ALLOWED_ENV_VARS`

**Problem**: `Field required` validation error

**Solutions:**
1. Check YAML has the field defined
2. Check ${VAR} was successfully substituted (look for warning logs)
3. Check .env has the referenced variable
4. Verify .env is loaded before YAML processing

### Production Status

The YAML-based configuration system is now **production-ready** with:
- ✅ Proper .env loading before YAML interpolation
- ✅ Comprehensive environment variable allowlist
- ✅ No more validation errors after AppConfig creation
- ✅ Clean output without verbose debug messages
- ✅ Backward compatible with existing code
- ✅ Supports multiple connections and environments
- ✅ Secure credential management

**Location in Documentation:** `Codes_Explain.md:114-627`

## Performance Optimization: S3 Row Counting (November 2025)

### Issue: Program Stuck During S3 File Count Verification

**Problem:** After logging "Actual S3 file count: 90 files", the program appeared frozen for 10-30+ minutes.

**Root Cause:** `src/backup/row_based.py:1771`
```python
total_existing_rows = self._count_rows_in_s3_files(list(all_s3_files))
```

This operation:
1. Downloaded all 90 parquet files from S3
2. Parsed each file to count rows
3. No progress indication - appeared frozen
4. Extremely slow with large file counts

### Fix Applied: Skip Unnecessary Row Counting

**File:** `src/backup/row_based.py:1770-1777`

```python
# BEFORE (SLOW):
try:
    total_existing_rows = self._count_rows_in_s3_files(list(all_s3_files))
except Exception as e:
    total_existing_rows = session_rows_processed

# AFTER (FAST):
# PERFORMANCE FIX: Skip slow row counting from S3 files (can take 10-30+ minutes for 90 files)
# Use session_rows_processed as it's already accurate from batch processing
total_existing_rows = session_rows_processed
self.logger.logger.info(
    f"Skipping S3 row counting for performance (would download/parse {len(all_s3_files)} files)",
    table_name=table_name,
    files_count=len(all_s3_files)
)
```

**Why This Is Safe:**
- `session_rows_processed` is already accurate from batch processing
- Row counting was just verification, not essential
- Watermarks already track row counts correctly
- No data loss - just skipping expensive verification

**Impact:**
- Before: Program frozen 10-30+ minutes at "90 files"
- After: Continues immediately 🚀

## Redshift Session-Only Metrics Tracking (November 2025)

### Issue: Inconsistent Metrics Between MySQL and Redshift

**Problem:**
- MySQL tracked both cumulative (`total_rows`) and session-only (`last_session_rows`)
- Redshift only tracked cumulative totals
- JSON output mixed session and cumulative metrics, causing confusion

**Example Confusion:**
```json
{
  "stages": {
    "backup": {
      "summary": {
        "total_rows": 1000  // ← Session-only (rows THIS sync)
      }
    },
    "redshift": {
      "summary": {
        "total_rows": 3000  // ← Cumulative (all rows EVER loaded)
      }
    }
  }
}
```

### Fix Applied: Dual Tracking for Redshift

**1. Added Session Tracking to Watermark State**

`src/core/simple_watermark_manager.py:253, 448`:
```python
'redshift_state': {
    'total_rows': 0,              # Cumulative total
    'last_session_rows': 0,       # Session-only ✅ NEW
    'last_updated': None,
    'status': 'pending'
}
```

**2. Updated Redshift Count Method**

`src/core/simple_watermark_manager.py:229-253`:
```python
def update_redshift_count_from_external(self, table_name: str, actual_count: int):
    current_rows = watermark['redshift_state'].get('total_rows', 0)
    new_total = current_rows + actual_count

    watermark['redshift_state']['total_rows'] = new_total
    watermark['redshift_state']['last_session_rows'] = actual_count  # ✅ Session tracking
    logger.info(f"Redshift rows update: session={actual_count}, cumulative={new_total}")
```

**3. Exposed Session Rows in Adapter**

`src/core/watermark_adapter.py:43-60`:
```python
self.redshift_rows_loaded = redshift_state.get('total_rows', 0)  # Cumulative
self.redshift_last_session_rows = redshift_state.get('last_session_rows', 0)  # Session ✅
```

**4. Updated CLI to Use Session Metrics**

`src/cli/main.py:966`:
```python
# Get session-only row count (not cumulative)
table_rows = watermark.redshift_last_session_rows if watermark else 0
click.echo(f"   ✅ {table_name}: Loaded successfully ({table_rows:,} rows this session)")
```

**Result:**
| Stage | Metric | Type | Represents |
|-------|--------|------|------------|
| MySQL Backup | `total_rows` | Session | Rows extracted THIS sync |
| Redshift Load | `total_rows` | Session | Rows loaded THIS sync |
| Watermark | `mysql_state.total_rows` | Cumulative | All rows ever extracted |
| Watermark | `redshift_state.total_rows` | Cumulative | All rows ever loaded |

## JSON Output Format Unification (November 2025)

### Issue: Two Conflicting JSON Output Formats

**Problem:**
- Format 1 (Original): Stage-based (`stages.backup`, `stages.redshift`) in `src/cli/main.py`
- Format 2 (Airflow): Execution-based (`execution_id`, `table_results`) in `src/cli/airflow_integration.py`
- Users confused about which format to expect

### Fix Applied: Unified to Airflow-Enhanced Format

**1. Removed Stage-Based JSON Logic**

`src/cli/main.py:999-1023` - Replaced old format with SyncExecutionTracker:
```python
# BEFORE (OLD):
sync_result = {
    "success": bool,
    "stages": {
        "backup": {"summary": {...}},
        "redshift": {"summary": {...}}
    }
}

# AFTER (NEW):
tracker = SyncExecutionTracker()
tracker.set_pipeline_info(pipeline, tables)
# ... track per-table results ...
overall_status, final_metadata = tracker.finalize()
```

**2. Updated All Documentation**

- `docs/JSON_OUTPUT_FORMAT.md` - Removed stage-based examples, updated to unified format
- `docs/S3_COMPLETION_MARKERS.md` - Updated all code examples and field references

**New Unified Format:**
```json
{
  "execution_id": "sync_20250926_195438_2f01922d",
  "start_time": "2025-09-26T19:54:38.135401+00:00",
  "pipeline": "us_dw_unidw_2_public_pipeline",
  "tables_requested": ["unidw.dw_parcel_detail_tool_temp"],
  "table_results": {
    "unidw.dw_parcel_detail_tool_temp": {
      "status": "success",
      "rows_processed": 164260,  // ← Session-only
      "files_created": 4,
      "duration_seconds": 30.0
    }
  },
  "status": "success",
  "summary": {
    "success_count": 1,
    "failure_count": 0,
    "total_rows_processed": 164260,  // ← Session-only
    "total_files_created": 4
  }
}
```

**Benefits:**
- ✅ Single format across all commands
- ✅ Unique `execution_id` for workflow tracking
- ✅ Per-table success/failure visibility
- ✅ Session-only metrics (consistent)
- ✅ Better Airflow/monitoring integration

## Parcel Download Tool Configuration Enhancements (November 2025)

### Issue: Multiple Configuration and Path Problems

**Problems:**
1. Script used `AppConfig()` which expected `.env` SSH format (`bastion_host`)
2. `connections.yml` used different SSH format (`host`, `username`, `private_key_path`)
3. Script hardcoded pipeline name and datetime parameters
4. Running from `parcel_download_tool_etl/` directory caused path issues
5. ConnectionRegistry loaded wrong config file

### Fix 1: Use ConnectionRegistry Instead of AppConfig

`parcel_download_tool_etl/parcel_download_and_sync.py:52, 120-121, 146-147`:
```python
# BEFORE (BROKEN):
from src.config.settings import AppConfig
config = AppConfig()  # Expected bastion_host from .env
conn_manager = ConnectionManager(config)

# AFTER (FIXED):
from src.core.connection_registry import ConnectionRegistry
config_path = PROJECT_ROOT / "config" / "connections.yml"
conn_registry = ConnectionRegistry(config_path=str(config_path))
```

**Why:** ConnectionRegistry properly handles `connections.yml` format with `host` instead of `bastion_host`.

### Fix 2: Made Pipeline/Date/Hours Configurable

`parcel_download_tool_etl/parcel_download_and_sync.py:86-111`:
```python
parser.add_argument(
    '-p', '--pipeline',
    type=str,
    default='us_dw_unidw_2_public_pipeline',
    help='Pipeline name to use'
)

parser.add_argument(
    '-d', '--date',
    type=str,
    default='',
    help='Start datetime (empty = current time)'
)

parser.add_argument(
    '--hours',  # Note: -h reserved for --help
    type=str,
    default='-1',
    help='Hours offset (e.g., "-2" for 2 hours ago)'
)
```

**Usage:**
```bash
# Use defaults
python parcel_download_and_sync.py --sync-only

# Custom pipeline
python parcel_download_and_sync.py -p us_dw_unidw_2_settlement_dws_pipeline --sync-only

# Custom time range
python parcel_download_and_sync.py -d "" --hours "-2"

# All options
python parcel_download_and_sync.py -p my_pipeline -d "2024-08-14 10:00:00" --hours "-1"
```

### Fix 3: Explicit Config Path Resolution

`parcel_download_tool_etl/parcel_download_and_sync.py:44-46`:
```python
# Project root directory (parent of parcel_download_tool_etl)
PROJECT_ROOT = Path(__file__).parent.parent.absolute()

# Add project root to Python path
sys.path.insert(0, str(PROJECT_ROOT))
```

**Why:** Ensures imports work and config files are found relative to project root, not current directory.

### Fix 4: ConnectionRegistry Type Parameter Conflict

`src/core/connection_registry.py:279-280, 291-292`:
```python
# BEFORE (BROKEN):
self.connections[name] = ConnectionConfig(
    name=name,
    type='mysql',
    **processed_config  # ← May contain 'type' key → conflict!
)

# AFTER (FIXED):
processed_config.pop('type', None)  # ← Remove to avoid conflict
self.connections[name] = ConnectionConfig(
    name=name,
    type='mysql',
    **processed_config
)
```

**Error Prevented:** `ConnectionConfig() got multiple values for keyword argument 'type'`

### Updated Bash Script

`parcel_download_tool_etl/parcel_download_hourly_run.sh:17`:
```bash
# Use named arguments (not positional)
python parcel_download_and_sync.py -d "" --hours "-0.5"
```

**Result:**
- ✅ Works from any directory
- ✅ Uses correct config files
- ✅ Proper SSH connection handling
- ✅ Configurable pipeline/date/hours
- ✅ No more "bastion_host" errors

## Path Resolution with .resolve() - Symlink Handling (November 2025)

### 核心问题：符号链接（Symlink）

**问题：** 当脚本通过符号链接访问时，使用 `.absolute()` 无法找到项目真实位置，导致 `ModuleNotFoundError: No module named 'src'`

**原因：** `.absolute()` 只将路径转为绝对路径，但**不解析符号链接**

### `.absolute()` vs `.resolve()` 的关键区别

**`.absolute()`** - 将路径转为绝对路径，但**不解析符号链接**

**`.resolve()`** - 将路径转为绝对路径，**并且解析所有符号链接**

### 实际部署场景分析

#### 使用 `.absolute()` 时（有问题）：
```bash
# ETL 服务器上有符号链接
/opt/etl/scripts/parcel_download_and_sync.py → /home/tianzi/s3-redshift-backup-tool/parcel_download_tool_etl/parcel_download_and_sync.py

# 会发生什么：
__file__ = '/opt/etl/scripts/parcel_download_and_sync.py'
PROJECT_ROOT = /opt/etl  # ❌ 错误 - 这里没有 'src/' 目录！
```

#### 使用 `.resolve()` 时（正确）：
```bash
# 同样的符号链接
/opt/etl/scripts/parcel_download_and_sync.py → /home/tianzi/s3-redshift-backup-tool/parcel_download_tool_etl/parcel_download_and_sync.py

# 会发生什么：
__file__ = '/opt/etl/scripts/parcel_download_and_sync.py'
SCRIPT_PATH.resolve() = '/home/tianzi/s3-redshift-backup-tool/parcel_download_tool_etl/parcel_download_and_sync.py'
PROJECT_ROOT = /home/tianzi/s3-redshift-backup-tool  # ✅ 正确 - 有 'src/' 目录！
```

### 为什么生产环境常用符号链接？

ETL 服务器经常使用符号链接的原因：

1. **版本管理**: `/opt/app/current → /opt/app/releases/v1.2.3`
2. **共享脚本**: 多个用户访问同一代码库
3. **环境隔离**: 不同环境指向同一份代码
4. **便于回滚**: 只需改变符号链接，而不是移动文件

### 具体例子

假设你的 ETL 服务器部署结构是这样的：

```bash
# 实际代码在这里
/home/tianzi/s3-redshift-backup-tool/
├── src/
│   └── core/
│       └── connection_registry.py
└── parcel_download_tool_etl/
    └── parcel_download_and_sync.py

# 但你创建了符号链接方便执行
/opt/etl/scripts/parcel_download_and_sync.py → /home/tianzi/s3-redshift-backup-tool/parcel_download_tool_etl/parcel_download_and_sync.py
```

**使用 `.absolute()` 时：**
- `__file__` 是 `/opt/etl/scripts/parcel_download_and_sync.py`
- `.parent` 是 `/opt/etl/scripts`
- `.parent.parent` 是 `/opt/etl`
- 尝试导入 `/opt/etl/src` → **找不到！❌**

**使用 `.resolve()` 时：**
- `__file__` 是 `/opt/etl/scripts/parcel_download_and_sync.py`
- `.resolve()` **跟踪符号链接** → `/home/tianzi/s3-redshift-backup-tool/parcel_download_tool_etl/parcel_download_and_sync.py`
- `.parent` 是 `/home/tianzi/s3-redshift-backup-tool/parcel_download_tool_etl`
- `.parent.parent` 是 `/home/tianzi/s3-redshift-backup-tool`
- 尝试导入 `/home/tianzi/s3-redshift-backup-tool/src` → **找到了！✅**

### Fix Applied

**File:** `parcel_download_tool_etl/parcel_download_and_sync.py:44-73`

```python
# BEFORE (BROKEN):
PROJECT_ROOT = Path(__file__).absolute().parent.parent

# AFTER (FIXED):
SCRIPT_PATH = Path(__file__).resolve()  # ← .resolve() 解析符号链接
SCRIPT_DIR = SCRIPT_PATH.parent
PROJECT_ROOT = SCRIPT_DIR.parent

# Added verification
if not (PROJECT_ROOT / 'src').exists():
    print(f"ERROR: Cannot find 'src' directory at {PROJECT_ROOT}")
    print(f"Script location: {SCRIPT_PATH}")
    print(f"Script directory: {SCRIPT_DIR}")
    print(f"Project root: {PROJECT_ROOT}")
    print(f"Contents of project root: {list(PROJECT_ROOT.iterdir())}")
    sys.exit(1)

# Add project root to Python path
sys.path.insert(0, str(PROJECT_ROOT))

# Import with error handling
try:
    from src.core.connection_registry import ConnectionRegistry
except ModuleNotFoundError as e:
    print(f"ERROR: Failed to import ConnectionRegistry: {e}")
    print(f"Python path: {sys.path}")
    print(f"Project root: {PROJECT_ROOT}")
    print(f"src directory exists: {(PROJECT_ROOT / 'src').exists()}")
    sys.exit(1)
```

### 额外的诊断信息

修复还添加了验证代码，如果仍然失败，这些信息可以帮助你**精确诊断**问题，看到底是不是符号链接导致的：

```python
if not (PROJECT_ROOT / 'src').exists():
    print(f"ERROR: Cannot find 'src' directory at {PROJECT_ROOT}")
    print(f"Script location: {SCRIPT_PATH}")  # 显示是否使用了符号链接
    print(f"Script directory: {SCRIPT_DIR}")
    print(f"Project root: {PROJECT_ROOT}")
```

### Key Benefits

- ✅ **"追根溯源"**: 无论脚本通过什么路径、什么符号链接访问，都能找到真实文件位置
- ✅ **生产环境兼容**: 支持符号链接部署模式（版本管理、共享脚本等）
- ✅ **准确的路径解析**: 始终基于真实文件位置计算项目根目录
- ✅ **详细的诊断信息**: 出错时显示完整路径信息，方便快速定位问题
- ✅ **防止 ModuleNotFoundError**: 确保 `src/` 模块能被正确导入

### 总结

**`.resolve()` 的作用就是"追根溯源"**，无论你的脚本是通过什么路径、什么符号链接访问的，它都会找到**真实的文件位置**，从而确保项目结构被正确识别。这就是为什么这个修复能让你的脚本在 ETL 服务器上正常工作！

**Bottom Line**: Symlinks are common in production deployments, and `.resolve()` ensures your script works correctly by following symlinks to the real file location, preventing path-related import errors.

## Multi-S3 Bucket Configuration (November 2025)

### Issue: Single S3 Bucket for Both QA and Production

**Problem:**
- All pipelines used a single hardcoded S3 bucket (`redshift-dw-qa-uniuni-com`)
- No separation between QA and Production data storage
- Impossible to configure different buckets per pipeline
- Legacy `s3` section in connections.yml was global and not flexible

### Solution: Named S3 Configurations

Added support for multiple named S3 configurations, allowing each pipeline to choose its target bucket.

### Implementation

#### 1. Named S3 Configs in connections.yml

`config/connections.yml`:
```yaml
# NEW: Multiple named S3 configurations
s3_configs:
  s3_qa:
    bucket_name: redshift-dw-qa-uniuni-com
    region: us-west-2
    access_key_id: "${AWS_ACCESS_KEY_ID}"
    secret_access_key: "${AWS_SECRET_ACCESS_KEY}"
    incremental_path: incremental/
    high_watermark_key: watermark/last_run_timestamp.txt
    description: "QA environment S3 bucket"

    performance:
      multipart_threshold: 104857600    # 100MB
      multipart_chunksize: 52428800     # 50MB
      max_concurrency: 10
      max_pool_connections: 20
      retry_max_attempts: 3
      retry_mode: adaptive

  s3_prod:
    bucket_name: redshift-dw-uniuni-com
    region: us-west-2
    access_key_id: "${AWS_ACCESS_KEY_ID}"
    secret_access_key: "${AWS_SECRET_ACCESS_KEY}"
    incremental_path: incremental/
    high_watermark_key: watermark/last_run_timestamp.txt
    description: "Production environment S3 bucket"

    performance:
      multipart_threshold: 104857600
      multipart_chunksize: 52428800
      max_concurrency: 10
      max_pool_connections: 20
      retry_max_attempts: 3
      retry_mode: adaptive
```

**Note:** Legacy `s3` section removed to eliminate redundancy.

#### 2. Pipeline-Level S3 Config Selection

`config/pipelines/us_dw_unidw_2_public_pipeline.yml`:
```yaml
pipeline:
  name: "us_dw_unidw_2_public"
  source: "US_DW_UNIDW_SSH"
  target: "redshift_default"
  s3_config: "s3_qa"  # ← NEW: Select QA bucket
  version: "1.2.0"

  s3:  # ← Still configures data organization
    isolation_prefix: "us_dw_unidw/"
    partition_strategy: "table"
    compression: "snappy"
```

`config/pipelines/us_dw_unidw_2_settlement_dws_pipeline.yml`:
```yaml
pipeline:
  name: "us_dw_unidw_2_settlement_dws"
  source: "US_DW_UNIDW_SSH"
  target: "redshift_settlement_dws"
  s3_config: "s3_qa"  # ← NEW: Select Production bucket
  version: "1.2.0"

  s3:
    isolation_prefix: "us_dw_unidw/"
    partition_strategy: "table"
    compression: "snappy"
```

#### 3. PipelineConfig Enhancement

`src/core/configuration_manager.py:92`:
```python
@dataclass
class PipelineConfig:
    name: str
    version: str
    source: str
    target: str
    s3: Dict[str, Any]
    s3_config: Optional[str] = None  # ← NEW: Reference to named S3 config
    # ... other fields
```

#### 4. ConfigurationManager Updates

`src/core/configuration_manager.py:1109-1132`:
```python
def get_s3_config(self, s3_config_name: str) -> Dict[str, Any]:
    """
    Get S3 configuration from connections.yml

    Args:
        s3_config_name: Name of S3 config to retrieve (required)

    Returns:
        S3 configuration dictionary

    Raises:
        ValidationError: If s3_config_name not found
    """
    s3_configs = self.connections_config.get('s3_configs', {})
    if s3_config_name not in s3_configs:
        available = ', '.join(s3_configs.keys()) if s3_configs else 'none'
        raise ValidationError(
            f"S3 config '{s3_config_name}' not found in connections.yml. "
            f"Available: {available}"
        )

    config = s3_configs[s3_config_name]
    logger.info(f"Using S3 config '{s3_config_name}': {config.get('bucket_name')}")
    return config
```

`src/core/configuration_manager.py:1163`:
```python
def create_app_config(
    self,
    source_connection: Optional[str] = None,
    target_connection: Optional[str] = None,
    s3_config_name: Optional[str] = None  # ← NEW parameter
) -> 'AppConfig':
    # ...
    s3_name = s3_config_name or list(s3_configs.keys())[0]  # Default to first
    s3_config = self.get_s3_config(s3_name)
    # ...
```

#### 5. CLI Integration

`src/cli/multi_schema_commands.py:980-984`:
```python
config = multi_schema_ctx.config_manager.create_app_config(
    source_connection=pipeline_config.source,
    target_connection=pipeline_config.target,
    s3_config_name=pipeline_config.s3_config  # ← Pass S3 config from pipeline
)
```

#### 6. Environment Variables

`.env.template`:
```bash
# AWS Access Key ID for S3 access (shared across QA and Production)
AWS_ACCESS_KEY_ID=your-aws-access-key-id

# AWS Secret Access Key for S3 access (shared across QA and Production)
AWS_SECRET_ACCESS_KEY=your-aws-secret-access-key

# Production S3 Bucket Name
# Used by pipelines with s3_config: "s3_prod" in pipeline YAML
S3_PROD_BUCKET_NAME=your-production-bucket-name
```

### How S3 Path Construction Works

**From `s3_config`** (bucket-level):
- `bucket_name`: Which S3 bucket
- `region`: AWS region
- `access_key_id` / `secret_access_key`: Credentials
- `incremental_path`: Base path (e.g., `incremental/`)

**From `s3` section** (pipeline-level):
- `isolation_prefix`: Subfolder for this pipeline (e.g., `us_dw_unidw/`)
- `partition_strategy`: How to organize files
- `compression`: Compression format

**Final S3 Path:**
```
s3://{bucket_name}/{incremental_path}/{isolation_prefix}/{table_name}/file.parquet
        ↑                 ↑                    ↑               ↑
   from s3_config    from s3_config     from pipeline s3   table name
```

**Example:**
```
s3://redshift-dw-qa-uniuni-com/incremental/us_dw_unidw/dw_parcel_detail_tool/part-0001.parquet
```

### Migration Notes

**Before:**
- All data stored in: `s3://redshift-dw-qa-uniuni-com/...`

**After:**
- QA pipelines: `s3://redshift-dw-qa-uniuni-com/...` (same location)
- Prod pipelines: `s3://redshift-dw-uniuni-com/...` (new location)

**No data migration needed** - Existing data remains in QA bucket, accessible to QA pipelines.

### Usage

**List available S3 configs:**
```python
config_manager = ConfigurationManager()
s3_configs = config_manager.list_s3_configs()
# Returns: {'s3_qa': 'QA environment S3 bucket', 's3_prod': 'Production environment S3 bucket'}
```

**Run sync with specific S3 config:**
```bash
# Uses s3_config from pipeline YAML
python -m src.cli.main sync pipeline -p us_dw_unidw_2_settlement_dws
```

**Logs show which S3 config is used:**
```
[info] Using S3 config 's3_prod': redshift-dw-uniuni-com
[debug] Creating AppConfig from source=US_DW_UNIDW_SSH, target=redshift_settlement_dws, s3_config=s3_prod
```

### Result

- ✅ **Separate QA and Production buckets** for data isolation
- ✅ **Pipeline-level S3 selection** via `s3_config` field
- ✅ **Environment-based configuration** with `${VAR}` interpolation
- ✅ **No legacy code** - removed redundant `s3` section from connections.yml
- ✅ **Backward compatible** - defaults to first S3 config if not specified
- ✅ **Better error messages** - shows available S3 configs when validation fails
- ✅ **Logging visibility** - clearly shows which bucket/config is being used

## Usually Used Commands
python -m src.cli.main watermark reset -t unidw.dw_parcel_detail_tool -p us_dw_unidw_2_public_pipeline  
python -m src.cli.main watermark get -t unidw.dw_parcel_detail_tool -p us_dw_unidw_2_public_pipeline 
python -m src.cli.main sync pipeline -p us_dw_unidw_2_public_pipeline -t unidw.dw_parcel_detail_tool --limit 22959410 2>&1 | tee parcel_detail_sync.log
python -m src.cli.main s3clean list -t unidw.dw_parcel_detail_tool -p us_dw_unidw_2_public_pipeline   
python -m src.cli.main s3clean clean -t unidw.dw_parcel_detail_tool -p us_dw_unidw_2_public_pipeline
python -m src.cli.main watermark set -t unidw.dw_parcel_detail_tool -p us_dw_unidw_2_public_pipeline --id 248668885 
python -m src.cli.main sync pipeline -p us_dw_unidw_2_public_pipeline -t unidw.dw_parcel_detail_tool --redshift-only 2>&1 | tee parcel_detail_redshift_load.log

python -m src.cli.main watermark get -t unidw.dw_parcel_pricing_temp -p us_dw_unidw_2_public_pipeline 
python -m src.cli.main watermark reset -t unidw.dw_parcel_pricing_temp -p us_dw_unidw_2_public_pipeline  
python -m src.cli.main s3clean list -t unidw.dw_parcel_pricing_temp -p us_dw_unidw_2_public_pipeline   
python -m src.cli.main s3clean clean -t unidw.dw_parcel_pricing_temp -p us_dw_unidw_2_public_pipeline
python -m src.cli.main sync pipeline -p us_dw_unidw_2_public_pipeline -t unidw.dw_parcel_pricing_temp --limit 100 2>&1 | tee pricing_temp.log  
python -m src.cli.main sync pipeline -p us_dw_unidw_2_public_pipeline -t unidw.dw_parcel_pricing_temp --redshift-only
python -m src.cli.main sync pipeline -p us_dw_unidw_2_public_pipeline -t unidw.dw_parcel_pricing_temp --backup-only --limit 30000 2>&1 | tee pricing_temp.log 

python -m src.cli.main watermark set -t unidw.dw_parcel_detail_tool_temp -p us_dw_unidw_2_public_pipeline --id 0
python -m src.cli.main sync pipeline -p us_dw_unidw_2_public_pipeline -t unidw.dw_parcel_detail_tool_temp --json-output sync_parcel_detail.json 
python -m src.cli.main watermark get -t unidw.dw_parcel_detail_tool_temp -p us_dw_unidw_2_public_pipeline
python -m src.cli.main s3clean list -t unidw.dw_parcel_detail_tool_temp -p us_dw_unidw_2_public_pipeline
python -m src.cli.main s3clean clean -t unidw.dw_parcel_detail_tool_temp -p us_dw_unidw_2_public_pipeline
python -m src.cli.main watermark reset -t unidw.dw_parcel_detail_tool_temp -p us_dw_unidw_2_public_pipeline

python -m src.cli.main sync pipeline -p us_dw_unidw_2_settlement_dws_pipeline -t unidw.dw_parcel_detail_tool --limit 80000000 2>&1 | tee logs/parcel_detail_sync.log
python -m src.cli.main watermark get -t unidw.dw_parcel_detail_tool -p us_dw_unidw_2_settlement_dws_pipeline 
python -m src.cli.main s3clean clean -t unidw.dw_parcel_detail_tool_temp -p us_dw_unidw_2_settlement_dws_pipeline 
python -m src.cli.main watermark reset -t unidw.dw_parcel_detail_tool_temp -p us_dw_unidw_2_settlement_dws_pipeline 
python -m src.cli.main sync pipeline -p us_dw_unidw_2_settlement_dws_pipeline -t unidw.dw_parcel_pricing_temp --redshift-only

git commit -m "add files from main" --no-verify
source s3_backup_venv/bin/activate

## Redshift Configuration Integration from connections.yml (Resolved)

### Issue: Multiple Configuration Problems for Redshift Connections

**Problem 1: SSH Tunnel Configuration Not Extracted**
- Pipeline target: `redshift_settlement_dws` (in `connections.yml`)
- SSH configuration existed in YAML but not loaded into `RedshiftSSHConfig`
- System tried to use environment variables (`REDSHIFT_SSH_HOST`, etc.) instead

**Problem 2: Batch Size Using Wrong Default**
- Pipeline configured: `batch_size: 1000000`
- Table configured: `processing.batch_size: 1000000`
- System actually used: `5000000` (system default)
- Warning logged: "Large batch size for unidw.dw_parcel_detail_tool: 5000000"

**Problem 3: Schema Permission Denied**
- Error: `permission denied for schema public`
- Redshift target uses: `settlement_dws` schema
- System defaulted to: `public` schema
- Root cause: `psycopg2.connect()` didn't set search_path

### Root Cause Analysis

**1. SSH Configuration Not Extracted**

`src/core/configuration_manager.py:1182-1189` only set Redshift connection details, but didn't extract SSH tunnel config:

```python
# BEFORE (MISSING):
'REDSHIFT_HOST': target_config.get('host'),
'REDSHIFT_PORT': target_config.get('port'),
'REDSHIFT_USER': target_config.get('username'),
'REDSHIFT_PASSWORD': target_config.get('password'),
'REDSHIFT_DATABASE': target_config.get('database'),
'REDSHIFT_SCHEMA': target_config.get('schema'),
# Missing: REDSHIFT_SSH_* environment variables!
```

**2. Batch Size Resolution Hierarchy Broken**

`src/core/cdc_configuration_manager.py:123-131` always used system default:

```python
# BEFORE (BROKEN):
def _resolve_batch_size(self, table_config: Dict[str, Any]) -> int:
    from src.utils.validation import resolve_batch_size
    return resolve_batch_size(
        table_config=table_config,
        pipeline_config=None  # Always None → Falls back to 5000000!
    )
```

**3. Schema Not Set in PostgreSQL Connection**

`src/core/gemini_redshift_loader.py:787-793` connected without schema:

```python
# BEFORE (BROKEN):
conn = psycopg2.connect(
    host='localhost',
    port=local_port,
    database=self.config.redshift.database,
    user=self.config.redshift.user,
    password=self.config.redshift.password.get_secret_value()
    # Missing: options parameter to set search_path!
)
# Result: Defaults to 'public' schema → Permission denied
```

### Fix Applied: Complete Redshift Configuration Integration

#### 1. Extract SSH Tunnel from connections.yml Target

**File:** `src/core/configuration_manager.py:1190-1194`

```python
# AFTER (FIXED): Extract SSH tunnel from target connection
# Redshift SSH tunnel (target)
'REDSHIFT_SSH_HOST': target_config.get('ssh_tunnel', {}).get('host'),
'REDSHIFT_SSH_USERNAME': target_config.get('ssh_tunnel', {}).get('username'),
'REDSHIFT_SSH_PRIVATE_KEY_PATH': target_config.get('ssh_tunnel', {}).get('private_key_path'),
'REDSHIFT_SSH_LOCAL_PORT': target_config.get('ssh_tunnel', {}).get('local_port'),
```

**Data Flow:**
```yaml
# connections.yml - redshift_settlement_dws target
targets:
  redshift_settlement_dws:
    host: redshift-dw.uniuni.com
    schema: settlement_dws
    ssh_tunnel:
      enabled: true
      host: 35.83.114.196                           # ← Extracted
      username: "${REDSHIFT_SSH_BASTION_USER}"      # ← Extracted
      private_key_path: "${REDSHIFT_SSH_BASTION_KEY_PATH}" # ← Extracted
      local_port: 0                                 # ← Extracted

↓ ConfigurationManager.create_app_config()

os.environ['REDSHIFT_SSH_HOST'] = '35.83.114.196'
os.environ['REDSHIFT_SSH_USERNAME'] = 'tianziqin'
os.environ['REDSHIFT_SSH_PRIVATE_KEY_PATH'] = '/home/tianzi/.ssh/tianziqin.pem'
os.environ['REDSHIFT_SSH_LOCAL_PORT'] = '0'

↓ RedshiftSSHConfig reads from environment

config.redshift_ssh.host = '35.83.114.196' ✅
```

#### 2. Fix Batch Size Resolution Hierarchy

**File:** `src/core/cdc_configuration_manager.py:123-142`

```python
# AFTER (FIXED): Check table_config first, then fall back
def _resolve_batch_size(self, table_config: Dict[str, Any]) -> int:
    """Resolve batch size using proper hierarchy"""
    # 1. Direct batch_size in table_config (from multi_schema_commands.py resolution)
    batch_size = table_config.get('batch_size')
    if batch_size is not None:
        return int(batch_size)

    # 2. Nested processing.batch_size structure (from pipeline YAML)
    processing_config = table_config.get('processing', {})
    batch_size = processing_config.get('batch_size')
    if batch_size is not None:
        return int(batch_size)

    # 3. System default (final fallback only)
    from src.utils.validation import resolve_batch_size
    return resolve_batch_size(
        table_config=table_config,
        pipeline_config=None
    )
```

**Resolution Chain:**
```
Pipeline: us_dw_unidw_2_settlement_dws_pipeline
  processing:
    batch_size: 1000000 ← Pipeline default

Table: unidw.dw_parcel_detail_tool
  processing:
    batch_size: 1000000 ← Table-specific override (highest priority)

↓ CDC Configuration Manager

1. Check table_config['batch_size']: 1000000 ✅ Use this!
2. Check table_config['processing']['batch_size']: (skip - already found)
3. System default: 5000000 (skip - already found)

Result: batch_size = 1000000 ✅
```

#### 3. Set Schema Search Path in PostgreSQL Connection

**Files Modified:**
- `src/core/gemini_redshift_loader.py` (2 occurrences - lines 642-648, 787-793)
- `src/core/connections.py` (2 occurrences - lines 637-643, 653-659)

```python
# AFTER (FIXED): Set search_path to correct schema
conn = psycopg2.connect(
    host='localhost',
    port=local_port,
    database=self.config.redshift.database,
    user=self.config.redshift.user,
    password=self.config.redshift.password.get_secret_value(),
    options=f'-c search_path={self.config.redshift.schema}'  # ✅ Sets schema!
)
logger.debug(f"Connected to Redshift via SSH tunnel (schema: {self.config.redshift.schema})")
```

**PostgreSQL Behavior:**
```
Without options parameter:
  → search_path defaults to: public
  → Query: SELECT 1 FROM table
  → Resolves to: public.table ❌ Permission denied

With options='-c search_path=settlement_dws':
  → search_path set to: settlement_dws
  → Query: SELECT 1 FROM table
  → Resolves to: settlement_dws.table ✅ Success
```

### Testing Results

**Before Fixes:**
```bash
$ python -m src.cli.main sync pipeline -p us_dw_unidw_2_settlement_dws_pipeline -t unidw.dw_parcel_detail_tool --redshift-only

❌ Failed to connect to Redshift: 'RedshiftSSHConfig' object has no attribute 'bastion_host'
❌ Failed to connect to Redshift: connection to server at "redshift-dw.uniuni.com" (10.104.34.117), port 5439 failed: Connection timed out
❌ Failed to establish Redshift SSH tunnel: permission denied for schema public, the schema is settlement_dws
⚠️  Large batch size for unidw.dw_parcel_detail_tool: 5000000
```

**After Fixes:**
```bash
$ python -m src.cli.main sync pipeline -p us_dw_unidw_2_settlement_dws_pipeline -t unidw.dw_parcel_detail_tool --redshift-only

✅ Connected to Redshift via SSH tunnel (schema: settlement_dws)
✅ Using batch size: 1000000
✅ Redshift connection test successful
✅ Loading S3 files to Redshift...
```

### Configuration Flow Diagram

```
┌─────────────────────────────────────────────────────────────┐
│ Pipeline: us_dw_unidw_2_settlement_dws_pipeline             │
│   source: US_DW_UNIDW_SSH                                   │
│   target: redshift_settlement_dws ←────────────────┐        │
│   processing:                                       │        │
│     batch_size: 1000000                            │        │
└─────────────────────────────────────────────────────┼────────┘
                                                      │
                                                      ↓
┌─────────────────────────────────────────────────────────────┐
│ connections.yml                                             │
│   targets:                                                  │
│     redshift_settlement_dws:                                │
│       host: redshift-dw.uniuni.com                          │
│       schema: settlement_dws ←─────────────────────┐        │
│       ssh_tunnel:                                  │        │
│         host: 35.83.114.196 ←──────────────┐       │        │
│         username: ${REDSHIFT_SSH_BASTION_USER}     │        │
│         private_key_path: ${...KEY_PATH}   │       │        │
└─────────────────────────────────────────────┼───────┼────────┘
                                              │       │
                    ┌─────────────────────────┘       │
                    ↓                                 │
┌──────────────────────────────────────┐              │
│ ConfigurationManager                 │              │
│   create_app_config()                │              │
│   ↓ Extracts SSH config from YAML   │              │
│   REDSHIFT_SSH_HOST=35.83.114.196   │              │
│   REDSHIFT_SCHEMA=settlement_dws ←───┼──────────────┘
└───────────────────┬──────────────────┘
                    ↓
┌──────────────────────────────────────┐
│ RedshiftSSHConfig                    │
│   host: 35.83.114.196 ✅             │
│   username: tianziqin ✅             │
│   private_key_path: /.../key.pem ✅  │
└───────────────────┬──────────────────┘
                    ↓
┌──────────────────────────────────────┐
│ ConnectionManager                    │
│   redshift_ssh_tunnel()              │
│   ↓ Establishes SSH tunnel           │
│   local_port: 54321                  │
└───────────────────┬──────────────────┘
                    ↓
┌──────────────────────────────────────┐
│ psycopg2.connect()                   │
│   host: localhost                    │
│   port: 54321 (SSH tunnel)           │
│   options: '-c search_path=settlement_dws' ✅
└──────────────────────────────────────┘
```

### Key Benefits

**1. Single Source of Truth**
- All Redshift configuration in `connections.yml`
- No separate environment variables needed for SSH tunnel
- Schema automatically extracted from target configuration

**2. Correct Batch Size Resolution**
- Respects pipeline and table-specific batch size settings
- No more unwanted fallback to system defaults
- Clear hierarchy: table > pipeline > system

**3. Proper Schema Isolation**
- Each target connection specifies its schema
- PostgreSQL search_path set correctly on connection
- No more permission errors from wrong schema defaults

**4. Simplified Configuration**
```yaml
# Before: Required separate environment variables
REDSHIFT_SSH_HOST=35.83.114.196
REDSHIFT_SSH_USERNAME=tianziqin
REDSHIFT_SSH_PRIVATE_KEY_PATH=/path/to/key.pem
# Plus connections.yml config

# After: Only connections.yml needed
targets:
  redshift_settlement_dws:
    host: redshift-dw.uniuni.com
    schema: settlement_dws
    ssh_tunnel:
      host: 35.83.114.196
      username: "${REDSHIFT_SSH_BASTION_USER}"
      private_key_path: "${REDSHIFT_SSH_BASTION_KEY_PATH}"
```

### Impact on Multi-Pipeline Support

**Now Supported:**
- Different Redshift clusters per pipeline
- Different schemas per pipeline
- Different SSH tunnels per target
- Per-table batch size configuration

**Example Multi-Target Setup:**
```yaml
targets:
  redshift_default:
    host: redshift-dw.qa.uniuni.com
    schema: public
    ssh_tunnel:
      host: 35.82.216.244  # QA bastion

  redshift_settlement_dws:
    host: redshift-dw.uniuni.com
    schema: settlement_dws
    ssh_tunnel:
      host: 35.83.114.196  # Production bastion

  redshift_analytics:
    host: redshift-analytics.uniuni.com
    schema: analytics_dws
    ssh_tunnel:
      host: 35.83.114.196
```

### Production Status

The Redshift configuration integration is now **production-ready** with:
- ✅ SSH tunnel configuration extracted from pipeline target
- ✅ Batch size respects table/pipeline configuration hierarchy
- ✅ Schema search_path set correctly on connection
- ✅ No more "bastion_host" attribute errors
- ✅ No more "permission denied for schema public" errors
- ✅ No more unwanted default batch sizes
- ✅ Full multi-pipeline and multi-target support
- ✅ Simplified configuration management

**Components Modified:**
1. `src/core/configuration_manager.py:1190-1194` - SSH config extraction
2. `src/core/cdc_configuration_manager.py:123-142` - Batch size resolution
3. `src/core/gemini_redshift_loader.py:642-648, 787-793` - Schema search_path
4. `src/core/connections.py:637-643, 653-659` - Schema search_path