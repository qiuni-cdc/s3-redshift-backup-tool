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
python -m src.cli.main sync pipeline -p us_dw_unidw_2_public_pipeline -t unidw.dw_parcel_pricing_temp --backup-only --limit 30000 2>&1 | tee pricing_temp.log 

python -m src.cli.main watermark set -t unidw.dw_parcel_detail_tool_temp -p us_dw_unidw_2_public_pipeline --id 0
python -m src.cli.main sync pipeline -p us_dw_unidw_2_public_pipeline -t unidw.dw_parcel_detail_tool_temp --json-output sync_parcel_detail.json 
python -m src.cli.main watermark get -t unidw.dw_parcel_detail_tool_temp -p us_dw_unidw_2_public_pipeline
python -m src.cli.main s3clean list -t unidw.dw_parcel_detail_tool_temp -p us_dw_unidw_2_public_pipeline
python -m src.cli.main s3clean clean -t unidw.dw_parcel_detail_tool_temp -p us_dw_unidw_2_public_pipeline
python -m src.cli.main watermark reset -t unidw.dw_parcel_detail_tool_temp -p us_dw_unidw_2_public_pipeline

git commit -m "add files from main" --no-verify
source s3_backup_venv/bin/activate

---

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