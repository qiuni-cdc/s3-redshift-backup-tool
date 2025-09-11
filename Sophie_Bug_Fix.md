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

## Usually Used Commands
python -m src.cli.main watermark reset -t unidw.dw_parcel_detail_tool -p us_dw_unidw_2_public_pipeline  
python -m src.cli.main watermark get -t unidw.dw_parcel_detail_tool -p us_dw_unidw_2_public_pipeline 
python -m src.cli.main sync pipeline -p us_dw_unidw_2_public_pipeline -t  unidw.dw_parcel_detail_tool --limit 10000 
python -m src.cli.main s3clean list -t unidw.dw_parcel_detail_tool -p us_dw_unidw_2_public_pipeline  
python -m src.cli.main watermark set -t unidw.dw_parcel_detail_tool -p us_dw_unidw_2_public_pipeline --id 0 
python -m src.cli.main sync pipeline -p us_dw_unidw_2_public_pipeline -t unidw.dw_parcel_detail_tool --redshift-only 2>&1 | tee redshift_load.log


python -m src.cli.main watermark get -t unidw.dw_parcel_pricing_temp -p us_dw_unidw_2_public_pipeline 
python -m src.cli.main watermark reset -t unidw.dw_parcel_pricing_temp -p us_dw_unidw_2_public_pipeline  
python -m src.cli.main s3clean list -t unidw.dw_parcel_pricing_temp -p us_dw_unidw_2_public_pipeline   
python -m src.cli.main s3clean clean -t unidw.dw_parcel_pricing_temp -p us_dw_unidw_2_public_pipeline
python -m src.cli.main sync pipeline -p us_dw_unidw_2_public_pipeline -t unidw.dw_parcel_pricing_temp --limit 100 2>&1 | tee pricing_temp.log 
python -m src.cli.main sync pipeline -p us_dw_unidw_2_public_pipeline -t unidw.dw_parcel_pricing_temp --backup-only --limit 30000 2>&1 | tee pricing_temp.log

## File Duplication in Watermark Bug Fix (Resolved)

### Issue: Files Appearing in Both Backup and Processed Lists

**Problem**: Files were appearing in both `backup_s3_files` and `processed_s3_files` lists simultaneously, causing confusion about which files need Redshift loading.

**Symptoms**:
- Same file appears in both backup and processed lists
- Files marked as processed but still showing as awaiting Redshift load
- Debug output showed chunk_s3_files containing already processed files

**Example Debug Output**:
```
existing_backup_files (0):
chunk_s3_files (2):
  0. us_dw_unidw_ssh_unidw_dw_parcel_pricing_temp_20250911_152442_batch_chunk_1_batch_1.parquet
  1. us_dw_unidw_ssh_unidw_dw_parcel_pricing_temp_20250911_153130_batch_chunk_1_batch_1.parquet
processed_s3_files (1):
  0. us_dw_unidw_ssh_unidw_dw_parcel_pricing_temp_20250911_152442_batch_chunk_1_batch_1.parquet
```

**Root Cause**: 
The `_get_chunk_s3_files()` method discovered files based on recent upload time (last 5 minutes), which included files that were already processed in previous runs but still within the time window.

**Technical Details**:
- `chunk_s3_files` contained files from S3 scan of recently uploaded files
- This list included files already in `processed_s3_files` from previous runs
- Backup logic added all chunk files to backup list without checking if already processed

**Fix Applied**:
```python
# BEFORE (BUGGY): Added all chunk files without checking
for file_uri in chunk_s3_files:
    if file_uri not in all_backup_files:
        all_backup_files.append(file_uri)

# AFTER (FIXED): Skip already processed files
processed_files = set(watermark.processed_s3_files) if watermark.processed_s3_files else set()

for file_uri in chunk_s3_files:
    file_name = file_uri.split('/')[-1]
    if file_uri in processed_files:
        print(f"  Skipped (already processed): {file_name}")
    elif file_uri not in all_backup_files:
        all_backup_files.append(file_uri)
        print(f"  Added to backup: {file_name}")
```

**Location**: `src/backup/row_based.py:1261-1270`

**Result After Fix**:
- Files only appear in one list: backup OR processed, never both
- Debug output shows "Skipped (already processed)" for duplicate files
- Clean file lifecycle: S3 upload → backup_s3_files → Redshift load → processed_s3_files
- No more confusion about file processing status

**Prevention**: The fix ensures that the file discovery mechanism checks against already processed files before adding to backup queue, maintaining proper file state separation.