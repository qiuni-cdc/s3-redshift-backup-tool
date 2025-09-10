# S3-Redshift Backup System - Q&A

## 📋 **Table of Contents**
- [🔥 Hot Questions & Critical Issues](#-hot-questions--critical-issues)
- [🔖 Watermark Management](#-watermark-management)
- [📁 File Filtering & Selection](#-file-filtering--selection)
- [⚡ Sync Behavior & Limits](#-sync-behavior--limits)
- [🔧 Troubleshooting](#-troubleshooting)
- [🚀 Performance & Optimization](#-performance--optimization)
- [💡 Common Scenarios](#-common-scenarios)
- [🏗️ Architecture & Design](#️-architecture--design)

---

## 🔥 **Hot Questions & Critical Issues**

### **Q: Why did my sync load 5M+ rows when I used --limit 50000?**
**A:** **CRITICAL MISUNDERSTANDING** - The `--limit` parameter only controls **NEW MySQL extraction**, not **S3-to-Redshift loading**.

**What Actually Happens:**
```bash
# You run this:
python -m src.cli.main sync --limit 50000

# What happens:
# 1. MySQL → S3: Extract MAX 50,000 NEW rows ✅
# 2. S3 → Redshift: Load ALL files after watermark cutoff ⚠️ 
#    (Could be 506 files × 10K rows = 5M+ rows)
```

**The Two-Stage Process:**
- **Stage 1 (MySQL → S3)**: Respects `--limit 50000`
- **Stage 2 (S3 → Redshift)**: Loads ALL eligible files (no row limit)

**Solution:** Control Redshift loading with watermark management, not `--limit`.

### **Q: My sync reports "✅ success" but loaded 0 rows. What's wrong?**
**A:** **FILE FILTERING BUG** - The most common issue that was recently fixed.

**Root Cause:** Complex session window logic was excluding valid files.

**Before Fix:**
```
"Found 0 S3 parquet files" → "0 rows, 0 files" → "✅ success" (FALSE POSITIVE)
```

**After Fix:**
```
"Found 506 S3 parquet files" → "loaded 10,000+ rows" → "✅ success" (REAL SUCCESS)
```

**Verification Steps:**
1. Check logs for "No S3 parquet files found"
2. Run `watermark get` to see actual file counts
3. Look for "After cutoff: False" in debug logs

### **Q: I get "TypeError: can't compare offset-naive and offset-aware datetimes"**
**A:** **TIMEZONE BUG** - Fixed in latest version.

**Root Cause:** Watermark timestamps could be timezone-naive while S3 file timestamps were timezone-aware.

**Fix Applied:** Robust timezone handling ensures both timestamps are timezone-aware before comparison.

**If Still Occurring:** Update to latest codebase - this bug has been completely resolved.

### **Q: My watermark shows 3M extracted rows but Redshift has 2.5M. Why?**
**A:** **WATERMARK DOUBLE-COUNTING BUG** - Fixed with v2.0 watermark system.

**Old Problem:** Additive watermark logic caused double-counting in multi-session backups.

**New Solution:** v2.0 system uses absolute counts and proper session tracking.

**Fix Command:**
```bash
# Correct the inflated count
python -m src.cli.main watermark-count set-count -t TABLE --count 2500000 --mode absolute
```

### **Q: Why does the system load old files from weeks ago?**
**A:** **WATERMARK CUTOFF LOGIC** - All files after the watermark timestamp are loaded.

**Example:**
- **Watermark cutoff**: 2025-08-25 12:40:34
- **Files loaded**: ALL files created after Aug 25 (including Sept 8, Sept 9, etc.)
- **Result**: 506 files spanning multiple weeks

**Solutions:**
```bash
# Option 1: Reset watermark to recent date
python -m src.cli.main watermark reset -p PIPELINE -t TABLE
python -m src.cli.main watermark set -p PIPELINE -t TABLE --timestamp '2025-09-09 00:00:00'

# Option 2: Clean old S3 files
python -m src.cli.main s3clean clean -t TABLE --older-than "7d"
```

### **Q: SSH tunnel keeps failing during sync**
**A:** **CONNECTION MANAGEMENT ISSUE** - Multiple SSH tunnels competing.

**Common Causes:**
- Multiple sync processes opening concurrent tunnels
- SSH key permission issues (should be 600)
- Bastion host connection limits
- Network timeouts during large data transfers

**Quick Fixes:**
```bash
# Check SSH key permissions
chmod 600 ~/.ssh/your_key.pem

# Test SSH connectivity
ssh -i ~/.ssh/your_key.pem user@bastion_host

# Use smaller batch sizes to reduce tunnel duration
python -m src.cli.main sync --limit 10000
```

### **Q: Schema errors about "incompatible Parquet schema" or "VARCHAR length exceeded"**
**A:** **SCHEMA ARCHITECTURE BUG** - Major fix implemented.

**Root Cause:** Three different schema discovery systems with conflicting precision handling.

**Fix Applied:** 
- Unified all components under single `FlexibleSchemaManager`
- Automatic VARCHAR length doubling for safety (255 → 510)
- Column name sanitization for Redshift compatibility (`190_time` → `col_190_time`)

**Result:** Schema compatibility issues resolved automatically.

### **Q: Files are being loaded multiple times to Redshift**
**A:** **BLACKLIST DEDUPLICATION FAILURE** - Check v2.0 migration status.

**Causes:**
- Using legacy watermark system
- Inconsistent S3 URI formats in blacklist
- Watermark corruption

**Diagnosis:**
```bash
# Check watermark version and processed files
python -m src.cli.main watermark get -t TABLE --show-files

# Look for consistent S3 URI format
# Should be: s3://bucket/path/file.parquet
```

### **Q: Sync is extremely slow (hours for small tables)**
**A:** **PERFORMANCE BOTTLENECK** - Multiple potential causes.

**Common Issues:**
- Too many S3 files to scan (1000+)
- Large batch sizes causing memory issues
- SSH tunnel latency
- Network bandwidth constraints

**Performance Optimization:**
```bash
# Clean old files
python -m src.cli.main s3clean clean -t TABLE --older-than "30d"

# Use smaller batches
python -m src.cli.main sync --limit 10000

# Check file count
python -m src.cli.main s3clean list -t TABLE
```

---

## 🔖 **Watermark Management**

### **Q: What exactly is a watermark?**
**A:** A **checkpoint system** that tracks sync progress to enable incremental processing.

**Watermark Components:**
```json
{
  "mysql_state": {
    "last_timestamp": "2025-08-25T06:30:08",  // Data cutoff point
    "last_id": 80453182,                       // Highest ID processed
    "total_rows": 110000,                      // Cumulative extracted rows
    "status": "success"                        // MySQL stage status
  },
  "redshift_state": {
    "total_rows": 0,                           // Redshift loaded rows
    "status": "success"                        // Redshift stage status
  },
  "processed_files": [                         // Blacklist for deduplication
    "s3://bucket/file1.parquet",
    "s3://bucket/file2.parquet"
  ]
}
```

### **Q: How do I check what my watermark contains?**
**A:** Multiple levels of detail available:

```bash
# Basic watermark info
python -m src.cli.main watermark get -p PIPELINE -t TABLE

# Show all processed files (blacklist)
python -m src.cli.main watermark get -p PIPELINE -t TABLE --show-files

# Validate row counts against Redshift
python -m src.cli.main watermark-count validate-counts -p PIPELINE -t TABLE
```

### **Q: When should I reset a watermark?**
**A:** **Reset scenarios:**
- **Fresh sync**: Start from beginning or specific date
- **Data corruption**: Redshift table was dropped/corrupted
- **Testing**: Need to reprocess same data
- **Migration**: Moving to new environment

**Reset methods:**
```bash
# Complete reset (dangerous - clears all progress)
python -m src.cli.main watermark reset -p PIPELINE -t TABLE

# Set specific starting point (safer)
python -m src.cli.main watermark set -p PIPELINE -t TABLE --timestamp '2025-09-01 00:00:00'
```

### **Q: What's the difference between v1.0 and v2.0 watermarks?**
**A:** **Major architectural improvements:**

**v1.0 (Legacy Problems):**
- Complex accumulation logic → double-counting bugs
- Session tracking → false negatives  
- Multiple storage locations → consistency issues

**v2.0 (Current Benefits):**
- Simple absolute counts → no accumulation bugs
- Blacklist deduplication → reliable duplicate prevention
- Unified storage → consistent state management
- Automatic migration → zero breaking changes

### **Q: How do I fix watermark count discrepancies?**
**A:** **Systematic approach:**

```bash
# Step 1: Diagnose the issue
python -m src.cli.main watermark-count validate-counts -p PIPELINE -t TABLE

# Step 2: Get actual Redshift count
# Query: SELECT COUNT(*) FROM your_table;

# Step 3: Fix with actual count
python -m src.cli.main watermark-count set-count -p PIPELINE -t TABLE --count ACTUAL_COUNT --mode absolute

# Step 4: Verify fix
python -m src.cli.main watermark get -p PIPELINE -t TABLE
```

---

## 📁 **File Filtering & Selection**

### **Q: How exactly does the system decide which S3 files to load?**
**A:** **Simple watermark-based logic** (no complex session windows):

```python
def should_load_file(file_timestamp, watermark_cutoff, processed_files):
    # Step 1: Skip if already processed (blacklist check)
    if file_uri in processed_files:
        return False
    
    # Step 2: Include if newer than watermark cutoff
    if file_timestamp > watermark_cutoff:
        return True
    
    # Step 3: If no watermark, include all files (fresh sync)
    if not watermark_cutoff:
        return True
    
    return False
```

**Key Points:**
- **No arbitrary time windows** (old session logic removed)
- **No row count limits** (only timestamp-based)
- **Deterministic behavior** (same inputs = same outputs)

### **Q: Why does "Found 506 S3 files" seem wrong for my table?**
**A:** **File accumulation over time** - the system finds ALL files after watermark cutoff.

**Example Analysis:**
- **Watermark cutoff**: 2025-08-25 12:40:34
- **Files found**: Every file created after this timestamp
- **Sources**: Test runs, failed syncs, historical backups, development files

**File breakdown:**
```bash
# Check what files exist
python -m src.cli.main s3clean list -t TABLE --show-timestamps

# Expected output:
# 2025-08-29: 50 files (historical backup)
# 2025-09-08: 450 files (large test run)  
# 2025-09-09: 6 files (recent tests)
# Total: 506 files
```

### **Q: How can I control which files get loaded without resetting everything?**
**A:** **Selective file management strategies:**

**Option 1 - Update Watermark Cutoff:**
```bash
# Only load files from last 3 days
python -m src.cli.main watermark set -p PIPELINE -t TABLE --timestamp '2025-09-07 00:00:00'
```

**Option 2 - Clean Old Files:**
```bash
# Remove files older than 7 days
python -m src.cli.main s3clean clean -t TABLE --older-than "7d" --dry-run  # Preview first
python -m src.cli.main s3clean clean -t TABLE --older-than "7d"             # Execute
```

**Option 3 - Manual Blacklist Management:**
```bash
# Check current processed files
python -m src.cli.main watermark get -t TABLE --show-files

# Note: No direct blacklist editing - would need code changes
```

### **Q: What's the difference between filename timestamp and S3 LastModified?**
**A:** **Two timestamp sources with different meanings:**

**Filename Timestamp (Preferred):**
- **Format**: `us_dw_ro_ssh_settlement_settle_orders_20250909_170529_batch_chunk_1_batch_1.parquet`
- **Extraction**: `20250909_170529` → `2025-09-09 17:05:29`
- **Meaning**: When the backup process ran (accurate for filtering)

**S3 LastModified (Fallback):**
- **Source**: S3 object metadata
- **Meaning**: When file was uploaded to S3 (may be delayed)
- **Issue**: Can be minutes/hours after actual backup time

**System Logic:**
```python
# Prefer filename timestamp for accuracy
effective_timestamp = filename_timestamp if filename_timestamp else s3_last_modified
```

### **Q: Why did the old session window logic fail?**
**A:** **Complex logic created false negatives:**

**Old Problematic Logic:**
```python
# Generated 12-hour time windows around extraction time
session_start = mysql_extraction_time - 2_hours
session_end = mysql_extraction_time + 10_hours

# Files excluded if outside window (WRONG!)
if not (session_start <= file_time <= session_end):
    exclude_file()  # False negative!
```

**Problems:**
- Arbitrary time calculations
- Timezone confusion
- Edge cases where valid files were excluded
- Hard to debug and understand

**New Simple Logic:**
```python
# Just compare timestamps directly
if file_timestamp > watermark_cutoff:
    include_file()  # Clear and reliable
```

---

## ⚡ **Sync Behavior & Limits**

### **Q: What do all the different sync command variations do?**
**A:** **Complete command reference:**

```bash
# Full pipeline (most common)
python -m src.cli.main sync -p PIPELINE -t TABLE
# MySQL → S3 → Redshift

# Backup only (testing MySQL extraction)
python -m src.cli.main sync -p PIPELINE -t TABLE --backup-only
# MySQL → S3 (stops here)

# Redshift only (load existing S3 files)
python -m src.cli.main sync -p PIPELINE -t TABLE --redshift-only
# S3 → Redshift (skips MySQL)

# With row limits (limits MySQL extraction)
python -m src.cli.main sync -p PIPELINE -t TABLE --limit 50000
# MySQL: max 50K rows → S3 → Redshift: ALL eligible files

# Testing with dry run
python -m src.cli.main sync -p PIPELINE -t TABLE --dry-run
# Preview operations without execution
```

### **Q: How do I test sync behavior without processing large amounts of data?**
**A:** **Progressive testing approach:**

**Step 1 - Preview Operations:**
```bash
# See what would happen
python -m src.cli.main sync -p PIPELINE -t TABLE --dry-run
```

**Step 2 - Small MySQL Test:**
```bash
# Extract only 1000 rows
python -m src.cli.main sync -p PIPELINE -t TABLE --limit 1000 --backup-only
```

**Step 3 - Check S3 Files:**
```bash
# See what files exist
python -m src.cli.main s3clean list -t TABLE
```

**Step 4 - Limited Redshift Test:**
```bash
# Update watermark to only load recent files
python -m src.cli.main watermark set -p PIPELINE -t TABLE --timestamp '2025-09-09 16:00:00'
python -m src.cli.main sync -p PIPELINE -t TABLE --redshift-only
```

### **Q: Why does --max-chunks not limit Redshift loading?**
**A:** **Batch parameters only affect MySQL extraction:**

**Parameter Scope:**
- `--limit N`: Rows per MySQL query chunk
- `--max-chunks M`: Maximum number of chunks to process
- **Total MySQL extraction**: N × M rows maximum
- **Redshift loading**: Not affected by these parameters

**Example:**
```bash
# This limits MySQL to 5,000 rows total
python -m src.cli.main sync --limit 1000 --max-chunks 5

# But Redshift will still load ALL eligible S3 files
# (Could be millions of rows if many files exist)
```

### **Q: When does the system create new S3 files vs. load existing ones?**
**A:** **Two-stage decision logic:**

**MySQL → S3 Stage:**
- **Creates new files** if watermark shows new data available
- **Skips creation** if no new data since last watermark
- **File naming**: Includes timestamp for tracking

**S3 → Redshift Stage:**
- **Loads existing files** that match filtering criteria
- **Never creates files** (only reads existing ones)
- **Processes files** created by current or previous backup sessions

**File Flow:**
```
MySQL Data → [Backup Creates S3 Files] → [Redshift Loads S3 Files] → Redshift Table
             ↑                          ↑
             Controlled by --limit      Controlled by watermark cutoff
```

---

## 🔧 **Troubleshooting**

### **Q: Sync hangs on "Executing CDC-generated chunk query" - what's wrong?**
**A:** **MySQL query performance issue:**

**Common Causes:**
- **Large date range**: Query scanning millions of rows
- **Missing indexes**: No index on timestamp/ID columns
- **Lock contention**: Other processes holding table locks
- **Network timeout**: SSH tunnel disconnection

**Immediate Solutions:**
```bash
# Use smaller batches
python -m src.cli.main sync --limit 10000

# Use more recent watermark (smaller date range)
python -m src.cli.main watermark set -p PIPELINE -t TABLE --timestamp '2025-09-09 12:00:00'

# Check if indexes exist on created_at and id columns
```

**Query Analysis:**
```sql
-- Check what the system is trying to query
SELECT COUNT(*) FROM settlement.settle_orders 
WHERE created_at > '2025-08-25 00:00:00' 
   OR (created_at = '2025-08-25 00:00:00' AND id > 0);

-- If this takes forever, you need better indexes
```

### **Q: I get "No successful MySQL backup found" but backup seemed to work**
**A:** **Watermark status mismatch:**

**Root Cause:** MySQL stage watermark shows 'failed' or 'pending' status.

**Diagnosis:**
```bash
# Check current watermark status
python -m src.cli.main watermark get -p PIPELINE -t TABLE

# Look for:
# MySQL Status: failed/pending (problem)
# vs
# MySQL Status: success (required for Redshift loading)
```

**Solutions:**
```bash
# Option 1: Retry backup stage
python -m src.cli.main sync -p PIPELINE -t TABLE --backup-only

# Option 2: Manual watermark fix (if data is actually in S3)
# Check S3 first: python -m src.cli.main s3clean list -t TABLE
# If files exist, this might be a watermark state issue
```

### **Q: SSH tunnel works for MySQL but fails for Redshift (or vice versa)**
**A:** **Different bastion hosts/configurations:**

**Common Setup:**
- **MySQL**: Uses one bastion host (e.g., 35.83.114.196)
- **Redshift**: Uses different bastion host (e.g., 35.82.216.244)
- **Different keys**: May require different SSH keys

**Debugging:**
```bash
# Test MySQL bastion
ssh -i ~/.ssh/mysql_key.pem user@35.83.114.196

# Test Redshift bastion  
ssh -i ~/.ssh/redshift_key.pem user@35.82.216.244

# Check key permissions
ls -la ~/.ssh/*.pem
# Should be 600 (-rw-------)
```

**Configuration Check:**
- Verify both bastion hosts are configured correctly
- Ensure SSH keys are deployed to both bastions
- Check if connection limits are being hit

### **Q: Files show "✅ INCLUDING file" in logs but still get "No S3 parquet files found"**
**A:** **Log vs. actual filtering mismatch:**

**Possible Causes:**
1. **Exception after filtering**: Error occurred after file selection but before return
2. **Empty filtered list**: All files were actually excluded despite logs
3. **Blacklist exclusion**: Files were excluded by processed files check
4. **Code bug**: Logging and actual filtering logic diverged

**Debug Steps:**
```bash
# Check for error traceback in logs
grep -A 10 "No S3 parquet files found" sync_log.log

# Verify processed files blacklist
python -m src.cli.main watermark get -p PIPELINE -t TABLE --show-files

# Check if files actually exist in S3
python -m src.cli.main s3clean list -t TABLE --show-timestamps
```

### **Q: Redshift COPY command fails with "access denied" or permission errors**
**A:** **IAM/Redshift permissions issue:**

**Required Permissions:**
- **S3**: `s3:GetObject` on the bucket/prefix
- **Redshift**: `COPY` privilege on target table
- **IAM role**: Must be associated with Redshift cluster

**Quick Verification:**
```sql
-- Test Redshift access
SELECT COUNT(*) FROM your_table LIMIT 1;

-- Test S3 access (run in Redshift)
COPY your_table FROM 's3://bucket/test/path/' 
IAM_ROLE 'arn:aws:iam::account:role/RedshiftRole'
FORMAT AS PARQUET;
```

**Common Issues:**
- IAM role not attached to Redshift cluster
- S3 bucket policy blocking access
- Redshift in different region than S3 bucket

---

## 🚀 **Performance & Optimization**

### **Q: How many S3 files is "too many" for good performance?**
**A:** **Performance thresholds and optimization:**

**File Count Guidelines:**
- **< 100 files**: Excellent performance, no optimization needed
- **100-1000 files**: Good performance, monitor processed file list size
- **1000-10000 files**: Acceptable with optimization (caching, pagination)
- **> 10000 files**: Performance warnings, requires cleanup

**Performance Bottlenecks:**
```bash
# Check current file count
python -m src.cli.main watermark get -t TABLE --show-files | wc -l

# Get file statistics
python -m src.cli.main s3clean list -t TABLE
```

**Optimization Strategies:**
```bash
# Regular cleanup (recommended)
python -m src.cli.main s3clean clean -t TABLE --older-than "30d"

# Aggressive cleanup (if needed)
python -m src.cli.main s3clean clean -t TABLE --older-than "7d"

# Pattern-based cleanup
python -m src.cli.main s3clean clean -t TABLE --pattern "test_*"
```

### **Q: What's the optimal batch size for different table sizes?**
**A:** **Batch size tuning guidelines:**

**Small Tables (< 1M rows):**
- **Batch size**: 50K-100K rows
- **Memory**: Low impact
- **Speed**: Fewer chunks = faster

**Medium Tables (1M-10M rows):**
- **Batch size**: 25K-50K rows  
- **Balance**: Performance vs. memory
- **Recommendation**: 50K for most cases

**Large Tables (> 10M rows):**
- **Batch size**: 10K-25K rows
- **Memory**: Critical consideration
- **Network**: Smaller chunks = more resilient

**Testing Approach:**
```bash
# Start conservative
python -m src.cli.main sync --limit 10000

# Scale up if performance is good
python -m src.cli.main sync --limit 25000

# Monitor memory usage and adjust
```

### **Q: How can I speed up slow syncs?**
**A:** **Systematic performance optimization:**

**Step 1 - Identify Bottleneck:**
```bash
# Check file count
python -m src.cli.main s3clean list -t TABLE | wc -l

# Check watermark state
python -m src.cli.main watermark get -p PIPELINE -t TABLE

# Check S3 file age distribution
python -m src.cli.main s3clean list -t TABLE --show-timestamps
```

**Step 2 - Apply Targeted Fixes:**

**Too Many Files:**
```bash
# Clean old files
python -m src.cli.main s3clean clean -t TABLE --older-than "7d"
```

**Large Date Range:**
```bash
# Update watermark to recent date
python -m src.cli.main watermark set -p PIPELINE -t TABLE --timestamp '2025-09-08 00:00:00'
```

**Network Issues:**
```bash
# Smaller batches
python -m src.cli.main sync --limit 10000

# Check SSH tunnel stability
```

**Memory Issues:**
```bash
# Reduce batch size
python -m src.cli.main sync --limit 5000
```

### **Q: When should I use s3clean vs. watermark reset?**
**A:** **Different approaches for different goals:**

**Use s3clean when:**
- **Goal**: Remove unnecessary files while preserving progress
- **Scenario**: Too many old/test files cluttering S3
- **Benefit**: Keeps watermark progress, just removes files
- **Safety**: Non-destructive to sync progress

```bash
# Removes old files but keeps watermark intact
python -m src.cli.main s3clean clean -t TABLE --older-than "30d"
```

**Use watermark reset when:**
- **Goal**: Start sync progress from scratch
- **Scenario**: Data corruption, testing, environment migration
- **Benefit**: Clean slate for sync process
- **Risk**: Loses all progress, may reprocess data

```bash
# Resets all progress, forces full reload
python -m src.cli.main watermark reset -p PIPELINE -t TABLE
```

**Hybrid Approach:**
```bash
# Clean files first, then reset watermark to specific date
python -m src.cli.main s3clean clean -t TABLE --older-than "7d"
python -m src.cli.main watermark set -p PIPELINE -t TABLE --timestamp '2025-09-08 00:00:00'
```

---

## 💡 **Common Scenarios**

### **Q: How do I start a completely fresh sync from a specific date?**
**A:** **Complete fresh sync procedure:**

```bash
# Step 1: Reset all watermark data
python -m src.cli.main watermark reset -p PIPELINE -t TABLE

# Step 2: Set starting timestamp
python -m src.cli.main watermark set -p PIPELINE -t TABLE --timestamp '2025-09-01 00:00:00'

# Step 3: Clean any conflicting S3 files (optional)
python -m src.cli.main s3clean clean -t TABLE --older-than "40d"

# Step 4: Run fresh sync
python -m src.cli.main sync -p PIPELINE -t TABLE

# Result: Will extract all data from Sept 1st onwards
```

### **Q: How do I load only files from the last 24 hours?**
**A:** **Recent-files-only loading:**

```bash
# Option 1: Update watermark cutoff
python -m src.cli.main watermark set -p PIPELINE -t TABLE --timestamp '2025-09-09 00:00:00'
python -m src.cli.main sync -p PIPELINE -t TABLE --redshift-only

# Option 2: Clean old files first
python -m src.cli.main s3clean clean -t TABLE --older-than "1d"
python -m src.cli.main sync -p PIPELINE -t TABLE --redshift-only

# Option 3: Check what files exist first
python -m src.cli.main s3clean list -t TABLE --show-timestamps
# Then decide on cutoff timestamp
```

### **Q: How do I test sync with a small dataset without affecting production?**
**A:** **Safe testing methodology:**

```bash
# Step 1: Test with small extraction
python -m src.cli.main sync -p PIPELINE -t TABLE --limit 1000 --backup-only

# Step 2: Verify S3 files were created
python -m src.cli.main s3clean list -t TABLE

# Step 3: Set watermark to only load test files
python -m src.cli.main watermark set -p PIPELINE -t TABLE --timestamp '2025-09-09 16:00:00'

# Step 4: Load only recent test files
python -m src.cli.main sync -p PIPELINE -t TABLE --redshift-only

# Result: Only test data is processed, no impact on production watermarks
```

### **Q: How do I recover from a failed sync that left things in a bad state?**
**A:** **Systematic recovery procedure:**

**Step 1 - Assess Current State:**
```bash
# Check watermark status
python -m src.cli.main watermark get -p PIPELINE -t TABLE

# Check S3 files
python -m src.cli.main s3clean list -t TABLE

# Check Redshift table (if accessible)
# SELECT COUNT(*) FROM your_table;
```

**Step 2 - Identify Failure Point:**
- **MySQL stage failed**: Retry backup-only
- **Redshift stage failed**: Retry redshift-only  
- **Both failed**: Full reset and restart
- **Partial success**: Verify data integrity

**Step 3 - Apply Appropriate Recovery:**

**MySQL Stage Recovery:**
```bash
python -m src.cli.main sync -p PIPELINE -t TABLE --backup-only
```

**Redshift Stage Recovery:**
```bash
python -m src.cli.main sync -p PIPELINE -t TABLE --redshift-only
```

**Complete Recovery:**
```bash
# If data integrity is questionable
python -m src.cli.main watermark reset -p PIPELINE -t TABLE
python -m src.cli.main sync -p PIPELINE -t TABLE
```

### **Q: How do I migrate from one environment to another?**
**A:** **Environment migration procedure:**

**Step 1 - Export Current State:**
```bash
# Document current watermark
python -m src.cli.main watermark get -p PIPELINE -t TABLE > watermark_backup.txt

# List all S3 files
python -m src.cli.main s3clean list -t TABLE > s3_files_backup.txt
```

**Step 2 - Setup New Environment:**
- Configure new pipeline definitions
- Set up connection strings
- Test connectivity

**Step 3 - Initialize New Environment:**
```bash
# Option A: Start fresh
python -m src.cli.main watermark reset -p NEW_PIPELINE -t TABLE
python -m src.cli.main sync -p NEW_PIPELINE -t TABLE

# Option B: Resume from specific point
python -m src.cli.main watermark set -p NEW_PIPELINE -t TABLE --timestamp 'LAST_KNOWN_GOOD_TIMESTAMP'
python -m src.cli.main sync -p NEW_PIPELINE -t TABLE
```

---

## 🏗️ **Architecture & Design**

### **Q: Why was the file filtering logic redesigned?**
**A:** **Critical reliability issues with old design:**

**Old Session Window Problems:**
- **Complex time calculations**: 12-hour windows, timezone arithmetic
- **False negatives**: Valid files excluded by arbitrary rules
- **Hard to debug**: Multiple variables affecting file selection
- **Unreliable**: Edge cases caused data loading failures

**New Watermark-Based Benefits:**
- **Simple logic**: Direct timestamp comparison
- **Deterministic**: Same inputs always produce same outputs
- **Debuggable**: Clear include/exclude reasoning in logs
- **Reliable**: No arbitrary time windows to fail

**Technical Comparison:**
```python
# OLD (Problematic):
session_start = extraction_time - timedelta(hours=2)
session_end = extraction_time + timedelta(hours=10)
if session_start <= file_time <= session_end:
    include_file()  # Complex, error-prone

# NEW (Simple):
if file_time > watermark_cutoff:
    include_file()  # Clear, reliable
```

### **Q: What's the difference between sequential and parallel backup strategies?**
**A:** **Two processing approaches with different trade-offs:**

**Sequential Backup (Default):**
- **Process**: One table at a time
- **Benefits**: Predictable resource usage, easier debugging
- **Best for**: Most use cases, large tables, limited resources
- **Memory**: Lower peak usage
- **Debugging**: Simpler error isolation

**Parallel Backup:**
- **Process**: Multiple tables simultaneously  
- **Benefits**: Higher throughput for many small tables
- **Best for**: Many small tables, abundant resources
- **Memory**: Higher peak usage
- **Complexity**: More complex error handling

**Configuration:**
```yaml
# Sequential (recommended)
processing:
  strategy: "sequential"
  max_parallel_tables: 1

# Parallel (advanced)
processing:
  strategy: "parallel"
  max_parallel_tables: 4
```

### **Q: How does the v2.0 watermark system prevent double-counting?**
**A:** **Architectural improvements to eliminate accumulation bugs:**

**v1.0 Problem (Fixed):**
```python
# Additive logic caused double-counting
session_1_rows = 500000
session_2_total = 2500000  # Already includes session 1
watermark_count = session_1_rows + session_2_total  # = 3M (WRONG!)
```

**v2.0 Solution:**
```python
# Absolute counts with session tracking
if same_session:
    watermark_count = session_total  # Replace (no addition)
else:
    watermark_count += incremental_rows  # Add only new rows
```

**Key Mechanisms:**
- **Session ID tracking**: Prevents intra-session double-counting
- **Mode control**: Auto-detection of update type (absolute vs. additive)
- **External validation**: CLI commands to verify against Redshift data
- **Blacklist deduplication**: File-level duplicate prevention

### **Q: Why does the system use both timestamp and ID for CDC?**
**A:** **Hybrid CDC strategy for maximum reliability:**

**Timestamp-Only Problems:**
- **Non-unique**: Multiple records can have same timestamp
- **Clock skew**: Server time inconsistencies
- **Resolution limits**: Millisecond precision may not be enough

**ID-Only Problems:**
- **Not time-based**: Can't easily resume from specific date
- **ID gaps**: Deleted records create holes
- **Rollback issues**: ID sequences may not be monotonic

**Hybrid Benefits:**
```sql
-- Combines both for reliability
SELECT * FROM table 
WHERE timestamp > '2025-08-25 06:30:08'
   OR (timestamp = '2025-08-25 06:30:08' AND id > 80453182)
ORDER BY timestamp, id
```

**Advantages:**
- **Reliable pagination**: No missed records due to timestamp ties
- **Time-based resumption**: Can restart from specific dates
- **Monotonic ordering**: Consistent processing order
- **Gap handling**: Handles deletions and ID gaps gracefully

### **Q: How does the blacklist deduplication system work?**
**A:** **File-level duplicate prevention mechanism:**

**Blacklist Storage:**
```json
{
  "processed_files": [
    "s3://bucket/incremental/year=2025/month=09/day=09/file1.parquet",
    "s3://bucket/incremental/year=2025/month=09/day=09/file2.parquet"
  ]
}
```

**Lookup Process:**
```python
# O(1) set-based lookup for performance
processed_files_set = set(watermark.processed_files)

for s3_file in candidate_files:
    if s3_file in processed_files_set:
        skip_file(s3_file)  # Already processed
    else:
        process_file(s3_file)  # New file
        processed_files_set.add(s3_file)  # Add to blacklist
```

**Performance Optimization:**
- **Cached sets**: O(1) lookup for large file lists
- **Efficient storage**: JSON array in S3 watermark object
- **Memory management**: Only loads blacklist when needed
- **Scalability**: Tested with 10,000+ files

---

## 📚 **Additional Resources**

### **Documentation Library:**
- **`USER_MANUAL.md`** - Complete CLI command reference with examples
- **`WATERMARK_DEEP_DIVE.md`** - Technical implementation details and troubleshooting
- **`LARGE_TABLE_GUIDELINES.md`** - Best practices for tables with millions of rows
- **`README.md`** - Project overview and quick start guide

### **Configuration Examples:**
- **`config/pipelines/`** - Sample pipeline configurations for different use cases
- **`config/connections/`** - Database connection examples
- **Pipeline templates** - Ready-to-use configurations for common scenarios

### **Monitoring & Debugging:**
- **Log files**: `/tmp/sync_*.log` - Detailed execution logs
- **JSON metadata**: `/tmp/sync_*.json` - Structured execution results
- **Watermark inspection**: CLI commands for detailed state analysis
- **Performance metrics**: Built-in timing and throughput measurements

---

## 🆘 **Getting Help**

### **Self-Diagnosis Steps:**
1. **Check logs**: Look for specific error messages and stack traces
2. **Verify watermark**: Use `watermark get` to understand current state
3. **Test connectivity**: Ensure SSH tunnels and database connections work
4. **Review configuration**: Verify pipeline and connection configs are correct
5. **Check resources**: Monitor memory usage, disk space, and network connectivity

### **Escalation Criteria:**
- **Data integrity issues**: Suspected data loss or corruption
- **Performance degradation**: Sync times significantly increased
- **New error patterns**: Errors not covered in this Q&A
- **Configuration problems**: Unable to resolve connection or setup issues

### **Information to Gather:**
- **Error logs**: Complete stack traces and error messages
- **Watermark state**: Output from `watermark get` command
- **Environment details**: Pipeline configs, connection strings (sanitized)
- **Data volumes**: Table sizes, file counts, processing times
- **Recent changes**: Any configuration or environment changes

**Remember**: The system is designed with safety and reliability in mind. When in doubt, check the watermark state first to understand what the system is trying to accomplish, then work backwards to identify the issue.