# Watermark Management Deep Dive

🔖 **Comprehensive Guide to Watermark-Based Data Loss Prevention**

This document provides an in-depth technical explanation of how watermarks prevent data loss and ensure reliable incremental processing.

---

## 🧠 **Core Concept: What Are Watermarks?**

### **Definition**
A **watermark** is a persistent timestamp that represents the **last successfully processed data point** for a specific table. It acts as a resumable checkpoint in your data stream.

```
Watermark = "All data up to this timestamp has been safely processed"
Next processing starts from: WHERE update_at > watermark_timestamp
```

### **Visual Representation**

```
MySQL Table Timeline:
├─────────┬─────────┬─────────┬─────────┬─────────┬─────────┤
│ Jan 01  │ Feb 15  │ Mar 30  │ May 12  │ Jul 05  │ Aug 14  │
├─────────┴─────────┴─────────┼─────────┴─────────┴─────────┤
│   ✅ Already Processed      │🔖       🔄 To Be Processed   │
│                             │Watermark                     │
└─────────────────────────────┴──────────────────────────────┘

Watermark Position: 2025-05-12 14:30:00
Next Query: WHERE update_at > '2025-05-12 14:30:00'
```

---

## 🏗️ **Watermark Architecture**

### **Storage Location**
Watermarks are stored in **S3** as JSON files for:
- ✅ **Persistence**: Survives system restarts
- ✅ **Reliability**: Cloud-based storage
- ✅ **Accessibility**: Available across systems
- ✅ **Backup**: Automatic S3 versioning

### **File Structure**
```
s3://your-bucket/watermarks/
├── settlement.settlement_normal_delivery_detail.json
├── settlement.settlement_claim_detail.json
├── settlement.partner_info.json
└── ...
```

### **Watermark JSON Structure**
```json
{
  "table_name": "settlement.settlement_normal_delivery_detail",
  "last_mysql_extraction_time": "2025-08-14T16:45:33.123456Z",
  "last_mysql_data_timestamp": "2025-08-14T15:30:22Z",
  "mysql_rows_extracted": 1250000,
  "mysql_status": "success",
  "last_redshift_load_time": "2025-08-14T17:12:45.654321Z", 
  "redshift_rows_loaded": 1250000,
  "redshift_status": "success",
  "backup_strategy": "sequential",
  "s3_file_count": 250,
  "processed_s3_files": [
    "s3://bucket/path/file1.parquet",
    "s3://bucket/path/file2.parquet"
  ],
  "last_error": null,
  "created_at": "2025-08-14T20:58:18.585905Z",
  "updated_at": "2025-08-14T22:45:33.970251Z",
  "metadata": {
    "manual_watermark": false,
    "total_processing_time_seconds": 4320,
    "average_batch_size": 5000
  }
}
```

**New Field Added (August 2025):**
- **`processed_s3_files`**: List of S3 files already loaded to Redshift
- **Purpose**: Prevents duplicate loading of the same S3 files
- **Impact**: Eliminates duplicate data in incremental syncs

---

## 🛡️ **Data Loss Prevention Mechanisms**

### **1. Atomic Updates**

**Problem Without Atomic Updates:**
```python
# ❌ DANGEROUS: Non-atomic approach
def process_batch_unsafe(batch_data):
    # Update watermark BEFORE processing
    watermark_manager.update(table_name, max_timestamp)  # ❌ Too early!
    
    # If this fails, watermark is wrong but data wasn't processed
    upload_success = s3_manager.upload(batch_data)  # ❌ Could fail
```

**Solution With Atomic Updates:**
```python
# ✅ SAFE: Atomic approach
def process_batch_safe(batch_data):
    try:
        # 1. Process data first
        upload_success = s3_manager.upload(batch_data)
        
        # 2. ONLY update watermark after confirmed success
        if upload_success:
            max_timestamp = get_max_timestamp(batch_data)
            watermark_manager.update(table_name, max_timestamp)  # ✅ Safe!
            return True
        else:
            # ❌ Upload failed - watermark unchanged
            # Next run will retry this exact batch
            return False
            
    except Exception as e:
        # ❌ Any failure - watermark stays at last safe point
        # System will retry from last known good state
        log.error(f"Batch failed: {e}")
        return False
```

### **🐛 Critical Bug Fixes Applied (August 2025)**

**Fixed Watermark Timestamp Calculation:**
```python
# ❌ BEFORE (Bug): Used MAX from ALL data in time range
def get_watermark_timestamp_old(table_name, last_watermark):
    query = f"""
    SELECT MAX(update_at) 
    FROM {table_name} 
    WHERE update_at > '{last_watermark}' 
    AND update_at <= NOW()
    """
    # Problem: Returns future timestamps not yet extracted
    
# ✅ AFTER (Fixed): Uses MAX from ONLY extracted rows
def get_watermark_timestamp_fixed(table_name, last_watermark, limit):
    query = f"""
    SELECT MAX(update_at) as max_update_at 
    FROM (
        SELECT update_at
        FROM {table_name} 
        WHERE update_at > '{last_watermark}' 
        ORDER BY update_at, ID
        LIMIT {limit}
    ) as extracted_rows
    """
    # Solution: Watermark reflects actual last processed row
```

**Impact of Fix:**
- ✅ **Accurate Watermarks**: Now reflects actual data extracted
- ✅ **Predictable Progression**: Watermark advances with actual work done
- ✅ **No Data Gaps**: Prevents skipping unprocessed data

### **2. Dual-Stage Tracking**

The system tracks **two separate stages** to prevent data loss:

```json
{
  // Stage 1: MySQL → S3
  "last_mysql_data_timestamp": "2025-08-14T15:30:22Z",
  "mysql_status": "success",
  "mysql_rows_extracted": 1250000,
  
  // Stage 2: S3 → Redshift  
  "last_redshift_load_time": "2025-08-14T17:12:45Z",
  "redshift_status": "success", 
  "redshift_rows_loaded": 1250000
}
```

**Why Dual-Stage Matters:**
```
Scenario: S3 upload succeeds, Redshift loading fails

Without dual-stage:
┌─────────────────────────────────────────────────────────┐
│ ❌ Single watermark updated after S3 upload             │
│ ❌ Redshift load fails                                  │
│ ❌ Next run skips this data (thinks it's in Redshift)  │
│ ❌ DATA LOST in Redshift!                              │
└─────────────────────────────────────────────────────────┘

With dual-stage:
┌─────────────────────────────────────────────────────────┐
│ ✅ MySQL→S3 watermark updated (data in S3)             │
│ ❌ Redshift load fails                                  │
│ ✅ Redshift watermark unchanged                         │
│ ✅ Next run: skip backup, retry Redshift load          │
│ ✅ NO data loss!                                       │
└─────────────────────────────────────────────────────────┘
```

### **3. Per-Table Granularity**

Each table maintains **independent watermarks**:

```bash
# Different tables can have different progress
Table A: last_processed = 2025-08-14 10:30:00  (recently updated)
Table B: last_processed = 2025-08-10 15:45:00  (older data)
Table C: last_processed = 2025-08-14 12:00:00  (medium freshness)
```

**Benefits:**
- ✅ **Isolated Failures**: One table failure doesn't affect others
- ✅ **Independent Processing**: Tables can be synced separately
- ✅ **Granular Recovery**: Fix specific tables without global reset
- ✅ **Performance Optimization**: Focus on tables that need updates

---

## 🔄 **Watermark Lifecycle**

### **1. Initialization**
```bash
# New table - no watermark exists
python -m src.cli.main watermark get -t new_table
# Result: "No watermark found, will start from default: 2025-01-01 00:00:00"

# Set initial watermark
python -m src.cli.main watermark set -t new_table --timestamp '2025-08-01 00:00:00'
```

### **2. Processing Cycle**
```
1. Read current watermark: '2025-08-01 00:00:00'
   ↓
2. Generate query: WHERE update_at > '2025-08-01 00:00:00'
   ↓  
3. Process batches sequentially:
   - Batch 1: rows with update_at 2025-08-01 00:00:01 to 2025-08-01 05:30:00
   - Batch 2: rows with update_at 2025-08-01 05:30:01 to 2025-08-01 11:15:00
   - Batch 3: rows with update_at 2025-08-01 11:15:01 to 2025-08-01 16:45:00
   ↓
4. Update watermark after each successful batch:
   - After Batch 1: watermark = '2025-08-01 05:30:00'
   - After Batch 2: watermark = '2025-08-01 11:15:00'  
   - After Batch 3: watermark = '2025-08-01 16:45:00'
   ↓
5. Next sync starts from: WHERE update_at > '2025-08-01 16:45:00'
```

### **3. Error Recovery**
```
Normal processing:
Batch 1 ✅ → Watermark: 2025-08-01 05:30:00
Batch 2 ✅ → Watermark: 2025-08-01 11:15:00
Batch 3 ❌ → Watermark: UNCHANGED (still 2025-08-01 11:15:00)

Recovery processing:
Next sync reads watermark: 2025-08-01 11:15:00
Generates query: WHERE update_at > '2025-08-01 11:15:00'
Retries failed batch and continues...
```

---

## 🔧 **Watermark Operations**

### **Manual Watermark Management**

#### **1. View Current Watermark**
```bash
python -m src.cli.main watermark get -t settlement.normal_delivery_detail

# Output:
📅 Current Watermark for settlement.normal_delivery_detail:

   🔄 MySQL → S3 Backup Stage:
      Status: success
      Rows Extracted: 1,250,000
      S3 Files Created: 250
      Last Data Timestamp: 2025-08-14T15:30:22Z
      Last Extraction Time: 2025-08-14T16:45:33Z

   📊 S3 → Redshift Loading Stage:
      Status: success
      Rows Loaded: 1,250,000
      Last Load Time: 2025-08-14T17:12:45Z

   🔜 Next Incremental Backup:
      Will start from: 2025-08-14 15:30:22
```

#### **2. Set Watermark for Fresh Sync**
```bash
# Reset to start fresh from specific date
python -m src.cli.main watermark set -t table_name --timestamp '2025-08-01 00:00:00'

# Set to recent time for catching up
python -m src.cli.main watermark set -t table_name --timestamp '2025-08-10 20:00:00'
```

#### **3. Reset Watermark**
```bash
# Complete reset (back to default: 2025-01-01 00:00:00)
python -m src.cli.main watermark reset -t table_name

# Reset specific stage
python -m src.cli.main watermark reset -t table_name --mysql-only
python -m src.cli.main watermark reset -t table_name --redshift-only
```

#### **4. List All Watermarks**
```bash
python -m src.cli.main watermark list

# Output shows all tables and their current watermarks
```

### **Automated Watermark Updates**

During sync operations, watermarks are updated automatically:

```python
# Simplified update logic
def update_watermarks_after_batch(batch_data, table_name):
    # Find the maximum timestamp in this batch
    max_timestamp = max(row['update_at'] for row in batch_data)
    
    # Update watermark only after successful S3 upload
    success = watermark_manager.update_mysql_watermark(
        table_name=table_name,
        extraction_time=datetime.now(),
        max_data_timestamp=max_timestamp,
        rows_extracted=len(batch_data),
        status='success',
        backup_strategy='sequential',
        s3_file_count=batch_id
    )
    
    return success
```

---

## 🎯 **Watermark Strategies for Different Scenarios**

### **1. Fresh Complete Sync**
```bash
# Scenario: Sync entire table from beginning
python -m src.cli.main s3clean clean -t table_name
python -m src.cli.main watermark reset -t table_name
python -m src.cli.main sync -t table_name
```

### **2. Catch-up from Specific Date**
```bash
# Scenario: Sync data from last month
python -m src.cli.main watermark set -t table_name --timestamp '2025-07-01 00:00:00'
python -m src.cli.main sync -t table_name
```

### **3. Incremental Daily Sync**
```bash
# Scenario: Daily sync (watermark automatically managed)
# Day 1: Watermark starts at 2025-08-01, syncs to 2025-08-02
python -m src.cli.main sync -t table_name

# Day 2: Watermark starts at 2025-08-02, syncs to 2025-08-03  
python -m src.cli.main sync -t table_name

# Day 3: Continues automatically...
python -m src.cli.main sync -t table_name
```

### **4. Resume Interrupted Sync**
```bash
# Scenario: Large sync was interrupted
# Check where it stopped
python -m src.cli.main watermark get -t table_name

# Simply run sync again - it resumes automatically
python -m src.cli.main sync -t table_name
```

### **5. Selective Time Range Sync**
```bash
# Scenario: Sync specific time range only

# Set start point
python -m src.cli.main watermark set -t table_name --timestamp '2025-08-10 00:00:00'

# Sync with backup-only to stop at S3
python -m src.cli.main sync -t table_name --backup-only

# Manually set end point (if needed) and load to Redshift
python -m src.cli.main sync -t table_name --redshift-only
```

---

## 🚨 **Troubleshooting Watermark Issues**

### **Common Watermark Problems**

#### **1. Watermark Not Updating**
**Symptoms:**
- Multiple syncs show same watermark timestamp
- No progress despite running backup

**Diagnosis:**
```bash
# Check watermark status
python -m src.cli.main watermark get -t table_name

# Check S3 files to see if data is being uploaded
python -m src.cli.main s3clean list -t table_name

# Run with debug to see detailed processing
python -m src.cli.main sync -t table_name --debug
```

**Solutions:**
```bash
# Check if query returns data
# Connect to MySQL and run:
SELECT COUNT(*) FROM table_name WHERE update_at > 'current_watermark';

# If no new data, watermark won't update (expected behavior)
# If data exists but watermark not updating, check for errors
```

#### **2. Watermark Corruption**
**Symptoms:**
- Invalid timestamp format
- Watermark in future
- Negative row counts

**Recovery:**
```bash
# Backup current watermark
python -m src.cli.main watermark get -t table_name > watermark_backup.json

# Reset to known good state
python -m src.cli.main watermark set -t table_name --timestamp '2025-08-10 15:00:00'

# Test with limited rows
python -m src.cli.main sync -t table_name --limit 1000
```

#### **3. Duplicate Data in Redshift**
**Symptoms:**
- Redshift row count higher than expected
- Same data appears multiple times

**Diagnosis:**
```sql
-- Check for duplicates in Redshift
SELECT update_at, COUNT(*) 
FROM table_name 
GROUP BY update_at 
HAVING COUNT(*) > 1;
```

**Solutions:**
```bash
# This usually indicates watermark was reset incorrectly
# Clean Redshift table and restart with proper watermark
# OR use Redshift UPSERT logic to handle duplicates
```

#### **4. Missing Data in Redshift**
**Symptoms:**
- Redshift row count lower than expected
- Gaps in timestamp sequence

**Diagnosis:**
```bash
# Check watermark progression
python -m src.cli.main watermark get -t table_name

# Check S3 files existence
python -m src.cli.main s3clean list -t table_name

# Verify MySQL source data
```

**Solutions:**
```bash
# Rerun sync to catch missing data
python -m src.cli.main sync -t table_name

# If still missing, reset watermark to earlier time
python -m src.cli.main watermark set -t table_name --timestamp 'earlier_time'
```

### **Watermark Validation**

#### **1. Consistency Checks**
```bash
# Verify watermark progression makes sense
python -m src.cli.main watermark get -t table_name

# Check that:
# - last_mysql_data_timestamp <= current time
# - mysql_rows_extracted > 0 (if data exists)
# - redshift_rows_loaded matches mysql_rows_extracted
# - Status fields are 'success' for completed operations
```

#### **2. Data Integrity Checks**
```bash
# Compare source vs destination
# MySQL count:
# SELECT COUNT(*) FROM table_name WHERE update_at > 'watermark_timestamp';

# Redshift count:  
# SELECT COUNT(*) FROM table_name;

# S3 file count should match batches processed
python -m src.cli.main s3clean list -t table_name | wc -l
```

---

## 📊 **Watermark Performance Optimization**

### **Watermark Update Frequency**

**Per-Batch Updates (Current Approach):**
```
✅ Pros: Fine-grained recovery, minimal data loss
❌ Cons: More S3 API calls for watermark updates
```

**Batch-Group Updates (Alternative):**
```
✅ Pros: Fewer S3 API calls, better performance
❌ Cons: Larger recovery window if interrupted
```

### **Watermark Storage Optimization**

**Current: Individual JSON Files**
```
s3://bucket/watermarks/table1.json
s3://bucket/watermarks/table2.json
s3://bucket/watermarks/table3.json
```

**Alternative: Consolidated Storage**
```
s3://bucket/watermarks/all_tables.json
{
  "table1": {...},
  "table2": {...},
  "table3": {...}
}
```

### **Watermark Caching**

The system implements intelligent caching:
```python
# Watermarks cached in memory during sync
# Only written to S3 when changed
# Reduces S3 API calls and improves performance
```

---

## 🔗 **Integration with Backup Strategies**

### **Sequential Strategy + Watermarks**
```
Perfect fit:
- Sequential processing matches linear watermark progression
- Simple to track and debug
- Optimal for most use cases
```

### **Inter-table Strategy + Watermarks**  
```
Works well:
- Each table has independent watermark
- Parallel processing of different tables
- No watermark conflicts
```

### **Intra-table Strategy + Watermarks (Disabled)**
```
Problematic:
- Complex chunk boundary interactions
- Potential gaps in watermark progression  
- Data loss risks due to boundary bugs
- This is why intra-table strategy was disabled
```

---

## 📚 **Advanced Watermark Patterns**

### **1. Backfill Pattern**
```bash
# Sync recent data first (high priority)
python -m src.cli.main watermark set -t table --timestamp '2025-08-01 00:00:00'
python -m src.cli.main sync -t table

# Then backfill historical data
python -m src.cli.main watermark set -t table --timestamp '2025-01-01 00:00:00'  
python -m src.cli.main sync -t table --backup-only
```

### **2. Staged Loading Pattern**
```bash
# Stage 1: Backup everything to S3
python -m src.cli.main sync -t table --backup-only

# Stage 2: Load to Redshift in chunks
python -m src.cli.main sync -t table --redshift-only
```

### **3. Validation Pattern**
```bash
# Sync with verification
python -m src.cli.main sync -t table --verify-data

# Check watermark consistency
python -m src.cli.main watermark get -t table

# Validate row counts match expectations
```

---

## 🎯 **Best Practices Summary**

### **Watermark Management DO's ✅**

1. **✅ Monitor Watermark Progress** - Check regularly during long syncs
2. **✅ Understand Dual-Stage Tracking** - MySQL→S3 vs S3→Redshift
3. **✅ Use Manual Watermarks for Fresh Syncs** - Set specific start points
4. **✅ Backup Watermarks Before Major Changes** - Save current state
5. **✅ Verify Data Integrity** - Compare source vs destination counts
6. **✅ Trust the Resume Capability** - System designed for interruptions
7. **✅ Use Appropriate Granularity** - Per-table watermarks are optimal

### **Watermark Management DON'Ts ❌**

1. **❌ Don't Manually Edit Watermark Files** - Use CLI commands
2. **❌ Don't Skip Watermark Verification** - Always check after major operations
3. **❌ Don't Reset Watermarks Unnecessarily** - Can cause data duplication
4. **❌ Don't Ignore Watermark Errors** - Address issues immediately
5. **❌ Don't Assume Watermarks are Perfect** - Validate with data checks
6. **❌ Don't Mix Manual and Automatic Updates** - Choose one approach
7. **❌ Don't Delete Watermark Files Directly** - Use reset commands

---

*🤖 Generated with [Claude Code](https://claude.ai/code) - Technical deep dive into watermark-based data loss prevention*