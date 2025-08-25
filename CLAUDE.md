# S3 to Redshift Incremental Backup System

🎉 **PRODUCTION READY** - A fully operational Python application for incremental data backup from MySQL to S3 and Redshift, successfully migrated from Google Colab prototype with enterprise architecture, comprehensive testing, and verified deployment capabilities.

## ⚠️ **CRITICAL TESTING RULES - NEVER VIOLATE**

### 🚨 **MANDATORY VERIFICATION PROTOCOLS**
1. **Redshift Connection**: ALWAYS use SSH tunnel (configuration in `.env`)
2. **Data Verification**: MUST verify actual row counts in Redshift tables  
3. **No False Positives**: Do NOT report "success" based on log messages alone
4. **Real Testing Required**: Test with actual data movement verification
5. **Document Failures**: Log all failures and partial successes accurately

### 🔍 **Required Verification Steps Before Claiming Success**
```bash
# 1. Connect to Redshift via SSH tunnel using .env configuration
# 2. Query actual row count in target table
SELECT COUNT(*) FROM target_table;
# 3. Verify data actually exists  
SELECT * FROM target_table LIMIT 5;
# 4. Compare source vs target row counts
# 5. Only then report success/failure accurately
```

### 🚫 **FORBIDDEN PRACTICES** 
- ❌ Reporting success based on "✅ Loaded successfully" log messages
- ❌ Assuming sync worked without checking Redshift row counts  
- ❌ Using direct connection instead of SSH tunnel
- ❌ Claiming testing passed without actual data verification
- ❌ Making assumptions about data movement without proof

### 🧪 **Testing Memories**
- Please test with real env when the change impact the core functionality
- Please run unit and integrate test after major code change

## Project Overview

This system implements two backup strategies:
- **Sequential Backup**: Process tables one by one (recommended for most use cases)
- **Inter-table Parallel**: Process multiple tables simultaneously

## Complete MySQL → S3 → Redshift Pipeline

**PRODUCTION CAPABILITY**: Full end-to-end sync with dynamic schema discovery

### Sync Command Features
- **Full Pipeline**: `python -m src.cli.main sync -t table_name` (MySQL → S3 → Redshift)
- **Backup Only**: `--backup-only` flag (MySQL → S3)  
- **Redshift Only**: `--redshift-only` flag (S3 → Redshift)
- **Testing Control**: `--limit N` flag to limit rows per query (for development/testing)
- **Dynamic Schema**: Automatic schema discovery for any table structure
- **Direct Parquet Loading**: Uses `FORMAT AS PARQUET` for efficient Redshift loading
- **Comprehensive Logging**: Detailed progress tracking and error reporting

### Watermark Management System

**ENHANCED WATERMARK CAPABILITIES** - Now supports both automated and manual watermark management:

#### Automated Watermark Tracking
- **S3-based Storage**: Watermarks stored in S3 for reliability and persistence
- **Table-level Granularity**: Individual watermarks per table for precise incremental processing
- **Dual-stage Tracking**: Separate watermarks for MySQL→S3 and S3→Redshift stages
- **Atomic Updates**: Prevents race conditions and ensures consistency
- **Error Recovery**: Backup and restore capabilities for watermark corruption

#### Manual Watermark Control (NEW)
- **CLI Commands**: `watermark get|set|reset|list` for manual watermark management
- **Fresh Sync Support**: Reset and set starting timestamps for complete fresh syncs
- **Selective Loading**: Load only S3 files created after specific timestamps
- **Manual Override**: Override automated watermarks for custom sync scenarios
- **Row Count Management**: `watermark_count set-count|validate-counts` for fixing discrepancies

#### Watermark Architecture
```
Watermark Components:
├── last_mysql_data_timestamp     # Data cutoff point for MySQL extraction
├── last_mysql_extraction_time    # When backup process ran
├── mysql_status                  # pending/success/failed
├── redshift_status              # pending/success/failed  
├── backup_strategy              # sequential/inter-table/manual_cli
└── metadata                     # Additional tracking information
```

#### Common Watermark Workflows

**1. Fresh Sync from Specific Date:**
```bash
python -m src.cli.main watermark reset -t settlement.table_name
python -m src.cli.main watermark set -t settlement.table_name --timestamp '2025-08-09 20:00:01'
python -m src.cli.main sync -t settlement.table_name
```

**2. Load Existing S3 Files After Date:**
```bash
python -m src.cli.main watermark reset -t settlement.table_name  
python -m src.cli.main watermark set -t settlement.table_name --timestamp '2025-08-09 20:00:01'
python -m src.cli.main sync -t settlement.table_name --redshift-only
```

**3. Check Current Watermark Status:**
```bash
# Basic watermark information
python -m src.cli.main watermark get -t settlement.table_name

# Show processed S3 files list
python -m src.cli.main watermark get -t settlement.table_name --show-files
```

**4. Fix Watermark Row Count Discrepancies:**
```bash
# Validate watermark counts against actual Redshift data
python -m src.cli.main watermark-count validate-counts -t settlement.table_name

# Fix inflated backup counts (set absolute count)
python -m src.cli.main watermark-count set-count -t settlement.table_name --count 3000000 --mode absolute

# Add incremental count (if needed)  
python -m src.cli.main watermark-count set-count -t settlement.table_name --count 500000 --mode additive
```

### Filtering Logic
- **Session-based**: Files within time window around backup extraction time (±10 hours)
- **Watermark-based**: Files created after manually set data timestamp
- **Manual Priority**: Manual watermarks (`backup_strategy='manual_cli'`) take precedence
- **Timezone Handling**: Wide session windows handle timezone differences and processing delays

## S3 Storage Management (s3clean)

**PRODUCTION-READY S3 CLEANUP SYSTEM** - Enterprise-grade storage management with comprehensive safety features.

### S3Clean Commands
```bash
# Safe exploration and preview
python -m src.cli.main s3clean list -t table_name
python -m src.cli.main s3clean list -t table_name --show-timestamps  # Detailed timestamps
python -m src.cli.main s3clean clean -t table_name --dry-run

# Targeted cleanup operations  
python -m src.cli.main s3clean clean -t table_name
python -m src.cli.main s3clean clean -t table_name --older-than "7d"
python -m src.cli.main s3clean clean -t table_name --pattern "batch_*"

# System-wide cleanup (use with caution)
python -m src.cli.main s3clean clean-all --older-than "30d"
```

### Safety Features
- **Multi-layer Protection**: Dry-run preview, confirmation prompts, table validation
- **Time-based Filtering**: Only clean files older than specified age (`7d`, `24h`, `30m`)
- **Pattern Matching**: Target specific file patterns for selective cleanup
- **Table Isolation**: Prevents accidental deletion across wrong tables
- **Size Reporting**: Shows exactly how much space will be freed

### Common S3Clean Workflows

**1. Storage Maintenance:**
```bash
# Check current storage usage
python -m src.cli.main s3clean list -t settlement.table_name

# Clean old backup files (recommended)
python -m src.cli.main s3clean clean -t settlement.table_name --older-than "7d"
```

**2. Emergency Cleanup:**
```bash
# Preview cleanup before execution
python -m src.cli.main s3clean clean -t settlement.table_name --dry-run

# Force cleanup without prompts (for automation)
python -m src.cli.main s3clean clean -t settlement.table_name --force
```

## Architecture Features

### Enterprise Security
- **Credential Protection**: Comprehensive sanitization of logs and outputs
- **SSH Tunnel Support**: Secure Redshift connections via bastion hosts
- **Environment Isolation**: Separate configurations for different environments

### Performance Optimizations
- **Parallel Processing**: Multiple backup strategies for different use cases
- **Efficient Data Formats**: Direct Parquet loading to Redshift
- **Incremental Processing**: Watermark-based change detection
- **Resource Management**: Configurable batch sizes and connection pooling
- **Testing Controls**: Optional row limits for development and testing scenarios

### Operational Excellence
- **Comprehensive Logging**: Detailed progress tracking and error reporting
- **Error Recovery**: Robust handling of network failures and partial loads
- **Monitoring Support**: Integration with external monitoring systems
- **Automated Testing**: Full test coverage with CI/CD integration

---

## 📚 **Documentation Library**

### **📊 Production Guides**
- **`LARGE_TABLE_GUIDELINES.md`** - Complete guidelines for backing up large tables (1M+ rows)
- **`WATERMARK_DEEP_DIVE.md`** - Technical deep-dive into watermark-based data loss prevention  
- **`ORPHANED_FILES_HANDLING.md`** - Managing intermediate files during resume operations

### **🔧 User References**
- **`USER_MANUAL.md`** - Comprehensive CLI usage guide
- **`WATERMARK_CLI_GUIDE.md`** - Watermark management commands
- **`README.md`** - Project overview and quick start

### **🚀 Recommended Reading Path**
1. **Basic Usage**: `USER_MANUAL.md` 
2. **Large Tables**: `LARGE_TABLE_GUIDELINES.md`
3. **Advanced Topics**: `WATERMARK_DEEP_DIVE.md`

---

## 🐛 **CRITICAL BUG FIXES & LESSONS LEARNED**

### **P0 WATERMARK DOUBLE-COUNTING BUG (RESOLVED)**

**Issue Discovered (August 25, 2025)**: Critical watermark row count discrepancy
- **Symptoms**: Watermark showing 3M extracted rows vs 2.5M loaded, while Redshift contains 3M actual rows
- **Root Cause**: Additive watermark logic causing double-counting in multi-session backups
- **Impact**: Inflated extraction counts, confusion about actual data processing

#### **Technical Root Cause**
```python
# OLD BUGGY BEHAVIOR (FIXED):
# Session 1: 500K rows → watermark = 500K
# Session 2: 2.5M session total → watermark = 500K + 2.5M = 3M
# But if Session 2 already included Session 1 data internally → DOUBLE COUNTING

# NEW FIXED BEHAVIOR:
# Uses session ID tracking and mode control to prevent double-counting
# mode='auto' intelligently detects same-session vs cross-session updates
```

#### **Comprehensive Fix Implemented**
1. **Mode-Controlled Watermark Updates** (`src/core/s3_watermark_manager.py`)
   - `mode='absolute'`: Replace existing count (same session updates)
   - `mode='additive'`: Add to existing count (different session updates) 
   - `mode='auto'`: Automatic detection based on session IDs

2. **Session Tracking System**
   - Unique session IDs prevent intra-session double-counting
   - Cross-session accumulation works correctly for incremental processing

3. **CLI Count Management Commands**
   ```bash
   # Immediate fix for discrepancies
   python -m src.cli.main watermark-count set-count -t table_name --count N --mode absolute
   
   # Validation against actual Redshift data
   python -m src.cli.main watermark-count validate-counts -t table_name
   ```

4. **Updated Backup Strategies** (`src/backup/row_based.py:815-886`)
   - Session-controlled final watermark updates
   - Prevents legacy additive double-counting bugs

#### **Critical Lessons Learned**

**🔴 WATERMARK INTEGRITY PRINCIPLES**
1. **Session Isolation**: Same backup session should NEVER double-count rows
2. **Cross-Session Accumulation**: Different sessions should ADD incremental rows
3. **Validation is Mandatory**: Always compare watermark vs actual Redshift counts
4. **Mode Awareness**: Be explicit about additive vs replacement updates

**🔴 TESTING REQUIREMENTS** 
1. **Multi-Session Testing**: Test backup resumption across different sessions
2. **Count Validation**: Verify watermark counts match actual data movement  
3. **Edge Case Coverage**: Test scenarios like failed/resumed/partial syncs
4. **Production Verification**: Always validate fixes with actual production tables

**🔴 DEBUGGING METHODOLOGY**
1. **Symptom Recognition**: Row count mismatches indicate counting logic bugs
2. **Session Analysis**: Track which sessions contributed to watermark counts
3. **Direct Validation**: Query actual Redshift data to establish ground truth
4. **Fix Verification**: Test fixes with multiple session scenarios

#### **Prevention Measures**
- **Comprehensive Testing**: All watermark updates now tested with session scenarios
- **CLI Validation Tools**: Built-in commands to detect and fix count discrepancies  
- **Documentation**: Clear guidance on watermark count management
- **Code Reviews**: Enhanced review process for watermark-related code changes

#### **User Recovery Process**
```bash
# Step 1: Validate current state
python -m src.cli.main watermark-count validate-counts -t your_table_name

# Step 2: Fix discrepancy with actual Redshift count
python -m src.cli.main watermark-count set-count -t your_table_name --count ACTUAL_COUNT --mode absolute

# Step 3: Verify fix
python -m src.cli.main watermark get -t your_table_name
```

**Status**: ✅ **RESOLVED** - All fixes tested and validated. Future backups protected against double-counting.