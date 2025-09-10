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
- **Full Pipeline**: `python -m src.cli.main sync pipeline -p pipeline -t table` (MySQL → S3 → Redshift)
- **Backup Only**: `--backup-only` flag (MySQL → S3)  
- **Redshift Only**: `--redshift-only` flag (S3 → Redshift)
- **Row Control**: `--limit N --max-chunks M` for precise row count control (N × M total rows)
- **Dynamic Schema**: Automatic schema discovery for any table structure
- **Direct Parquet Loading**: Uses `FORMAT AS PARQUET` for efficient Redshift loading
- **Comprehensive Logging**: Detailed progress tracking and error reporting

### Precise Row Count Control

**ENHANCED CLI CONTROL** - Use `--limit` and `--max-chunks` together for exact row counts:

```bash
# Formula: Total Rows = --limit × --max-chunks

# Example: Extract exactly ~2.1M rows
python -m src.cli.main sync pipeline -p us_dw_hybrid_v1_2 -t settlement.settle_orders --limit 75000 --max-chunks 29
# Result: 29 chunks × 75,000 rows = 2,175,000 rows

# Example: Extract exactly ~2.1M rows with smaller chunks  
python -m src.cli.main sync pipeline -p us_dw_hybrid_v1_2 -t settlement.settle_orders --limit 10000 --max-chunks 211
# Result: 211 chunks × 10,000 rows = 2,110,000 rows
```

**Key Points**:
- Without `--max-chunks`, backup continues until no more data (can extract more than intended)
- With `--max-chunks`, both backup and load stages process the same amount of data
- Default chunk size is 75,000 rows if `--limit` not specified

### Watermark Management System

**SIMPLIFIED WATERMARK SYSTEM (v2.1)** - Enhanced reliability with absolute count tracking:

#### Core Principles
- **Absolute Counts Only**: No accumulation across sessions, eliminates double-counting bugs
- **Session-Based**: Each sync shows exactly how many rows IT processed
- **Predictable Reset**: Always zeros all counts completely
- **Simple Logic**: No complex modes or accumulation detection

#### Watermark Commands
- **CLI Commands**: `watermark get|set|reset|list` for watermark management
- **Fresh Sync Support**: Reset and set starting timestamps for complete fresh syncs
- **Row Count Management**: `watermark-count set-count|validate-counts` for fixing discrepancies

#### Watermark Architecture
```
Watermark Components:
├── last_mysql_data_timestamp     # Data cutoff point for MySQL extraction
├── last_mysql_extraction_time    # When backup process ran
├── mysql_status                  # pending/success/failed
├── mysql_rows_extracted          # Absolute count from current session
├── redshift_status              # pending/success/failed  
├── redshift_rows_loaded         # Absolute count loaded to Redshift
├── backup_strategy              # sequential/inter-table/manual_cli
└── processed_files              # S3 files successfully loaded (blacklist)
```

#### Common Watermark Workflows

**1. Fresh Sync from Specific Date:**
```bash
python -m src.cli.main watermark reset -p us_dw_hybrid_v1_2 -t settlement.settle_orders
python -m src.cli.main watermark set -p us_dw_hybrid_v1_2 -t settlement.settle_orders --timestamp '2025-08-09 20:00:01'
python -m src.cli.main sync pipeline -p us_dw_hybrid_v1_2 -t settlement.settle_orders --limit 75000 --max-chunks 29
```

**2. Load Existing S3 Files After Date:**
```bash
python -m src.cli.main watermark reset -p us_dw_hybrid_v1_2 -t settlement.settle_orders  
python -m src.cli.main watermark set -p us_dw_hybrid_v1_2 -t settlement.settle_orders --timestamp '2025-08-09 20:00:01'
python -m src.cli.main sync pipeline -p us_dw_hybrid_v1_2 -t settlement.settle_orders --redshift-only
```

**3. Check Current Watermark Status:**
```bash
# Basic watermark information
python -m src.cli.main watermark get -p us_dw_hybrid_v1_2 -t settlement.settle_orders

# Show processed S3 files list
python -m src.cli.main watermark get -p us_dw_hybrid_v1_2 -t settlement.settle_orders --show-files
```

**4. Fix Watermark Row Count Issues:**
```bash
# Validate watermark counts against actual Redshift data
python -m src.cli.main watermark-count validate-counts -t settlement.settle_orders

# Fix count discrepancies with absolute count
python -m src.cli.main watermark-count set-count -t settlement.settle_orders --count 3000000 --mode absolute
```

## S3 Storage Management (s3clean)

**PRODUCTION-READY S3 CLEANUP SYSTEM** - Enterprise-grade storage management with comprehensive safety features.

### S3Clean Commands
```bash
# Safe exploration and preview
python -m src.cli.main s3clean list -t table_name
python -m src.cli.main s3clean clean -t table_name --dry-run

# Targeted cleanup operations  
python -m src.cli.main s3clean clean -t table_name --older-than "7d"
python -m src.cli.main s3clean clean-all --older-than "30d" --force
```

### Safety Features
- **Multi-layer Protection**: Dry-run preview, confirmation prompts, table validation
- **Time-based Filtering**: Only clean files older than specified age
- **Pattern Matching**: Target specific file patterns for selective cleanup
- **Size Reporting**: Shows exactly how much space will be freed

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

### Operational Excellence
- **Comprehensive Logging**: Detailed progress tracking and error reporting
- **Error Recovery**: Robust handling of network failures and partial loads
- **Monitoring Support**: Integration with external monitoring systems
- **Automated Testing**: Full test coverage with CI/CD integration

---

## 🐛 **CRITICAL BUG FIXES & LESSONS LEARNED**

### **P0 WATERMARK ROW COUNT ACCUMULATION BUG (RESOLVED - Sep 10, 2025)**

**Issue**: Critical watermark row count accumulation causing backup/load discrepancies
- **Symptoms**: Backup showed 32.5M rows extracted, but only 2.1M rows loaded to Redshift
- **Root Cause**: Complex mode-based accumulation logic (auto/absolute/additive) was unreliable
- **Impact**: Confusing row count displays, difficulty debugging sync operations

#### **Technical Root Cause**
```python
# DANGEROUS ARCHITECTURE (ELIMINATED):
# ❌ Complex mode detection: auto/absolute/additive
# ❌ Cross-session accumulation: old_count + new_count
# ❌ Incomplete reset: preserved old counts during reset
# Result: 30.45M (old) + 2.1M (new) = 32.5M (wrong total)
```

#### **Comprehensive Fix Implemented**
1. **Eliminated All Mode Logic**
   - Removed auto/absolute/additive mode detection
   - Simplified to always use absolute counts
   - No more complex session tracking or accumulation

2. **Fixed Reset to Zero All Counts**
   ```python
   def _create_default_watermark(self, table_name: str):
       return {
           'mysql_state': {'total_rows': 0},  # ALWAYS 0 on reset
           'redshift_state': {'total_rows': 0}  # ALWAYS 0 on reset
       }
   ```

3. **Simplified Update Logic**
   ```python
   def update_mysql_state(self, rows_extracted=None):
       # Simple rule: if provided, use it; otherwise keep existing
       if rows_extracted is not None:
           total_rows = rows_extracted  # Always absolute
   ```

#### **Critical Lessons Learned**

**🔴 WATERMARK DESIGN PRINCIPLES**
1. **KISS Principle**: Simple absolute counts are more reliable than complex modes
2. **Session Isolation**: Each sync session should report its own totals only
3. **Complete Reset**: Reset must zero ALL count fields, not preserve any
4. **Predictable Behavior**: Watermark should always show exactly what was provided

**🔴 CLI PARAMETER CLARITY**
1. **Precise Control**: Use `--limit` + `--max-chunks` for exact row count control
2. **Both Stages Match**: Proper parameters ensure backup and load process same amount
3. **Formula Understanding**: Total Rows = --limit × --max-chunks

#### **User Recovery Process**
```bash
# The fix is already deployed - no user action required
# For precise row control going forward:
python -m src.cli.main sync pipeline -p pipeline -t table --limit 75000 --max-chunks 29
# This processes exactly 2,175,000 rows in both backup and load stages
```

**Status**: ✅ **RESOLVED** - All watermark count accumulation issues eliminated. Simple absolute counting implemented.

### **P0 SCHEMA ARCHITECTURE BUG (RESOLVED)**

**Issue Discovered (August 27, 2025)**: Critical three-way schema management conflict causing production Parquet compatibility errors
- **Symptoms**: `Error: Redshift Spectrum error about incompatible Parquet schema for columns`
- **Root Cause**: Three conflicting schema discovery systems with different precision handling
- **Impact**: Production pipeline failures, data loading errors, inconsistent schema interpretation

#### **Technical Root Cause**
```python
# DANGEROUS ARCHITECTURE (ELIMINATED):
# ❌ Hardcoded Schemas (config/schemas.py) → Validation System  
# ❌ DynamicSchemaManager (different precision) → Redshift Loading
# ❌ FlexibleSchemaManager → Backup Strategies
# Result: DECIMAL(10,4) vs DECIMAL(15,4) vs different conversion = Parquet conflicts
```

#### **Comprehensive Fix Implemented**
1. **Schema System Unification**
   - Migrated all components to unified `FlexibleSchemaManager`
   - Eliminated hardcoded schema dependencies in validation system
   - Fixed Redshift loader to use consistent schema discovery
   - **REMOVED DynamicSchemaManager entirely** - no longer exists in codebase
   - Protected legacy functions with runtime warnings

2. **API Consistency Fix**
   ```python
   # FIXED: Correct FlexibleSchemaManager API usage
   pyarrow_schema, redshift_ddl = schema_manager.get_table_schema(table_name)
   # Was incorrectly assuming: schema_info.pyarrow_schema (caused tuple errors)
   ```

**Status**: ✅ **RESOLVED** - All components unified under single schema system. Production Parquet compatibility issues eliminated.

### **P0 VARCHAR LENGTH & COLUMN NAMING BUG (RESOLVED)**

**Issue Discovered (August 31, 2025)**: MySQL VARCHAR data exceeding declared lengths and Redshift-incompatible column names causing production loading failures
- **Symptoms**: `Redshift Spectrum Scan Error` with VARCHAR length violations, "Column name is invalid" for columns starting with numbers
- **Root Cause**: MySQL utf8mb4 charset allows data exceeding VARCHAR declarations; Redshift strictly enforces length limits and column naming rules
- **Impact**: Production pipeline failures, data loading errors, sync interruptions

#### **Comprehensive Fix Implemented**
1. **VARCHAR Length Safety Buffer** - Automatic doubling of VARCHAR sizes for safety
2. **Column Name Sanitization** - Numeric-start columns renamed (e.g., `190_time` → `col_190_time`)
3. **Persistent Column Mapping System** - Automatic detection and reuse of transformations

**Status**: ✅ **RESOLVED** - All schema compatibility issues resolved automatically. Production pipelines handle VARCHAR length and column naming transparently.

---

## 📚 **Documentation Library**

### **📊 Production Guides**
- **`LARGE_TABLE_GUIDELINES.md`** - Complete guidelines for backing up large tables (1M+ rows)
- **`WATERMARK_DEEP_DIVE.md`** - Technical deep-dive into watermark-based data loss prevention  
- **`WATERMARK_ROW_COUNT_ACCUMULATION_FIX.md`** - Latest watermark simplification details

### **🔧 User References**
- **`USER_MANUAL.md`** - Comprehensive CLI usage guide
- **`WATERMARK_CLI_GUIDE.md`** - Watermark management commands
- **`README.md`** - Project overview and quick start

---

**🌟 Current Status**: Production-ready system with all critical bugs resolved. Simplified watermark architecture ensures reliable row count tracking across all operations.