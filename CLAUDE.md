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

## Project Overview

This system implements three backup strategies:
- **Sequential Backup**: Process tables one by one
- **Inter-table Parallel**: Process multiple tables simultaneously 
- **Intra-table Parallel**: Split large tables into time-based chunks

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

#### Watermark Architecture
```
Watermark Components:
├── last_mysql_data_timestamp     # Data cutoff point for MySQL extraction
├── last_mysql_extraction_time    # When backup process ran
├── mysql_status                  # pending/success/failed
├── redshift_status              # pending/success/failed  
├── backup_strategy              # sequential/inter-table/intra-table/manual_cli
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
python -m src.cli.main watermark get -t settlement.table_name
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