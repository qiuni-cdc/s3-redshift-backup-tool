# 📅 Enhanced Watermark Management System

## 🚀 **Complete Watermark CLI Commands**

### **Table-Specific Watermark Operations**

```bash
# Get watermark for specific table
python -m src.cli.main watermark get -t settlement.settlement_normal_delivery_detail

# Get watermark with processed S3 files list
python -m src.cli.main watermark get -t settlement.settlement_normal_delivery_detail --show-files

# Set manual starting timestamp for table
python -m src.cli.main watermark set -t settlement.settlement_normal_delivery_detail --timestamp "2025-08-09 20:00:01"

# Reset watermark for specific table (fresh start)
python -m src.cli.main watermark reset -t settlement.settlement_normal_delivery_detail

# List all table watermarks
python -m src.cli.main watermark list
```

### **Global System Watermark Operations**

```bash
# Get current system watermark
python -m src.cli.main watermark get

# Set system watermark with presets
python -m src.cli.main watermark set aug4 --force
python -m src.cli.main watermark set 1week --force

# Reset system watermark with presets
python -m src.cli.main watermark reset aug4 --force
```

## 🎯 **Enhanced Watermark Architecture**

### **Dual-Level System**

**1. System-Level Watermarks (Legacy)**
- **Storage**: S3 file `watermark/last_run_timestamp.txt`
- **Scope**: Global system default
- **Use Case**: Fallback for tables without specific watermarks

**2. Table-Level Watermarks (Production)**
- **Storage**: S3-based JSON with comprehensive metadata
- **Scope**: Individual tables with full tracking
- **Structure**:
```json
{
  "last_mysql_data_timestamp": "2025-08-09 20:00:01",
  "last_mysql_extraction_time": "2025-08-14 15:30:45",
  "mysql_status": "success",
  "redshift_status": "pending", 
  "backup_strategy": "manual_cli",
  "metadata": {
    "created_at": "2025-08-14T15:30:45Z",
    "updated_at": "2025-08-14T15:30:45Z"
  }
}
```

## 📋 **Production Usage Patterns**

### **Complete Fresh Sync Workflow**

```bash
# 1. Reset table watermark
python -m src.cli.main watermark reset -t settlement.settlement_normal_delivery_detail

# 2. Set starting point for data processing
python -m src.cli.main watermark set -t settlement.settlement_normal_delivery_detail --timestamp '2025-08-09 20:00:01'

# 3. Run full sync (MySQL → S3 → Redshift)
python -m src.cli.main sync -t settlement.settlement_normal_delivery_detail

# 4. Verify watermark was updated
python -m src.cli.main watermark get -t settlement.settlement_normal_delivery_detail
```

### **Load Existing S3 Files After Timestamp**

```bash
# 1. Reset and set manual watermark
python -m src.cli.main watermark reset -t settlement.settlement_normal_delivery_detail
python -m src.cli.main watermark set -t settlement.settlement_normal_delivery_detail --timestamp '2025-08-09 20:00:01'

# 2. Load only to Redshift (preserves manual watermark)
python -m src.cli.main sync -t settlement.settlement_normal_delivery_detail --redshift-only

# 3. Check results
python -m src.cli.main watermark get -t settlement.settlement_normal_delivery_detail
```

### **Multiple Table Management**

```bash
# Set watermarks for multiple tables
python -m src.cli.main watermark set -t settlement.settlement_claim_detail --timestamp '2025-08-09 20:00:01'
python -m src.cli.main watermark set -t settlement.settlement_normal_delivery_detail --timestamp '2025-08-09 20:00:01'

# List all table watermarks for review
python -m src.cli.main watermark list

# Process all tables with manual watermarks
python -m src.cli.main sync -t settlement.settlement_claim_detail
python -m src.cli.main sync -t settlement.settlement_normal_delivery_detail
```

## 🔧 **Advanced Watermark Features**

### **Manual vs Automated Watermarks**

**Manual Watermarks (backup_strategy='manual_cli')**
- **Use Case**: Precise control over data processing windows
- **Behavior**: Preserved during `--redshift-only` operations
- **Filtering**: Files created after manually set timestamp
- **Priority**: Takes precedence over automated watermarks

**Automated Watermarks (backup_strategy='sequential')**  
- **Use Case**: Regular incremental backups
- **Behavior**: Updated automatically by backup process
- **Filtering**: Session-based (±10 hours around extraction time)
- **Management**: Controlled by backup commands

### **Watermark Status Tracking**

```bash
# Check watermark details
python -m src.cli.main watermark get -t settlement.settlement_normal_delivery_detail
```

**Key Status Indicators:**
- **mysql_status**: pending/success/failed (backup phase)
- **redshift_status**: pending/success/failed (loading phase)  
- **backup_strategy**: manual_cli/sequential/inter-table/intra-table
- **timestamps**: Data cutoff vs extraction time tracking

### **S3 File Filtering Logic**

**Manual Watermarks:**
```
Files included: S3 creation time > manual timestamp
Strategy: Precise control over data window
Use case: Load existing S3 files after specific date
```

**Automated Watermarks:**
```
Files included: Within session window (extraction_time ± 10 hours)
Strategy: Handles timezone differences and processing delays
Use case: Regular incremental processing
```

## 🚨 **Troubleshooting Guide**

### **Common Issues and Solutions**

**Issue: "Found X files, filtered to 0 files for loading"**
```bash
# Check watermark vs S3 file timestamps
python -m src.cli.main watermark get -t settlement.table_name
aws s3 ls s3://bucket/incremental/ --recursive | grep table_name

# Solution: Adjust watermark timestamp to match S3 file creation times
python -m src.cli.main watermark set -t settlement.table_name --timestamp '2025-08-09 19:00:00'
```

**Issue: Manual watermark being overwritten**
```bash
# Check backup strategy in watermark
python -m src.cli.main watermark get -t settlement.table_name

# If backup_strategy != 'manual_cli', the backup process overwrote it
# Solution: Use --redshift-only to preserve manual watermarks
python -m src.cli.main sync -t settlement.table_name --redshift-only
```

**Issue: No S3 files found for manual watermark**
```bash
# Verify S3 files exist after watermark timestamp
aws s3 ls s3://bucket/incremental/settlement.table_name/ --recursive

# Solution: Set watermark before earliest S3 file timestamp
python -m src.cli.main watermark set -t settlement.table_name --timestamp '2025-08-08 00:00:00'
```

### **Watermark Debugging**

```bash
# Enable debug mode for detailed watermark logging
python -m src.cli.main sync -t settlement.table_name --redshift-only --debug

# Look for these log messages:
# ✅ "Using manual watermark-based incremental Redshift loading: files after YYYY-MM-DD"
# ❌ "Using session-based incremental Redshift loading: files from X to Y"
```

## 📊 **Legacy System Presets**

### **Available System Presets**

```bash
# Historical data processing presets
python -m src.cli.main watermark reset aug4 --force    # 2025-08-04 00:00:00
python -m src.cli.main watermark reset aug1 --force    # 2025-08-01 00:00:00

# Relative time presets
python -m src.cli.main watermark reset 1week --force   # 1 week ago
python -m src.cli.main watermark reset 1month --force  # 1 month ago
```

## 🎯 **Best Practices**

### **Production Recommendations**

**1. Use Table-Specific Watermarks**
```bash
# Preferred: Table-specific control
python -m src.cli.main watermark get -t settlement.table_name

# Avoid: System-wide watermarks for production
python -m src.cli.main watermark get  # Legacy approach
```

**2. Preserve Manual Watermarks**
```bash
# For existing S3 files: Use --redshift-only
python -m src.cli.main sync -t settlement.table_name --redshift-only

# For fresh data: Full sync will update watermark
python -m src.cli.main sync -t settlement.table_name
```

**3. Verify Watermark Changes**
```bash
# Always verify after setting watermarks
python -m src.cli.main watermark set -t settlement.table_name --timestamp '2025-08-09 20:00:01'
python -m src.cli.main watermark get -t settlement.table_name  # Confirm change applied
```

**4. Monitor Watermark Status**
```bash
# Regular watermark health check
python -m src.cli.main watermark list  # Review all table watermarks
```

## 🏆 **Production Status** ✅

**The Enhanced Watermark Management System provides:**

- ✅ **Table-Level Granularity**: Individual control per table
- ✅ **Manual Override Capabilities**: Precise data window control  
- ✅ **Automated Tracking**: Seamless integration with backup processes
- ✅ **Comprehensive Status**: Full pipeline state tracking
- ✅ **Enterprise Safety**: Validation and error recovery
- ✅ **Flexible Operations**: Support for all sync scenarios

**The system handles both automated incremental processing and manual data loading scenarios with enterprise-grade reliability and precision.** 🚀