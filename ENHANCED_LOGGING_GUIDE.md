# Enhanced Logging for flexible_progressive_backup.py

## 🚀 New Logging Features

The `flexible_progressive_backup.py` script now includes comprehensive logging enhancements to provide better visibility into backup operations.

---

## 📊 **Enhanced Chunk Logging**

### **Before Each Chunk**
```
🔄 Running chunk 5 (backup-only mode):
   Command: python -m src.cli.main sync -t settlement.table --limit 5000000 --backup-only
   Expected rows: up to 5,000,000
   Timeout: 7200s
```

### **After Each Successful Chunk**
```
✅ Chunk 5 completed successfully:
   📊 Rows processed: 4,892,341
   📁 S3 files created: 12
   ⏱️  Duration: 0:08:34.123456
   🚀 Rate: 34,298 rows/minute
   📈 Total processed: 24,561,705
   🔄 More data available: Yes
```

### **After Failed Chunks**
```
❌ Chunk 6 failed:
   ⏱️  Duration: 0:02:15.789012
   🔍 Error output: Connection timeout to MySQL server
   📋 Additional output: Attempting reconnection...
```

---

## 📈 **Enhanced Progress Monitoring**

### **Comprehensive Progress Updates** (Every 5 chunks or 10 minutes)
```
📊 === PROGRESS UPDATE ===
   ⏰ Session Time: 1:23:45.678901
   🔢 Chunks Processed: 8
   📈 Script Total Rows: 39,123,456
   🗄️  MySQL Status: success
   📊 MySQL Rows: 39,000,000
   📅 Last Data Timestamp: 2025-08-19 15:30:00
   🕐 Last Extraction: 2025-08-19 15:35:12
   🏭 Redshift Status: pending
   📊 Redshift Rows: 0
   🚀 Watermark Rate: 28,156 rows/minute
   🏃 Script Rate: 28,267 rows/minute
   ⏳ Timing: ~625s per chunk average
   ===============================
```

---

## 🛠️ **New Command Line Options**

### **Verbose Output**
```bash
# Show full sync command output for debugging
python flexible_progressive_backup.py settlement.huge_table --verbose-output
```

### **Debug Logging**
```bash
# Enable debug level logging with full output
python flexible_progressive_backup.py settlement.huge_table --log-level debug
```

### **Combined Options**
```bash
# Maximum visibility for troubleshooting
python flexible_progressive_backup.py settlement.huge_table \
    --log-level debug \
    --verbose-output \
    --chunk-size 1000000 \
    --max-chunks 5
```

---

## 📋 **Phase Transitions**

### **Backup Phase Start**
```
🚀 === STARTING PROGRESSIVE BACKUP PHASE ===
   📋 Table: settlement.massive_table
   📦 Chunk Size: 5,000,000 rows
   🔢 Max Chunks: unlimited
   ⏰ Timeout per chunk: 7200s
   📊 Monitor interval: every 5 chunks
   ===============================================
```

### **Pause Countdowns**
```
⏸️  Pausing 300 seconds before next chunk...
   ⏳ 4 minutes remaining...
   ⏳ 3 minutes remaining...
   ⏳ 2 minutes remaining...
   ⏳ 1 minutes remaining...
```

### **Phase Completion**
```
✅ Progressive backup phase completed successfully
   📊 Total chunks processed: 15
   📈 Total rows in session: 67,234,567
```

---

## 🔍 **Error Detection and Reporting**

### **Timeout Handling**
```
⏰ Chunk 12 timed out:
   ⏱️  Duration: 2:00:00.000000
   🚨 Timeout after: 7200s
```

### **Consecutive Failure Tracking**
```
⚠️  Consecutive failures: 2/3
🛑 Aborting after 3 consecutive failures
```

### **Data Consistency Warnings**
```
⚠️  Note: Script count (25,000,000) > Watermark (24,856,431)
```

---

## 📁 **Log File Management**

### **Automatic Log File Creation**
- **File Name**: `progressive_backup_{table}_{session_id}.log`
- **Example**: `progressive_backup_settlement_massive_table_20250819_143045.log`
- **Location**: Current working directory

### **Log File Contents**
- All console output with timestamps
- Detailed error messages
- Performance metrics
- Command execution details

---

## 🎯 **Usage Scenarios**

### **1. Production Monitoring**
```bash
# Standard production run with regular progress updates
python flexible_progressive_backup.py settlement.production_table \
    --chunk-size 10000000 \
    --mode backup-only
```

### **2. Debugging Issues**
```bash
# Maximum logging for troubleshooting
python flexible_progressive_backup.py settlement.problem_table \
    --log-level debug \
    --verbose-output \
    --chunk-size 100000 \
    --max-chunks 3
```

### **3. Performance Analysis**
```bash
# Monitor performance with frequent updates
python flexible_progressive_backup.py settlement.test_table \
    --chunk-size 5000000 \
    --monitor-interval 180  # Progress every 3 minutes
```

### **4. Testing and Validation**
```bash
# Limited test run with maximum visibility
python flexible_progressive_backup.py settlement.test_table \
    --dry-run  # Show what would be executed
```

---

## 💡 **Benefits**

### **Operational Visibility**
- **Real-time Progress**: Understand exactly what's happening during long-running backups
- **Performance Metrics**: Track processing rates and estimate completion times
- **Error Context**: Detailed error information for troubleshooting

### **Debugging Capabilities**
- **Command Visibility**: See exact CLI commands being executed
- **Output Inspection**: Review full sync command output when needed
- **Timing Analysis**: Understand where time is being spent

### **Production Monitoring**
- **Health Checks**: Monitor for failures and performance degradation
- **Progress Tracking**: Keep stakeholders informed of backup progress
- **Audit Trail**: Complete log files for post-mortem analysis

---

*The enhanced logging system provides comprehensive visibility into progressive backup operations while maintaining clean, readable output for normal operation and detailed diagnostics when needed.*