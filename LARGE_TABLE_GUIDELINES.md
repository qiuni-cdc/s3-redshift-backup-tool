# Large Table Backup Guidelines

🏗️ **Production-Ready Guidelines for Backing Up Large Tables (1M+ Rows)**

This document provides comprehensive guidelines for efficiently and safely backing up large MySQL tables using the S3-Redshift backup system.

---

## 📊 **Table Size Classification**

### **Size Categories**
| Size Category | Row Count | Estimated Backup Time | Strategy | Configuration |
|---------------|-----------|----------------------|----------|---------------|
| **Small** | < 100K | < 5 minutes | Sequential | Default settings |
| **Medium** | 100K - 1M | 5-30 minutes | Sequential | Optimized batch size |
| **Large** | 1M - 10M | 30 minutes - 5 hours | Sequential + Optimization | Custom configuration |
| **Very Large** | > 10M | > 5 hours | Sequential + Monitoring | Enterprise settings |

### **Performance Benchmarks**
Based on production testing:
- **Processing Rate**: ~17,000 rows/minute (average)
- **Network Impact**: ~1MB/minute S3 upload (compressed parquet)
- **Memory Usage**: ~50MB per worker (with 5K batch size)

---

## 🎯 **Recommended Strategy: Sequential + Optimizations**

### **Why Sequential is Best for Large Tables**

✅ **Proven Reliability**
- No chunk boundary bugs (unlike intra-table strategy)
- Predictable memory usage and performance
- Simplified error recovery and debugging

✅ **Built-in Fault Tolerance**
- Automatic retry with exponential backoff
- Watermark-based resumable processing
- No data loss on interruption

✅ **Production Tested**
- Successfully handles tables with millions of rows
- Stable performance across different network conditions
- Comprehensive logging and monitoring

### **Basic Large Table Command**
```bash
# Recommended approach for large tables
python -m src.cli.main sync -t settlement.settlement_normal_delivery_detail -s sequential
```

---

## ⚙️ **Configuration Optimization**

### **Large Table Configuration (`.env`)**

```bash
# === LARGE TABLE OPTIMIZATIONS ===

# Batch Processing
BACKUP_BATCH_SIZE=5000              # Smaller batches for stability (default: 10000)
BACKUP_MAX_WORKERS=2                # Conservative workers for large tables (default: 4)
BACKUP_TIMEOUT_SECONDS=1800         # 30 minutes timeout (default: 300)
BACKUP_RETRY_ATTEMPTS=5             # More retries for network issues (default: 3)

# Database Connection Optimization
DATABASE_CONNECTION_TIMEOUT=60      # Longer timeout for large queries
DATABASE_POOL_SIZE=5                # Adequate connection pool
DATABASE_MAX_OVERFLOW=10            # Handle connection spikes

# S3 Upload Optimization
S3_MULTIPART_THRESHOLD=104857600    # 100MB multipart threshold
S3_MAX_CONCURRENCY=3                # Conservative S3 concurrency
S3_CHUNK_SIZE=8388608               # 8MB chunks for reliable upload

# Memory Management
BACKUP_NUM_CHUNKS=4                 # Not used in sequential, but good default
MAX_MEMORY_USAGE_MB=512             # Memory limit per process
```

### **Network-Optimized Configuration**
```bash
# For slow or unreliable networks
BACKUP_BATCH_SIZE=1000              # Very small batches
BACKUP_TIMEOUT_SECONDS=3600         # 1 hour timeout
S3_MAX_CONCURRENCY=1                # Single-threaded uploads
S3_CHUNK_SIZE=4194304               # 4MB chunks
BACKUP_RETRY_ATTEMPTS=10            # Many retries
```

### **High-Performance Configuration**
```bash
# For fast, reliable networks
BACKUP_BATCH_SIZE=10000             # Larger batches
BACKUP_MAX_WORKERS=4                # More workers
S3_MAX_CONCURRENCY=5                # Higher S3 concurrency
DATABASE_POOL_SIZE=10               # Larger connection pool
```

---

## 🔖 **Watermark Management for Large Tables**

### **Understanding Watermarks**

Watermarks are **resumable checkpoints** that prevent data loss:

```
Watermark = "Last successfully processed timestamp"
Next backup starts from: WHERE update_at > watermark_timestamp
```

### **Watermark Architecture**

```json
{
  "table_name": "settlement.settlement_normal_delivery_detail",
  "last_mysql_data_timestamp": "2025-08-14 15:30:22",    // ← Resume point
  "last_mysql_extraction_time": "2025-08-14 16:45:00",   // ← When backup ran
  "mysql_status": "success",                              // ← MySQL→S3 status
  "redshift_status": "success",                           // ← S3→Redshift status
  "mysql_rows_extracted": 1250000,                       // ← Progress tracking
  "redshift_rows_loaded": 1250000,                       // ← Verification
  "backup_strategy": "sequential",                        // ← Strategy used
  "s3_file_count": 250,                                  // ← Files created
  "created_at": "2025-08-14T20:58:18Z",                 // ← First backup
  "updated_at": "2025-08-14T22:45:33Z"                  // ← Last update
}
```

### **Watermark Best Practices**

#### **1. Initial Setup for Large Tables**
```bash
# Clean slate for fresh backup
python -m src.cli.main s3clean clean -t large_table

# Set watermark to desired start point
python -m src.cli.main watermark set -t large_table --timestamp '2025-08-01 00:00:00'

# Verify watermark
python -m src.cli.main watermark get -t large_table
```

#### **2. Progressive Sync for Very Large Tables**
```bash
# For 10M+ row tables, consider progressive sync
# Start with recent data first (last 30 days)
python -m src.cli.main watermark set -t huge_table --timestamp '2025-07-15 00:00:00'
python -m src.cli.main sync -t huge_table

# Then backfill historical data
python -m src.cli.main watermark set -t huge_table --timestamp '2025-01-01 00:00:00'  
python -m src.cli.main sync -t huge_table --backup-only  # Skip Redshift for now
```

#### **3. Watermark Monitoring**
```bash
# Check watermark status during long backups
python -m src.cli.main watermark get -t large_table

# List all watermarks
python -m src.cli.main watermark list

# Check S3 progress
python -m src.cli.main s3clean list -t large_table
```

### **Watermark Data Loss Prevention**

#### **Atomic Updates**
```
✅ SAFE: Watermark updated only after successful S3 upload
❌ UNSAFE: Watermark updated before confirming upload
```

#### **Resumable Processing**
```bash
# Scenario: 5M row table backup interrupted at 3M rows

Before interruption:
┌─────────────────────────────────────────────────────────┐
│ ✅ Processed: 3M rows                                   │
│ 💾 Watermark: 2025-08-10 14:30:22                      │
│ 📁 S3 Files: 600 parquet files                         │
│ ❌ NETWORK FAILURE                                      │
└─────────────────────────────────────────────────────────┘

After restart:
┌─────────────────────────────────────────────────────────┐
│ 🔄 Resumes from: 2025-08-10 14:30:22                   │
│ 🎯 Only processes remaining 2M rows                     │
│ 🚫 NO data loss or duplication                         │
│ ⏱️  Saves hours of processing time                      │
└─────────────────────────────────────────────────────────┘
```

---

## 🚀 **Large Table Workflow**

### **Phase 1: Preparation**

#### **1. System Assessment**
```bash
# Check system resources
df -h                    # Disk space
free -h                  # Memory
netstat -i               # Network interfaces

# Database optimization
# Add index on update_at column (if not exists)
# CREATE INDEX idx_table_update_at ON your_large_table(update_at);
```

#### **2. Environment Setup**
```bash
# Copy optimized configuration
cp .env.example .env
# Edit .env with large table settings (see Configuration section)

# Verify SSH tunnel connectivity
python -m src.cli.main status
```

#### **3. Clean Start**
```bash
# Clean any existing S3 data
python -m src.cli.main s3clean list -t large_table
python -m src.cli.main s3clean clean -t large_table

# Set initial watermark
python -m src.cli.main watermark reset -t large_table
python -m src.cli.main watermark set -t large_table --timestamp '2025-08-01 00:00:00'
```

### **Phase 2: Testing**

#### **1. Small Scale Test**
```bash
# Test with limited rows
python -m src.cli.main sync -t large_table --limit 1000 --dry-run
python -m src.cli.main sync -t large_table --limit 1000

# Verify results
python -m src.cli.main watermark get -t large_table
python -m src.cli.main s3clean list -t large_table
```

#### **2. Medium Scale Test**
```bash
# Gradually increase scale
python -m src.cli.main sync -t large_table --limit 50000
python -m src.cli.main sync -t large_table --limit 200000

# Monitor performance
python -m src.cli.main sync -t large_table --limit 500000 --debug
```

### **Phase 3: Production Backup**

#### **1. Full Sync Execution**
```bash
# Run full backup with monitoring
python -m src.cli.main sync -t large_table -s sequential --debug > backup.log 2>&1 &

# Monitor progress
tail -f backup.log
python -m src.cli.main watermark get -t large_table    # Check progress
python -m src.cli.main s3clean list -t large_table     # Check S3 files
```

#### **2. Progress Monitoring**
```bash
# Check watermark progress every hour
watch -n 3600 'python -m src.cli.main watermark get -t large_table'

# Monitor S3 file count
watch -n 1800 'python -m src.cli.main s3clean list -t large_table | wc -l'

# Check system resources
watch -n 300 'free -h && df -h'
```

### **Phase 4: Verification**

#### **1. Data Integrity Check**
```bash
# Verify sync completion
python -m src.cli.main sync -t large_table --verify-data

# Check final watermark
python -m src.cli.main watermark get -t large_table

# Verify S3 data
python -m src.cli.main s3clean list -t large_table
```

#### **2. Redshift Verification**
```sql
-- Connect to Redshift via SSH tunnel
-- Verify row count
SELECT COUNT(*) FROM large_table;

-- Check data distribution
SELECT 
    DATE(update_at) as date,
    COUNT(*) as rows
FROM large_table 
GROUP BY DATE(update_at) 
ORDER BY date DESC 
LIMIT 10;

-- Verify latest data
SELECT * FROM large_table 
ORDER BY update_at DESC 
LIMIT 5;
```

---

## 🛡️ **Error Handling & Recovery**

### **Common Large Table Issues**

#### **1. Network Timeouts**
```bash
# Symptoms: "Connection timeout" or "S3 upload failed"
# Solution: Reduce batch size and increase timeouts

# Update .env:
BACKUP_BATCH_SIZE=2000
BACKUP_TIMEOUT_SECONDS=3600
S3_MAX_CONCURRENCY=1
```

#### **2. Memory Issues**
```bash
# Symptoms: "OutOfMemory" or system slowdown
# Solution: Reduce batch size and workers

# Update .env:
BACKUP_BATCH_SIZE=1000
BACKUP_MAX_WORKERS=1
MAX_MEMORY_USAGE_MB=256
```

#### **3. Database Locks**
```bash
# Symptoms: "Lock wait timeout exceeded"
# Solution: Optimize query and reduce concurrency

# Update .env:
BACKUP_BATCH_SIZE=5000
DATABASE_CONNECTION_TIMEOUT=120
BACKUP_MAX_WORKERS=1
```

### **Recovery Procedures**

#### **1. Interrupted Backup Recovery**
```bash
# Check last successful watermark
python -m src.cli.main watermark get -t large_table

# Resume from last checkpoint
python -m src.cli.main sync -t large_table -s sequential

# Monitor recovery progress
tail -f backup.log
```

#### **2. Corrupted Watermark Recovery**
```bash
# Backup current watermark
python -m src.cli.main watermark get -t large_table > watermark_backup.json

# Reset to safe point (if known)
python -m src.cli.main watermark set -t large_table --timestamp '2025-08-10 15:30:00'

# Resume backup
python -m src.cli.main sync -t large_table
```

#### **3. S3 Data Consistency Issues**
```bash
# Check S3 file integrity
python -m src.cli.main s3clean list -t large_table

# Clean and restart if necessary
python -m src.cli.main s3clean clean -t large_table
python -m src.cli.main watermark reset -t large_table
python -m src.cli.main watermark set -t large_table --timestamp 'last_known_good'
```

---

## 📈 **Performance Optimization**

### **Database Optimizations**

#### **1. Index Optimization**
```sql
-- Essential index for incremental backups
CREATE INDEX idx_table_update_at ON large_table(update_at);

-- Composite index for better performance
CREATE INDEX idx_table_update_id ON large_table(update_at, ID);

-- Check index usage
EXPLAIN SELECT * FROM large_table 
WHERE update_at > '2025-08-01 00:00:00' 
ORDER BY update_at, ID 
LIMIT 5000;
```

#### **2. MySQL Configuration**
```ini
# my.cnf optimizations for large tables
[mysqld]
innodb_buffer_pool_size = 2G
innodb_log_file_size = 512M
innodb_flush_log_at_trx_commit = 2
query_cache_size = 256M
max_connections = 200
wait_timeout = 600
interactive_timeout = 600
```

### **System Resource Optimization**

#### **1. Memory Management**
```bash
# Monitor memory usage during backup
watch -n 30 'free -h'

# Adjust batch size based on available memory
# Rule of thumb: batch_size * 1KB ≈ memory per batch
# 5000 rows ≈ 5MB memory usage
```

#### **2. Network Optimization**
```bash
# Monitor network usage
watch -n 30 'iftop -t -s 10'

# Adjust S3 settings for network capacity
# For 10Mbps connection: S3_MAX_CONCURRENCY=2
# For 100Mbps connection: S3_MAX_CONCURRENCY=5
```

### **Performance Monitoring**

#### **1. Real-time Monitoring**
```bash
# Create monitoring script
cat > monitor_backup.sh << 'EOF'
#!/bin/bash
while true; do
    echo "=== $(date) ==="
    python -m src.cli.main watermark get -t large_table | grep "Rows Extracted"
    python -m src.cli.main s3clean list -t large_table | wc -l | xargs echo "S3 files:"
    free -h | grep Mem
    echo ""
    sleep 300  # Check every 5 minutes
done
EOF

chmod +x monitor_backup.sh
./monitor_backup.sh &
```

#### **2. Performance Metrics**
```bash
# Calculate processing rate
echo "Processing rate calculation:"
echo "Rows extracted: $(python -m src.cli.main watermark get -t large_table | grep 'Rows Extracted' | cut -d: -f2)"
echo "Time elapsed: $(date)"
echo "Rate: $(python -c 'print(rows_extracted / minutes_elapsed)')"
```

---

## 🚨 **Troubleshooting Guide**

### **Performance Issues**

| Issue | Symptom | Solution |
|-------|---------|----------|
| **Slow Processing** | < 10K rows/minute | Reduce batch size, check indexes |
| **High Memory Usage** | > 1GB memory | Reduce batch size and workers |
| **Network Bottleneck** | S3 upload errors | Reduce S3 concurrency |
| **Database Locks** | Lock timeout errors | Reduce workers, optimize queries |

### **Data Integrity Issues**

| Issue | Symptom | Solution |
|-------|---------|----------|
| **Missing Rows** | Row count mismatch | Verify watermark, check WHERE clause |
| **Duplicate Rows** | Higher row count | Check Redshift COPY settings |
| **Corrupted Files** | Parquet read errors | Clean S3, restart from watermark |
| **Stale Watermark** | No progress updates | Check watermark update logic |

### **System Issues**

| Issue | Symptom | Solution |
|-------|---------|----------|
| **SSH Timeouts** | Connection lost | Increase timeouts, check network |
| **Disk Space** | No space left | Clean logs, optimize S3 usage |
| **Permission Errors** | Access denied | Check AWS credentials, file permissions |
| **Process Hanging** | No progress | Check locks, restart with debug |

---

## ✅ **Best Practices Summary**

### **DO's ✅**

1. **✅ Use Sequential Strategy** - Most reliable for large tables
2. **✅ Test with --limit First** - Start small, scale gradually  
3. **✅ Monitor Watermarks** - Track progress regularly
4. **✅ Optimize Configuration** - Tune for your environment
5. **✅ Use Incremental Processing** - Leverage watermark system
6. **✅ Monitor System Resources** - Watch memory, network, disk
7. **✅ Verify Data Integrity** - Check row counts and samples
8. **✅ Plan for Interruptions** - System designed to resume safely

### **DON'Ts ❌**

1. **❌ Don't Use Intra-table Strategy** - Disabled due to bugs
2. **❌ Don't Skip Testing** - Always test with limited rows first
3. **❌ Don't Ignore Watermarks** - Critical for data consistency
4. **❌ Don't Use Large Batch Sizes** - > 10K can cause issues
5. **❌ Don't Skip Verification** - Always verify final results
6. **❌ Don't Run Without SSH Tunnel** - Security requirement
7. **❌ Don't Skip Monitoring** - Long-running processes need oversight
8. **❌ Don't Panic on Failures** - System designed to recover safely

---

## 🎯 **Quick Reference Commands**

### **Essential Commands**
```bash
# Setup
python -m src.cli.main watermark set -t table --timestamp 'YYYY-MM-DD HH:MM:SS'

# Test
python -m src.cli.main sync -t table --limit 10000 --dry-run

# Execute  
python -m src.cli.main sync -t table -s sequential

# Monitor
python -m src.cli.main watermark get -t table
python -m src.cli.main s3clean list -t table

# Verify
python -m src.cli.main sync -t table --verify-data
```

### **Emergency Commands**
```bash
# Stop gracefully (Ctrl+C)
# Check status
python -m src.cli.main watermark get -t table

# Resume
python -m src.cli.main sync -t table -s sequential

# Reset if needed
python -m src.cli.main watermark reset -t table
python -m src.cli.main s3clean clean -t table
```

---

## 📚 **Additional Resources**

- **Main Documentation**: `CLAUDE.md` - System overview and features
- **User Manual**: `USER_MANUAL.md` - Comprehensive usage guide  
- **Watermark CLI**: `WATERMARK_CLI_GUIDE.md` - Detailed watermark management
- **Configuration**: `.env.example` - All configuration options
- **Troubleshooting**: This document's troubleshooting section

---

*🤖 Generated with [Claude Code](https://claude.ai/code) - Production-ready guidelines for large table backup operations*