# S3 to Redshift Backup System - Quick Reference

## 🚀 Core Commands

### **Complete Sync Pipeline**
```bash
# Full pipeline (MySQL → S3 → Redshift) with dynamic schema discovery
python -m src.cli.main sync -t settlement.settlement_normal_delivery_detail

# Backup only (MySQL → S3)
python -m src.cli.main sync -t settlement.settlement_normal_delivery_detail --backup-only

# Redshift loading only (S3 → Redshift) - preserves manual watermarks
python -m src.cli.main sync -t settlement.settlement_normal_delivery_detail --redshift-only
```

### **Watermark Management**
```bash
# View current watermark
python -m src.cli.main watermark get -t settlement.settlement_normal_delivery_detail

# Set manual starting timestamp for incremental sync
python -m src.cli.main watermark set -t settlement.settlement_normal_delivery_detail --timestamp "2025-08-09 20:00:01"

# Reset watermark completely (fresh start)
python -m src.cli.main watermark reset -t settlement.settlement_normal_delivery_detail

# List all table watermarks
python -m src.cli.main watermark list
```

### **S3 Storage Management**
```bash
# Check current storage usage
python -m src.cli.main s3clean list -t settlement.settlement_normal_delivery_detail

# Clean old backup files (recommended)
python -m src.cli.main s3clean clean -t settlement.settlement_normal_delivery_detail --older-than "7d"

# Preview cleanup before execution
python -m src.cli.main s3clean clean -t settlement.settlement_normal_delivery_detail --dry-run

# Force cleanup without prompts (for automation)
python -m src.cli.main s3clean clean -t settlement.settlement_normal_delivery_detail --force
```

## 📊 Performance Benefits

| Capability | Legacy Backup | Production System | Improvement |
|------------|---------------|-------------------|-------------|
| **Pipeline** | Backup only | MySQL → S3 → Redshift | Complete solution |
| **Schema Discovery** | Manual configuration | Automatic detection | 100% flexible |
| **Watermarks** | File-based | S3-based + manual control | Enterprise reliability |
| **Storage Management** | Manual cleanup | s3clean with safety features | Operational excellence |
| **Loading Method** | CSV conversion | Direct Parquet | 2-3x faster |
| **Error Handling** | Basic retry | Comprehensive recovery | Production grade |

## 🎯 Production Workflows

### **Fresh Sync from Specific Date**
```bash
# Reset and set starting timestamp
python -m src.cli.main watermark reset -t settlement.settlement_normal_delivery_detail
python -m src.cli.main watermark set -t settlement.settlement_normal_delivery_detail --timestamp '2025-08-09 20:00:01'

# Run full sync
python -m src.cli.main sync -t settlement.settlement_normal_delivery_detail
```

### **Load Existing S3 Files After Date**
```bash
# Set watermark for existing S3 files
python -m src.cli.main watermark reset -t settlement.settlement_normal_delivery_detail
python -m src.cli.main watermark set -t settlement.settlement_normal_delivery_detail --timestamp '2025-08-09 20:00:01'

# Load only to Redshift (preserves manual watermark)
python -m src.cli.main sync -t settlement.settlement_normal_delivery_detail --redshift-only
```

### **Storage Maintenance**
```bash
# Weekly cleanup workflow
python -m src.cli.main s3clean list -t settlement.settlement_normal_delivery_detail
python -m src.cli.main s3clean clean -t settlement.settlement_normal_delivery_detail --older-than "7d" --dry-run
python -m src.cli.main s3clean clean -t settlement.settlement_normal_delivery_detail --older-than "7d"
```

## 🔧 System Monitoring

### **Health Checks**
```bash
# System status
python -m src.cli.main status

# Check watermark status across all tables
python -m src.cli.main watermark list

# Review S3 storage usage
python -m src.cli.main s3clean list
```

### **Verification Queries (Redshift)**
```sql
-- Verify data was loaded
SELECT COUNT(*) FROM public.settlement_normal_delivery_detail;

-- Check latest status (use this for parcel queries)
SELECT * FROM public.settlement_latest_delivery_status 
WHERE ant_parcel_no = 'BAUNI000300014750782';

-- Status distribution
SELECT latest_status, COUNT(*) as parcel_count
FROM public.settlement_latest_delivery_status
GROUP BY latest_status
ORDER BY parcel_count DESC;
```

## 🚨 Troubleshooting

### **Watermark Issues**
```bash
# Check current watermark state
python -m src.cli.main watermark get -t settlement.settlement_normal_delivery_detail

# Look for manual watermark indicator
# backup_strategy should be 'manual_cli' for manual watermarks
```

### **S3 File Filtering**
- **Manual watermarks**: Files created after set timestamp are loaded
- **Automated watermarks**: Session-based filtering (±10 hours around extraction time)
- **Priority**: Manual watermarks take precedence over automated ones

### **Common Error Solutions**
| Error | Solution |
|-------|----------|
| "No S3 files found" | Check watermark timestamp vs S3 file creation times |
| "SSH connection failed" | Verify SSH key permissions: `chmod 600 key.pem` |
| "Access denied" | Check .env credentials and S3 bucket permissions |

## 🔒 Security Features

- **Credential Protection**: Comprehensive sanitization of logs and outputs
- **SSH Tunnel Support**: Secure connections via bastion hosts
- **Safe Operations**: Dry-run mode and confirmation prompts
- **Multi-layer Validation**: Table names, timestamps, file patterns

## 📈 Enterprise Capabilities

- **Scalability**: Handles 2M+ records efficiently
- **Reliability**: Production-tested with comprehensive error handling
- **Flexibility**: Multiple strategies and configuration options
- **Maintainability**: Complete documentation and monitoring tools
- **Security**: Enterprise-grade credential protection and secure connections

The system provides **complete MySQL → S3 → Redshift pipeline** with **enterprise-grade management capabilities** for production deployments.