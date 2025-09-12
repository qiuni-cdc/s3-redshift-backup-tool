# S3-Redshift Backup Tool - CLI Quick Reference Card

## Command Structure Overview

```
python -m src.cli.main <command> [subcommand] [options]

Main Commands:
├── sync         # Data synchronization (has subcommands)
├── watermark    # Watermark management  
├── watermark-count  # Row count management
├── s3clean      # S3 file cleanup
├── status       # System status check
├── column-mappings  # Column mapping management
├── backup       # Legacy backup command (v1.0.0)
└── info         # Strategy information
```

## 📊 Sync Commands

### Direct Sync (v1.0.0 Compatibility)
```bash
# Single table (uses default pipeline if configured)
python -m src.cli.main sync -t schema.table

# Multiple tables
python -m src.cli.main sync -t table1 -t table2 -t table3

# With options
python -m src.cli.main sync -t table --backup-only    # MySQL → S3 only
python -m src.cli.main sync -t table --redshift-only  # S3 → Redshift only
python -m src.cli.main sync -t table --limit 1000     # Limit rows for testing
```

### Pipeline-Based Sync (v1.1.0+ Recommended)
```bash
# Basic pipeline sync
python -m src.cli.main sync pipeline -p pipeline_name -t table_name

# Multiple tables with pipeline
python -m src.cli.main sync pipeline -p us_dw_pipeline -t table1 -t table2

# Pipeline sync options
python -m src.cli.main sync pipeline -p pipeline_name -t table --dry-run
python -m src.cli.main sync pipeline -p pipeline_name -t table --backup-only
python -m src.cli.main sync pipeline -p pipeline_name -t table --redshift-only
python -m src.cli.main sync pipeline -p pipeline_name -t table --parallel
python -m src.cli.main sync pipeline -p pipeline_name -t table --limit 10000
python -m src.cli.main sync pipeline -p pipeline_name -t table --max-chunks 5
```

### Connection-Based Sync (Ad-hoc)
```bash
# Explicit source and target connections
python -m src.cli.main sync connections -s SOURCE_CONN -r TARGET_CONN -t table

# With options
python -m src.cli.main sync connections -s US_DW_RO_SSH -r redshift_default -t table --batch-size 50000
```

## 💾 Watermark Commands

```bash
# View watermark
python -m src.cli.main watermark get -t schema.table
python -m src.cli.main watermark get -t schema.table --show-files

# Set watermark
python -m src.cli.main watermark set -t table --timestamp '2025-01-01 00:00:00'
python -m src.cli.main watermark set -t table --id 1000000
python -m src.cli.main watermark set -t table --timestamp '2025-01-01' --id 1000000

# Reset watermark
python -m src.cli.main watermark reset -t schema.table
python -m src.cli.main watermark force-reset -t schema.table

# List all watermarks
python -m src.cli.main watermark list
```

## 🔢 Watermark Count Commands

```bash
# Validate row counts
python -m src.cli.main watermark-count validate-counts -t schema.table

# Fix row counts
python -m src.cli.main watermark-count set-count -t table --count 3000000 --mode absolute
python -m src.cli.main watermark-count set-count -t table --count 500000 --mode additive
```

## 🧹 S3 Cleanup Commands

```bash
# List S3 files
python -m src.cli.main s3clean list -t schema.table
python -m src.cli.main s3clean list -t schema.table --show-timestamps

# Clean S3 files
python -m src.cli.main s3clean clean -t table --older-than 7d
python -m src.cli.main s3clean clean -t table --older-than 7d --dry-run
python -m src.cli.main s3clean clean -t table --older-than 7d --force
python -m src.cli.main s3clean clean -t table --pattern "batch_*"

# Clean all tables (use with caution)
python -m src.cli.main s3clean clean-all --older-than 30d
```

## 🗺️ Column Mapping Commands

```bash
# List all mappings
python -m src.cli.main column-mappings list

# Show mapping for specific table
python -m src.cli.main column-mappings show -t schema.table

# Clear mapping (if needed)
python -m src.cli.main column-mappings clear -t schema.table
```

## ℹ️ Information Commands

```bash
# Check system status
python -m src.cli.main status

# Get strategy information
python -m src.cli.main info -s sequential
python -m src.cli.main info -s inter-table
```

## 🎯 Common Workflows

### 1. Fresh Sync from Specific Date
```bash
python -m src.cli.main watermark reset -t table
python -m src.cli.main watermark set -t table --timestamp '2025-01-01 00:00:00'
python -m src.cli.main sync pipeline -p pipeline_name -t table
```

### 2. Test Sync with Limited Data
```bash
python -m src.cli.main sync pipeline -p pipeline_name -t table --limit 1000 --dry-run
python -m src.cli.main sync pipeline -p pipeline_name -t table --limit 1000
```

### 3. Fix Row Count Discrepancy
```bash
python -m src.cli.main watermark-count validate-counts -t table
python -m src.cli.main watermark-count set-count -t table --count 2500000 --mode absolute
```

### 4. Clean Old S3 Files
```bash
python -m src.cli.main s3clean list -t table --show-timestamps
python -m src.cli.main s3clean clean -t table --older-than 7d --dry-run
python -m src.cli.main s3clean clean -t table --older-than 7d
```

## 📝 Important Notes

### Pipeline Locations
- Pipeline configs: `config/pipelines/*.yml`
- Connection configs: `config/connections.yml`
- Default pipeline: `config/pipelines/default.yml`

### Multiple Tables
- Use multiple `-t` flags: `-t table1 -t table2`
- NOT comma-separated: ~~`-t table1,table2`~~

### Environment Variables
```bash
export BACKUP__BATCH_SIZE=5000      # Override batch size
export BACKUP__MAX_WORKERS=4        # Override max workers
```

### Common Options
- `--dry-run`: Preview without execution
- `--limit N`: Limit rows per query (testing)
- `--max-chunks N`: Limit total chunks processed
- `--backup-only`: Skip Redshift loading
- `--redshift-only`: Skip MySQL extraction
- `--force`: Skip confirmation prompts
- `--show-timestamps`: Show detailed timestamps

## 🚀 Quick Start Examples

```bash
# 1. Check everything is working
python -m src.cli.main status

# 2. Sync a table with pipeline
python -m src.cli.main sync pipeline -p us_dw_pipeline -t settlement.orders

# 3. Check sync progress
python -m src.cli.main watermark get -t settlement.orders

# 4. Clean old files weekly
python -m src.cli.main s3clean clean -t settlement.orders --older-than 7d
```

## ⚠️ Version Notes

- **v1.0.0**: Direct sync syntax (`sync -t table`)
- **v1.1.0+**: Pipeline/connection subcommands (`sync pipeline -p name -t table`)
- **v1.2.0+**: CDC strategies, column mappings, enhanced watermarks

---
*Use `--help` with any command for detailed options*