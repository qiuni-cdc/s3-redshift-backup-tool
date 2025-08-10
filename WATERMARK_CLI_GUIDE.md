# 📅 Watermark Management CLI Guide

The watermark management commands have been successfully integrated into the main CLI! Here's your complete guide:

## 🚀 **CLI Watermark Commands**

### **Basic Usage**

```bash
# Activate environment first
source test_env/bin/activate

# Get current watermark
python -m src.cli.main watermark get

# Set watermark to specific date/time
python -m src.cli.main watermark set '2025-08-04 00:00:00' --force

# Reset using presets
python -m src.cli.main watermark reset aug4 --force
```

## 📋 **Available Commands**

### **1. Get Current Watermark**

```bash
python -m src.cli.main watermark get
```

**Output includes:**
- Current watermark value
- S3 storage location
- Time analysis (days/hours since watermark)
- Recommendations based on age

### **2. Set Watermark**

```bash
# Set specific date/time
python -m src.cli.main watermark set '2025-08-01 12:30:00' --force

# Use preset shortcuts
python -m src.cli.main watermark set aug4 --force
python -m src.cli.main watermark set 1week --force
python -m src.cli.main watermark set 3days --force
```

**Options:**
- `--force` - Bypass safety checks (recommended for going backwards in time)
- Without `--force` - Interactive confirmation for safety

### **3. Reset Watermark (Presets)**

```bash
python -m src.cli.main watermark reset aug4 --force
python -m src.cli.main watermark reset 1week --force
python -m src.cli.main watermark reset 3days --force
```

**Available presets:**
- `aug4` - 2025-08-04 00:00:00 (known settlement data)
- `aug1` - 2025-08-01 00:00:00 (beginning of August)
- `1day` - 1 day ago from current time
- `3days` - 3 days ago from current time
- `1week` - 1 week ago from current time
- `2weeks` - 2 weeks ago from current time
- `1month` - 1 month ago from current time

## 🎯 **Usage Examples**

### **Check Current Status**

```bash
python -m src.cli.main watermark get
```

**Example output:**
```
📅 CURRENT WATERMARK STATUS
==================================================
Current watermark: 2025-08-04 00:00:00
S3 location: s3://redshift-dw-qa-uniuni-com/watermark/last_run_timestamp.txt

⏰ TIME ANALYSIS:
Days since watermark: 5
Hours since watermark: 141.5

✅ CURRENT: Watermark is reasonably recent
```

### **Reset for Historical Data Processing**

```bash
# Reset to August 4th (known settlement data start date)
python -m src.cli.main watermark reset aug4 --force
```

**Example output:**
```
🎯 PRESET WATERMARK RESET: aug4
==================================================
Setting watermark to: 2025-08-04 00:00:00

✅ WATERMARK RESET SUCCESSFUL!
✅ New watermark: 2025-08-04 00:00:00

🔄 NEXT STEPS:
   Run backup to process data from the new watermark:
   python -m src.cli.main backup -t settlement.settlement_normal_delivery_detail -s sequential
```

### **Set Custom Date/Time**

```bash
# Set to specific date and time
python -m src.cli.main watermark set '2025-08-05 14:30:00' --force
```

## 🔄 **Integration with Backup Commands**

### **Complete Workflow**

```bash
# 1. Check current watermark
python -m src.cli.main watermark get

# 2. Reset to capture historical data
python -m src.cli.main watermark reset aug4 --force

# 3. Run backup to process data from new watermark
python -m src.cli.main backup -t settlement.settlement_normal_delivery_detail -s sequential

# 4. Check system status
python -m src.cli.main status
```

### **For Different Data Ranges**

```bash
# Last week's data
python -m src.cli.main watermark reset 1week --force
python -m src.cli.main backup -t settlement.settlement_claim_detail -s sequential

# Last month's data  
python -m src.cli.main watermark reset 1month --force
python -m src.cli.main backup -t settlement.settlement_normal_delivery_detail -s sequential

# All settlement data from August 1st
python -m src.cli.main watermark reset aug1 --force
python -m src.cli.main backup -t settlement.settlement_claim_detail -t settlement.settlement_normal_delivery_detail -s sequential
```

## ⚠️ **Important Notes**

### **Safety Features**

- **Interactive Confirmation**: Without `--force`, the CLI will ask for confirmation before making changes
- **Validation**: Automatic validation of date/time formats
- **Impact Warning**: Clear explanation of what the watermark change will do
- **S3 Verification**: Automatic verification that the change was applied successfully

### **Force Mode**

The `--force` flag is recommended when:
- You want to process historical data (going backwards in time)
- You're automating scripts and don't want interactive prompts
- You're confident in your watermark change

### **Data Processing Impact**

When you change the watermark:
- **Forward in time**: Skip data (faster processing, may miss records)
- **Backward in time**: Reprocess historical data (slower, comprehensive)

## 📊 **Current Status** ✅

Your watermark is currently set to: **`2025-08-04 00:00:00`**

This means the next backup will process all settlement data updated since August 4th, 2025.

## 🏆 **CLI Integration Complete!**

The watermark management is now fully integrated into the production CLI with:
- ✅ **Complete command set** (get, set, reset)
- ✅ **Safety features** with force options
- ✅ **Preset shortcuts** for common scenarios
- ✅ **Integration** with existing backup commands
- ✅ **Production-ready** error handling and validation

**You can now manage watermarks entirely through the main CLI interface!** 🚀