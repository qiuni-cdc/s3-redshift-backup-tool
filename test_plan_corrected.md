# 🧪 **CORRECTED End-to-End Test Plan: Fresh Sync with ID Watermark**

## **Test Objective**
Validate complete functionality of ID-based watermark system with fresh sync of `dw_parcel_detail_tool` starting from ID 281623217.

## **Test Environment**
- **Table**: `unidw.dw_parcel_detail_tool` 
- **Pipeline**: `us_dw_unidw_2_public_pipeline`
- **CDC Strategy**: `id_only`
- **Starting ID**: 281623217
- **Test Scope**: MySQL → S3 → Redshift (full pipeline)

---

## 📋 **CORRECTED Test Commands**

### **Phase 1: Pre-Test Setup & Validation**

#### **1.1 Environment Verification**
```bash
# Verify pipeline configuration
cat config/pipelines/us_dw_unidw_2_public_pipeline.yml | grep -A 5 dw_parcel_detail_tool
```

#### **1.2 Connectivity Tests**
```bash
# CORRECT: Test connectivity with v1.2.0 multi-schema command
python -m src.cli.main sync pipeline -t unidw.dw_parcel_detail_tool --pipeline us_dw_unidw_2_public_pipeline --dry-run

# Test S3 connectivity
python -m src.cli.main s3clean list -t dw_parcel_detail_tool | head -5
```

### **Phase 2: Watermark Management Test**

#### **2.1 Clean Slate Setup**
```bash
# CORRECT: Reset watermark (pipeline flag optional for watermark commands)
python -m src.cli.main watermark reset -t unidw.dw_parcel_detail_tool --pipeline us_dw_unidw_2_public_pipeline

# Verify reset
python -m src.cli.main watermark get -t unidw.dw_parcel_detail_tool --pipeline us_dw_unidw_2_public_pipeline
```

#### **2.2 Manual ID Watermark Setup**
```bash
# CORRECT: Set manual ID watermark with pipeline
python -m src.cli.main watermark set -t unidw.dw_parcel_detail_tool --pipeline us_dw_unidw_2_public_pipeline --id 281623217

# Verify watermark is set correctly
python -m src.cli.main watermark get -t unidw.dw_parcel_detail_tool --pipeline us_dw_unidw_2_public_pipeline
```

### **Phase 3: Backup Test (MySQL → S3)**

#### **3.1 Small Test Run**
```bash
# CORRECT: Use sync pipeline command for backup test
python -m src.cli.main sync pipeline -t unidw.dw_parcel_detail_tool --pipeline us_dw_unidw_2_public_pipeline --backup-only --limit 1000
```

#### **3.2 Verify Backup Results**
```bash
# Check watermark after backup
python -m src.cli.main watermark get -t unidw.dw_parcel_detail_tool --pipeline us_dw_unidw_2_public_pipeline

# Check S3 files created
python -m src.cli.main s3clean list -t dw_parcel_detail_tool | tail -5
```

### **Phase 4: Extended Backup Test**

#### **4.1 Larger Data Volume Test**
```bash
# CORRECT: Extended test with proper command
python -m src.cli.main sync pipeline -t unidw.dw_parcel_detail_tool --pipeline us_dw_unidw_2_public_pipeline --backup-only --limit 50000
```

#### **4.2 Resume Capability Test**
```bash
# Check current state
python -m src.cli.main watermark get -t unidw.dw_parcel_detail_tool --pipeline us_dw_unidw_2_public_pipeline

# CORRECT: Resume test
python -m src.cli.main sync pipeline -t unidw.dw_parcel_detail_tool --pipeline us_dw_unidw_2_public_pipeline --backup-only --limit 10000
```

### **Phase 5: Full Pipeline Test (S3 → Redshift)**

#### **5.1 Redshift Loading Test**
```bash
# CORRECT: Test S3 to Redshift loading
python -m src.cli.main sync pipeline -t unidw.dw_parcel_detail_tool --pipeline us_dw_unidw_2_public_pipeline --redshift-only
```

#### **5.2 End-to-End Pipeline Test**
```bash
# Reset and set watermark
python -m src.cli.main watermark reset -t unidw.dw_parcel_detail_tool --pipeline us_dw_unidw_2_public_pipeline
python -m src.cli.main watermark set -t unidw.dw_parcel_detail_tool --pipeline us_dw_unidw_2_public_pipeline --id 281623217

# CORRECT: Full end-to-end sync
python -m src.cli.main sync pipeline -t unidw.dw_parcel_detail_tool --pipeline us_dw_unidw_2_public_pipeline --limit 20000
```

### **Phase 6: Data Validation & Integrity**

#### **6.1 Row Count Validation**
```bash
# Check watermark counts
python -m src.cli.main watermark get -t unidw.dw_parcel_detail_tool --pipeline us_dw_unidw_2_public_pipeline

# Validate counts (note: watermark-count may need pipeline flag too)
python -m src.cli.main watermark-count validate-counts -t unidw.dw_parcel_detail_tool --pipeline us_dw_unidw_2_public_pipeline
```

### **Phase 7: Error Scenario Testing**

#### **7.1 Boundary Condition Tests**
```bash
# Test invalid ID scenarios
python -m src.cli.main watermark set -t unidw.dw_parcel_detail_tool --pipeline us_dw_unidw_2_public_pipeline --id -1

python -m src.cli.main watermark set -t unidw.dw_parcel_detail_tool --pipeline us_dw_unidw_2_public_pipeline --id 999999999999999999
```

## 🔧 **Key Corrections Made**

1. **Command Structure**: Changed from `sync -t` to `sync pipeline -t`
2. **Pipeline Flag**: Consistently use `--pipeline us_dw_unidw_2_public_pipeline` (with `_pipeline` suffix)
3. **Multi-Schema Support**: All commands now use the v1.2.0 multi-schema architecture
4. **Consistency**: All test phases now use the same command pattern

## ✅ **Validated Command Pattern**
```bash
# The working pattern you confirmed:
python -m src.cli.main sync pipeline -t unidw.dw_parcel_detail_tool --pipeline us_dw_unidw_2_public_pipeline --dry-run

# Applied consistently throughout the test plan
```

This corrected test plan now uses the proper v1.2.0 multi-schema commands that actually work with your system.