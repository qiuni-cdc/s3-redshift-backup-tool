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

#### **Critical Lessons Learned**

**🔴 ARCHITECTURAL PRINCIPLES**
1. **Single Source of Truth**: Critical systems must have exactly one authoritative schema source
2. **Interface Consistency**: All components consuming shared data must use identical APIs
3. **Schema Evolution**: Hardcoded schemas become "ticking time bombs" as databases evolve
4. **Early Detection Failure**: Architectural flaws can remain hidden when system scope is limited

**🔴 PRODUCTION DEBUGGING METHODOLOGY**
1. **Multi-Component Analysis**: Schema errors require checking ALL pipeline stages, not just one
2. **API Contract Verification**: Always verify actual return types when refactoring components
3. **Cross-Stage Consistency**: Backup → Upload → Loading must use identical schema discovery
4. **Runtime Validation**: Production systems need loud warnings for deprecated/dangerous functions

**🔴 PREVENTION MEASURES**
- **Unified Schema Architecture**: All components now use `FlexibleSchemaManager` exclusively
- **Schema Drift Detection**: Built-in utilities to detect hardcoded vs dynamic schema mismatches
- **API Testing**: Cross-component schema consistency verification
- **Deprecation Warnings**: Legacy functions generate runtime warnings to prevent silent usage

#### **User Recovery Process**
```bash
# The fix is already deployed - no user action required
# If you encounter schema-related errors:
# 1. Check logs for deprecation warnings about hardcoded schemas
# 2. Verify all components use FlexibleSchemaManager
# 3. Run schema drift detection: python -m src.utils.schema_migration
```

**Status**: ✅ **RESOLVED** - All components unified under single schema system. Production Parquet compatibility issues eliminated.

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

---

## 🚀 **DEVELOPMENT ROADMAP - v2.0 Enterprise Data Platform**

### **📊 Current Status: v1.0.0 Production Ready (August 2025)**

**✅ Production Achievements:**
- **Battle-Tested Scale**: Successfully processes 65M+ row tables  
- **Zero Data Loss**: All critical P0/P1/P2 bugs resolved
- **Enterprise Features**: SSH tunnels, credential security, comprehensive CLI
- **Proven Reliability**: 5.5M+ rows processed in production workloads
- **Complete Documentation**: User guides, technical references, operational procedures

### **🎯 Vision: Enterprise Data Integration Platform**

**Transform from:** Specialized MySQL→S3→Redshift backup tool  
**Transform to:** Comprehensive multi-source data integration platform

### **📈 Evolution Phases**

#### **Phase 1: Multi-Schema Foundation (v1.1.0) - Q4 2025**

**🎯 Goal:** Support multiple database connections while preserving v1.0.0 compatibility

**Key Features:**
- **Connection Registry**: Pooled multi-database connection management
- **Enhanced CLI**: Pipeline-based commands (`--pipeline sales_pipeline --table customers`)  
- **Configuration Layer**: YAML-based connection and table definitions
- **Backward Compatibility**: All v1.0.0 commands continue working unchanged

**Technical Implementation:**
```yaml
# config/pipelines/sales_pipeline.yml
pipeline:
  name: "sales_to_reporting"
  source: "sales_mysql"
  target: "reporting_redshift"
  
tables:
  customers:
    cdc_strategy: "hybrid"
    cdc_timestamp_column: "updated_at"
    cdc_id_column: "customer_id"
```

**Success Metrics:**
- Support 5+ database connections simultaneously
- 100% v1.0.0 workflow compatibility maintained
- Configuration-driven table processing

#### **Phase 2: CDC Intelligence Engine (v1.2.0) - Q1 2026**

**🎯 Goal:** Flexible change data capture supporting multiple strategies beyond hardcoded `update_at`

**Key Features:**
- **Dynamic CDC Strategies**: Hybrid (timestamp+ID), full_sync, id_only, timestamp_only
- **Data Quality Framework**: Built-in validation (null_check, duplicate_check, range_check)
- **Enhanced Watermark System**: Multi-pipeline support with conflict resolution
- **Custom SQL Support**: User-defined incremental queries

**CDC Strategy Engine:**
```python
CDC_STRATEGIES = {
    'hybrid': HybridCDCStrategy,        # Most robust: timestamp + ID
    'timestamp_only': TimestampCDCStrategy,  # v1.0.0 compatibility
    'id_only': IdOnlyCDCStrategy,       # Append-only tables
    'full_sync': FullSyncStrategy,      # Complete refresh
    'custom_sql': CustomSQLStrategy     # User-defined queries
}
```

**Success Metrics:**
- Support 4+ CDC strategies for different table patterns
- Built-in data quality validation on all pipelines
- 90% reduction in data consistency issues

#### **Phase 3: Dimensional Intelligence (v1.3.0) - Q2 2026**

**🎯 Goal:** Support dimensional data with Slowly Changing Dimension (SCD) patterns

**Key Features:**
- **SCD Type 1 & Type 2**: Automated historical change tracking
- **Advanced MERGE Operations**: Redshift-native dimensional processing
- **Business Key Management**: Configurable natural key relationships
- **Change Detection**: Automated identification of dimensional changes

**SCD Processing:**
```python
class SCDProcessor:
    """Handles SCD Type 1, Type 2, and hybrid patterns"""
    
    def process_dimension(self, staging_data, dimension_config, redshift_connection):
        # Advanced SCD processing with configurable business keys
        # Multi-statement transaction with staging table strategy
```

**Configuration Example:**
```yaml
tables:
  dim_customers:
    table_type: "dimension"
    scd_type: "type_2"
    business_key: ["customer_id"]
    scd_columns: ["customer_name", "address", "tier"]
    surrogate_key: "customer_sk"
```

**Success Metrics:**
- Support SCD Type 1 and Type 2 processing
- Automated dimensional change tracking
- 95% reduction in manual dimensional data management

#### **Phase 4: Enterprise Platform (v2.0.0) - Q3-Q4 2026**

**🎯 Goal:** Full enterprise data integration platform with orchestration

**Key Features:**
- **Pipeline Orchestration**: Dependency management and parallel execution
- **Enterprise Monitoring**: Prometheus/CloudWatch integration with dashboards
- **Data Lineage**: End-to-end tracking of data flow and transformations
- **Multi-Tenancy**: Support for multiple business units and projects

**Advanced Architecture:**
```python
class PipelineRunner:
    """Enterprise orchestration with dependency management"""
    
    def execute_pipeline(self, pipeline_config):
        # 1. Build dependency graph
        # 2. Parallel execution where possible
        # 3. Data lineage tracking
        # 4. Enterprise monitoring integration
```

**Enterprise Metrics:**
```python
class MetricsCollector:
    """Integration with Prometheus/CloudWatch"""
    metrics = {
        'rows_processed_total': Counter('rows_processed_total'),
        'pipeline_duration_seconds': Histogram('pipeline_duration_seconds'),
        'data_quality_score': Gauge('data_quality_score'),
        'scd_changes_detected': Counter('scd_changes_detected'),
    }
```

**Success Metrics:**
- Support 10+ source databases simultaneously
- Process 1B+ rows daily across all pipelines
- 99.9% uptime for critical data flows
- 5+ business units using the platform

### **🛡️ Migration Strategy**

#### **Risk Mitigation Principles:**
1. **Backward Compatibility First**: v1.0.0 workflows must continue working
2. **Gradual Enhancement**: Add capabilities without breaking existing features  
3. **Feature Flags**: Enable/disable new functionality during transition
4. **Rollback Capability**: Quick return to previous behavior if needed

#### **Migration Path:**
- **Month 1-2**: Phase 1 implementation with comprehensive v1.0.0 regression testing
- **Month 3-4**: Phase 2 rollout with opt-in CDC strategies
- **Month 5-6**: Phase 3 SCD features for new dimensional use cases
- **Month 7-12**: Phase 4 enterprise features with full platform capabilities

### **📊 Expected ROI**

**Technical Benefits:**
- **10x Scale**: Support hundreds of tables across multiple databases
- **70% Development Efficiency**: Reduction in custom ETL development
- **50% Operational Overhead**: Reduction in pipeline maintenance
- **3x Time-to-Market**: Faster deployment of new data products

**Business Value:**
- **Multi-Project Support**: Single platform serves entire organization
- **Data Governance**: Built-in lineage and quality tracking
- **Operational Excellence**: Automated monitoring and alerting
- **Cost Optimization**: Consolidated tooling and reduced maintenance

### **🎯 Success Criteria**

**v2.0.0 Platform Goals:**
- Support 10+ source databases simultaneously  
- Process 1B+ rows daily across all pipelines
- 99.9% uptime for critical business data flows
- 95% user satisfaction score from data engineering teams
- 100+ tables under automated management
- 5+ business units actively using the platform

### **📚 Reference Documents**

- **Technical Architecture**: `NEXT_GENERATION_ARCHITECTURE_DESIGN.md`
- **Feature Analysis**: `ENHANCED_FEATURES_DESIGN.md` 
- **v1.0.0 Foundation**: `RELEASE_v1.0.0_SUMMARY.md`
- **Production Lessons**: Current bug fixes and operational experience

---

**🌟 Strategic Vision**: Transform battle-tested v1.0.0 backup system into comprehensive enterprise data integration platform while preserving the reliability and performance that made it production-ready.

**Next Steps:** Stakeholder review, proof-of-concept development, and detailed sprint planning for Phase 1 implementation.

---

## 🧠 **CRITICAL ARCHITECTURAL CONSISTENCY PRINCIPLES (v1.2 Lessons Learned)**

### 🚨 **Anti-Patterns to NEVER Repeat**
- **Duplicated Logic Syndrome**: Same operation implemented in multiple places
- **Schema Fragmentation**: Different precision/format handling across components
- **Implicit Contracts**: Assuming method behavior without validation
- **Metadata Contamination**: Allowing S3 paths, file references in schemas
- **Integration Test Gaps**: Testing components in isolation only

### ✅ **Required Patterns for All Architecture Work**
- **Single Source of Truth**: Shared concepts must have exactly one canonical implementation
- **Consistency Over Complexity**: Most bugs stem from inconsistent implementation, not design flaws
- **Integration Testing First**: Test component interactions, not just individual components
- **Metadata Contamination Prevention**: Always sanitize outputs for downstream compatibility
- **Central Authority Pattern**: Use centralized utilities for shared operations (table naming, schema cleaning)

### 🔍 **Mandatory Checks Before Architectural Changes**
1. Identify all shared concepts (table naming, schema handling, etc.)
2. Ensure single canonical implementation for each shared concept
3. Validate component interface contracts explicitly
4. Test cross-component interactions with real data
5. Check for metadata contamination risks

### 🚨 **Red Flags That Require Immediate Attention**
- Multiple implementations of same logical operation
- Different components handling same data with different logic
- Schema/naming inconsistencies between pipeline stages
- Metadata that could contaminate downstream systems
- New table name cleaning logic (should use central authority)
- Schema handling without cross-component validation
- Parquet generation without metadata sanitization

### 📋 **AI Assistant Memory Integration**
- **Read First**: Always read `LESSONS_LEARNED_V1_2_ARCHITECTURE.md` before architectural work
- **Apply Principles**: Use consistency-first thinking, not just feature-first thinking
- **Validate Integration**: Test cross-component interactions with real data
- **Document Patterns**: Update lessons learned when new patterns emerge