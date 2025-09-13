# Changelog

All notable changes to the S3-Redshift Backup System will be documented in this file.

## [Latest] - 2025-09-12

### 🎉 Major Features Added

#### ⭐ Target Table Name Mapping
- **Feature**: Map MySQL source tables to different Redshift target table names
- **Configuration**: Add `target_name` field to pipeline configuration
- **Usage**: `target_name: "custom_redshift_table_name"`
- **Benefits**: Flexible table naming, supports data lake architectures
- **Backward Compatible**: Existing configs work unchanged

#### ⭐ JSON Output Support  
- **Feature**: Machine-readable JSON output for automation
- **Usage**: Add `--json-output` flag to any sync command
- **Structure**: Complete execution metadata with success/failure status
- **Integration**: Perfect for CI/CD pipelines, monitoring, and Airflow
- **Exit Codes**: Proper exit codes (0/1) for automation scripts

#### ⭐ S3 Completion Markers
- **Feature**: S3-based completion tracking for workflow orchestration
- **Usage**: `--s3-completion-bucket bucket-name`
- **Files**: Creates execution_metadata.json, completion_marker.txt, table_metrics.json
- **Airflow Integration**: Native support for Airflow DAGs and sensors
- **Monitoring**: Detailed metrics for performance tracking

### 🚀 Performance Improvements

#### Batch Size Optimization
- **Change**: Increased hybrid pipeline batch size from 75k to 100k rows
- **Impact**: ~33% reduction in batch count for large tables
- **Benefit**: Improved throughput and resource efficiency
- **Pipeline**: `us_dw_hybrid_v1_2` configuration updated

### 🛠️ Development Experience

#### Simplified Watermark Locking
- **Change**: Disabled complex watermark locking for team development
- **Impact**: Eliminates lock contention during concurrent development  
- **Benefit**: Smoother team workflow and faster iteration
- **Note**: Production environments may need different locking strategy

### 🐛 Bug Fixes

#### Import Statement Fix
- **Fix**: Added missing `import os` to staged_backup_tool.py
- **Issue**: Runtime ImportError when accessing os module functions
- **Impact**: Resolves errors in staged backup functionality

### 📚 Documentation Updates

#### New Documentation Files
- **Added**: `docs/JSON_OUTPUT_FORMAT.md` - Complete JSON output specification
- **Added**: `docs/S3_COMPLETION_MARKERS.md` - S3 completion marker guide
- **Updated**: `USER_MANUAL.md` - Added new feature sections
- **Updated**: `README.md` - Added quick start for new features

#### Enhanced User Manual
- **Section**: Target Table Name Mapping with examples
- **Section**: JSON Output Format with CI/CD integration examples
- **Section**: S3 Completion Markers with Airflow integration
- **Examples**: Python and Bash automation scripts
- **Integration**: Monitoring and alerting examples

### 🔧 Technical Details

#### Target Table Mapping Implementation
- **Files Changed**: 
  - `src/core/gemini_redshift_loader.py` - Enhanced table name resolution
  - `src/cli/multi_schema_commands.py` - Updated dry-run preview
  - `config/pipelines/*.yml` - Added target_name support
- **Methods**: `_get_redshift_table_name()`, `_fix_ddl_table_name()`
- **Logging**: Enhanced visibility for table mapping decisions

#### JSON Output Implementation  
- **Files Changed**:
  - `src/cli/main.py` - Added --json-output flag and JSON generation
- **Structure**: Comprehensive execution metadata with stage-by-stage results
- **Integration**: Proper exit codes and structured error reporting

### 🎯 Usage Examples

#### Target Table Mapping
```yaml
# Pipeline Configuration
tables:
  unidw.dw_parcel_detail_tool:
    cdc_strategy: "id_only" 
    cdc_id_column: "id"
    target_name: "dw_parcel_detail_tool_new"
```

```bash
# Execution
python -m src.cli.main sync pipeline -p us_dw_unidw_2_public_pipeline -t unidw.dw_parcel_detail_tool
# Result: MySQL unidw.dw_parcel_detail_tool → Redshift dw_parcel_detail_tool_new
```

#### JSON Output  
```bash
# Automation-ready output
python -m src.cli.main sync pipeline -p pipeline -t table --json-output

# CI/CD Integration
sync_result=$(command --json-output)
success=$(echo "$sync_result" | jq '.success')
[ "$success" = "true" ] && echo "Success!" || echo "Failed!"
```

#### S3 Completion Markers
```bash
# Airflow Integration
python -m src.cli.main sync pipeline -p pipeline -t table \
  --json-output /tmp/result.json \
  --s3-completion-bucket airflow-completion-markers
```

### 🔄 Migration Notes

#### Existing Users
- **No Action Required**: All existing configurations continue to work
- **Optional Upgrade**: Add `target_name` fields for custom table mapping
- **New Features**: Available immediately with `--json-output` and `--s3-completion-bucket`

#### New Users  
- **Start Here**: Use latest pipeline configurations in `config/pipelines/`
- **Documentation**: Refer to updated `USER_MANUAL.md` for complete guide
- **Examples**: Check `docs/` directory for detailed integration examples

---

## Previous Versions

### [v1.2.0] - Previous Production Version
- Multi-schema support with pipeline configurations
- CDC strategy engine with hybrid, ID-only, and timestamp strategies  
- Flexible schema manager with dynamic discovery
- Watermark-based incremental processing
- S3 optimization with direct Parquet loading
- Comprehensive error handling and logging

### [v1.1.0] - Multi-Schema Architecture
- Pipeline-based configuration system
- Connection registry with SSH tunnel support
- Enhanced CDC strategies and validation
- S3 file management and cleanup tools

### [v1.0.0] - Initial Production Release
- Basic MySQL to S3 to Redshift pipeline
- Sequential and inter-table backup strategies
- Watermark management system
- Configuration-based setup