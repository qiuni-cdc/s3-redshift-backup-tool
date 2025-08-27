# Migration Guide: v1.0.0 → v1.1.0 Multi-Schema

**Target Audience:** Production users migrating from v1.0.0 to v1.1.0  
**Migration Complexity:** Low (Zero breaking changes)  
**Estimated Time:** 15-30 minutes  

---

## 🎯 **Migration Overview**

**✅ ZERO BREAKING CHANGES**: v1.1.0 is fully backward compatible with v1.0.0

Your existing workflows will continue working exactly as before. This migration is **optional** and enables additional capabilities without disrupting current operations.

### **What Stays the Same**
- All v1.0.0 CLI commands work unchanged
- Same performance and reliability 
- Same watermark system and S3 management
- Same configuration files and environment variables
- Same backup strategies and processing logic

### **What's New (Optional)**
- Multiple database connection support
- Pipeline-based configuration management
- Environment-specific settings
- Advanced connection health monitoring
- Enhanced CLI with multi-schema commands

---

## 📋 **Pre-Migration Checklist**

### **✅ Required: Verify Current System**
```bash
# 1. Verify v1.0.0 is working correctly
python -m src.cli.main sync -t settlement.table_name --limit 100

# 2. Check current watermark status
python -m src.cli.main watermark get -t settlement.table_name

# 3. Verify S3 access
python -m src.cli.main s3clean list -t settlement.table_name

# 4. Check current configuration
ls -la .env
echo "MYSQL_HOST: ${MYSQL_HOST}"
echo "REDSHIFT_HOST: ${REDSHIFT_HOST}"
```

### **✅ Optional: Backup Current State**
```bash
# Backup environment configuration
cp .env .env.backup

# Backup any custom scripts or configurations
tar -czf v1_0_0_backup_$(date +%Y%m%d).tar.gz .env scripts/ docs/
```

---

## 🔄 **Migration Steps**

### **Step 1: Enable v1.1.0 Features (Optional)**

You can choose to:
1. **Keep v1.0.0 mode**: Do nothing, everything continues working
2. **Enable v1.1.0 features**: Follow steps below for enhanced capabilities

To enable v1.1.0 features:

```bash
# Option A: Set environment variable (temporary)
export ENABLE_MULTI_SCHEMA=true

# Option B: Add to .env file (permanent)
echo "ENABLE_MULTI_SCHEMA=true" >> .env

# Option C: Create configuration structure
python -m src.cli.main multi-schema setup
```

### **Step 2: Verify Backward Compatibility**

```bash
# Test that all v1.0.0 commands still work
python -m src.cli.main sync -t settlement.table_name --limit 10
python -m src.cli.main watermark get -t settlement.table_name
python -m src.cli.main s3clean list -t settlement.table_name

# Check system status
python -m src.cli.main version
```

If any command fails, **immediately revert**:
```bash
unset ENABLE_MULTI_SCHEMA
# or remove from .env file
```

### **Step 3: Explore New Features (Optional)**

If Step 2 passed, explore v1.1.0 capabilities:

```bash
# Check available connections
python -m src.cli.main connections list

# View pipeline configurations
python -m src.cli.main config list-pipelines

# Test connection health
python -m src.cli.main connections test default

# View enhanced help
python -m src.cli.main sync --help
```

### **Step 4: Create Additional Configurations (Optional)**

Only if you want to use multi-database features:

```bash
# 1. Review default configuration
cat config/connections.yml

# 2. Add additional database connections as needed
# Edit config/connections.yml with your database details

# 3. Create pipeline configurations for new projects
# Copy and modify config_examples/sales_pipeline.yml

# 4. Test new configurations
python -m src.cli.main config validate-pipeline your_pipeline_name
```

---

## 🧪 **Testing Strategy**

### **Phase 1: Compatibility Testing**
```bash
# Test core v1.0.0 functionality
python -m src.cli.main sync -t settlement.table_name --limit 100

# Verify watermark consistency
python -m src.cli.main watermark get -t settlement.table_name

# Check S3 operations
python -m src.cli.main s3clean list -t settlement.table_name --dry-run
```

### **Phase 2: New Feature Testing (If Enabled)**
```bash
# Test connection management
python -m src.cli.main connections test --all

# Test pipeline validation
python -m src.cli.main config validate-pipeline default

# Test new sync syntax (should work same as v1.0.0)
python -m src.cli.main sync pipeline -p default -t settlement.table_name --limit 10
```

### **Phase 3: Production Validation**
```bash
# Run a small production sync
python -m src.cli.main sync -t settlement.small_table

# Verify results in Redshift
# Connect to Redshift and check row counts

# Run larger production sync if small test passes
python -m src.cli.main sync -t settlement.main_table --limit 50000
```

---

## 📊 **Migration Verification Checklist**

### **✅ v1.0.0 Compatibility Verified**
- [ ] All existing sync commands work unchanged
- [ ] Watermark commands function correctly
- [ ] S3 cleanup commands operate normally
- [ ] Performance matches v1.0.0 benchmarks
- [ ] No errors in application logs

### **✅ v1.1.0 Features Working (If Enabled)**
- [ ] `python -m src.cli.main version` shows v1.1.0 features
- [ ] `python -m src.cli.main connections list` shows default connections
- [ ] `python -m src.cli.main config list-pipelines` shows default pipeline
- [ ] Configuration validation passes
- [ ] New syntax works: `sync pipeline -p default -t table_name`

### **✅ Production Ready**
- [ ] Test sync completed successfully with actual data verification
- [ ] Redshift row counts match expectations
- [ ] Watermark tracking remains consistent
- [ ] No regression in processing speed or reliability

---

## 🚨 **Rollback Procedures**

If any issues occur, immediately rollback:

### **Quick Rollback**
```bash
# Disable v1.1.0 features
unset ENABLE_MULTI_SCHEMA
# OR remove from .env file

# Verify v1.0.0 mode restored
python -m src.cli.main version
python -m src.cli.main sync -t settlement.table_name --limit 10
```

### **Complete Rollback**
```bash
# Remove configuration directory (if created)
rm -rf config/

# Restore backup environment
cp .env.backup .env

# Verify complete v1.0.0 restoration
python -m src.cli.main sync -t settlement.table_name --limit 10
```

### **Rollback Verification**
```bash
# Ensure all v1.0.0 functionality works
python -m src.cli.main sync -t settlement.table_name --limit 100
python -m src.cli.main watermark get -t settlement.table_name
python -m src.cli.main s3clean list -t settlement.table_name
```

---

## 💡 **Best Practices**

### **Recommended Migration Approach**

1. **Week 1: Planning**
   - Review this migration guide
   - Identify any custom workflows that need testing
   - Schedule maintenance window for testing

2. **Week 2: Testing**
   - Test v1.1.0 in non-production environment
   - Verify all existing workflows continue working
   - Test new features if planning to use them

3. **Week 3: Production Migration**
   - Enable v1.1.0 features during low-activity period
   - Run comprehensive compatibility testing
   - Monitor system for 24-48 hours

4. **Week 4: Optimization**
   - Configure additional databases/pipelines as needed
   - Optimize configurations based on usage patterns
   - Document any custom procedures

### **Risk Mitigation**

- **Start with read-only operations**: Test `connections list`, `config status`
- **Use dry-run flags**: Always test with `--dry-run` first
- **Verify data integrity**: Always check actual Redshift row counts
- **Monitor performance**: Ensure no degradation in processing speed
- **Keep rollback ready**: Maintain ability to disable v1.1.0 features instantly

---

## 📞 **Support and Troubleshooting**

### **Common Issues**

**Issue**: "Multi-schema features not available"
```bash
# Solution: Verify dependencies and enable flag
python -m src.cli.main multi-schema info
export ENABLE_MULTI_SCHEMA=true
```

**Issue**: "Configuration validation failed"
```bash
# Solution: Check configuration syntax
python -m src.cli.main config validate-pipeline default
# Fix any YAML syntax errors in config files
```

**Issue**: "Connection test failed"
```bash
# Solution: Verify environment variables
python -m src.cli.main connections test default
# Check .env file for correct database credentials
```

### **Getting Help**

1. **Check version and features**:
   ```bash
   python -m src.cli.main version
   python -m src.cli.main features
   ```

2. **Review configuration status**:
   ```bash
   python -m src.cli.main config status
   python -m src.cli.main connections health
   ```

3. **Enable debug logging**:
   ```bash
   python -m src.cli.main --debug sync -t table_name --limit 1
   ```

---

## 🎉 **Migration Complete**

**Congratulations!** You've successfully migrated to v1.1.0 while maintaining full v1.0.0 compatibility.

### **Next Steps**

- **Immediate**: Continue using all v1.0.0 commands as usual
- **Optional**: Explore new multi-schema features for additional projects
- **Future**: Consider v1.2.0 CDC enhancements when available

### **Key Benefits Unlocked**

- ✅ **Future-ready**: Foundation for v1.2.0+ advanced features
- ✅ **Scalability**: Ready to handle multiple database projects
- ✅ **Flexibility**: Configuration-driven approach
- ✅ **Reliability**: Same battle-tested v1.0.0 core with enhancements

**Your v1.0.0 workflows remain unchanged and fully supported!**