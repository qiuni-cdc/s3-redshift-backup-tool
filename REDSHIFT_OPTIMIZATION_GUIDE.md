# Redshift Table Optimization Guide

Complete guide for optimizing Redshift table performance through DISTKEY, SORTKEY, and DISTSTYLE configuration.

---

## Table of Contents

1. [Overview](#overview)
2. [Latest Updates (v2.0+)](#latest-updates-v20)
3. [Understanding DISTKEY and SORTKEY](#understanding-distkey-and-sortkey)
4. [Configuration File Setup](#configuration-file-setup)
5. [Configuration Options](#configuration-options)
6. [Default Behavior](#default-behavior)
7. [Adding Custom Configurations](#adding-custom-configurations)
8. [Examples and Scenarios](#examples-and-scenarios)
9. [Best Practices](#best-practices)
10. [Troubleshooting](#troubleshooting)
11. [Monitoring and Validation](#monitoring-and-validation)

---

## Overview

This system provides **flexible Redshift table optimization** with three approaches:

1. **Custom Configuration** - Full manual control via `redshift_keys.json`
2. **Special Handling** - Optimized settings for `settle_orders` table
3. **AUTO Optimization** - Redshift manages optimization for all other tables

**Key Benefits:**
- ✅ Independent control over DISTSTYLE and SORTKEY
- ✅ Backward compatible with existing configurations
- ✅ Simple, predictable defaults for new tables
- ✅ Production-ready with AWS-proven AUTO features

---

## Latest Updates (v2.0+)

### 🆕 **Simplified AUTO Strategy**

**Major Change:** All tables (except `settle_orders`) now automatically get **both `DISTSTYLE AUTO` and `SORTKEY AUTO`** when no custom configuration is provided.

#### **What Changed:**

| Table Type | OLD (v1.x) | NEW (v2.0+) |
|------------|------------|-------------|
| `settle_orders` | Special handling | ✅ **Same (unchanged)** |
| Tables with timestamps | `DISTKEY(id)` + `SORTKEY(timestamps)` | **`DISTSTYLE AUTO` + `SORTKEY AUTO`** |
| Tables with PKs | `DISTKEY(pk)` + detected sortkey | **`DISTSTYLE AUTO` + `SORTKEY AUTO`** |
| Other tables | Complex detection | **`DISTSTYLE AUTO` + `SORTKEY AUTO`** |

#### **Why This Change:**

- ✅ **Simpler** - No complex column detection logic
- ✅ **Predictable** - Same behavior for all non-`settle_orders` tables
- ✅ **Adaptive** - Redshift learns from actual query patterns
- ✅ **Battle-tested** - AWS-proven at massive scale

---

## Understanding DISTKEY and SORTKEY

### **DISTKEY (Distribution Key)**

Determines how data is distributed across compute nodes in a Redshift cluster.

**Distribution Styles:**

| Style | Description | Best For |
|-------|-------------|----------|
| **AUTO** | Redshift automatically determines distribution | New tables, unknown patterns |
| **ALL** | Entire table replicated to every node | Small dimension tables (< 1M rows) |
| **EVEN** | Round-robin distribution | Tables without clear join patterns |
| **KEY** | Distributed based on column values | Large fact tables with frequent joins |

**How it works:**
- Rows with the same DISTKEY value are stored on the same node
- Enables local joins without data redistribution
- Critical for large table JOIN performance

**Choosing DISTKEY:**
- Use columns frequently used in JOIN conditions
- Choose high cardinality columns (many unique values)
- Avoid columns with skewed data distribution

---

### **SORTKEY (Sort Key)**

Defines the physical order in which data is stored within each node.

**Sort Key Types:**

| Type | Description | Best For |
|------|-------------|----------|
| **AUTO** | Redshift automatically determines sort order | Unknown query patterns |
| **COMPOUND** | Sorted by column order (default) | Sequential column filtering |
| **INTERLEAVED** | Equal weight to all columns | Multi-column filtering (more overhead) |

**How it works:**
- Redshift uses zone maps to skip irrelevant data blocks
- Reduces I/O by reading only relevant data
- Critical for WHERE clause and range query performance

**Choosing SORTKEY:**
- Use columns in WHERE clauses for filtering
- Timestamp columns for time-range queries
- Columns used in GROUP BY or ORDER BY
- Max 1-2 columns for optimal performance

---

## Configuration File Setup

### **Location**

Create `redshift_keys.json` in your project root:

```
/your/project/root/redshift_keys.json
```

### **Format**

```json
{
  "schema.table_name": {
    "distkey": "column_name",
    "sortkey": ["column1", "column2"]
  }
}
```

### **How It Works**

1. **Automatic Loading**: `FlexibleSchemaManager` loads the file during initialization
2. **Exact Matching**: Uses full table names (e.g., `settlement.settle_orders`)
3. **Column Validation**: Verifies columns exist before applying
4. **Priority System**: Custom configs always override defaults

---

## Configuration Options

### **1. Full AUTO Optimization**

Let Redshift manage everything:

```json
{
  "unidw.dw_parcel_pricing_temp": {
    "diststyle": "AUTO",
    "sortkey": "AUTO"
  }
}
```

**Generates:**
```sql
CREATE TABLE dw_parcel_pricing_temp (...)
DISTSTYLE AUTO
SORTKEY AUTO;
```

**Use when:** Unknown query patterns, development environments, new tables

---

### **2. Manual DISTKEY + AUTO SORTKEY**

Control distribution, let Redshift handle sorting:

```json
{
  "settlement.orders": {
    "distkey": "customer_id",
    "sortkey": "AUTO"
  }
}
```

**Generates:**
```sql
CREATE TABLE orders (...)
DISTKEY(customer_id)
SORTKEY AUTO;
```

**Use when:** Clear join patterns but unknown sort patterns

---

### **3. Full Manual Control**

Explicit configuration for everything:

```json
{
  "settlement.parcel_detail": {
    "distkey": "parcel_id",
    "sortkey": ["create_at", "parcel_id"]
  }
}
```

**Generates:**
```sql
CREATE TABLE parcel_detail (...)
DISTKEY(parcel_id)
COMPOUND SORTKEY(create_at, parcel_id);
```

**Use when:** Well-understood query patterns, performance-critical tables

---

### **4. DISTSTYLE ALL (Dimension Tables)**

Replicate small tables to all nodes:

```json
{
  "settlement.dim_product": {
    "diststyle": "ALL",
    "sortkey": ["product_id"]
  }
}
```

**Generates:**
```sql
CREATE TABLE dim_product (...)
DISTSTYLE ALL
SORTKEY(product_id);
```

**Use when:** Small lookup tables (< 1M rows), frequently joined

---

### **5. Interleaved SORTKEY**

Equal weight to all sort columns:

```json
{
  "settlement.parcel_detail": {
    "distkey": "parcel_id",
    "interleaved_sortkey": ["parcel_id", "create_at", "status"]
  }
}
```

**Generates:**
```sql
CREATE TABLE parcel_detail (...)
DISTKEY(parcel_id)
INTERLEAVED SORTKEY(parcel_id, create_at, status);
```

**Use when:** Multiple WHERE clause patterns, complex filtering

**⚠️ Warning:** Interleaved sortkeys have higher maintenance overhead. Consider COMPOUND or AUTO for most use cases.

---

### **All Configuration Options Reference**

Quick reference table for all available options:

#### **DISTKEY Options**

| Option | Value | Result | Use Case |
|--------|-------|--------|----------|
| `diststyle` | `"AUTO"` | `DISTSTYLE AUTO` | Unknown patterns, new tables |
| `diststyle` | `"ALL"` | `DISTSTYLE ALL` | Small dimension tables (< 1M rows) |
| `diststyle` | `"EVEN"` | `DISTSTYLE EVEN` | No clear join patterns |
| `distkey` | `"column_name"` | `DISTKEY(column_name)` | Explicit distribution key |

#### **SORTKEY Options**

| Option | Value | Result | Use Case |
|--------|-------|--------|----------|
| `sortkey` | `"AUTO"` | `SORTKEY AUTO` | Unknown query patterns |
| `sortkey` | `"column_name"` | `SORTKEY(column_name)` | Single sort column |
| `sortkey` | `["col1", "col2"]` | `COMPOUND SORTKEY(col1, col2)` | Sequential filtering |
| `interleaved_sortkey` | `["col1", "col2"]` | `INTERLEAVED SORTKEY(col1, col2)` | Multi-column filtering (overhead!) |

---

## Default Behavior

### **Optimization Strategy**

When a table is **NOT** in `redshift_keys.json`:

| Table Pattern | DISTKEY | SORTKEY | Why |
|---------------|---------|---------|-----|
| `settle_orders` | `tracking_number` | `tracking_number, create_at` | Business-critical table |
| **All other tables** | **`DISTSTYLE AUTO`** | **`SORTKEY AUTO`** | Let Redshift optimize |

### **Decision Tree**

```
Table optimization logic:
│
├─ In redshift_keys.json?
│  ├─ YES → Use custom config (highest priority)
│  └─ NO ↓
│
└─ Default Strategy:
   │
   ├─ Is table "settle_orders"?
   │  ├─ YES → Special handling:
   │  │        DISTKEY(tracking_number)
   │  │        SORTKEY(tracking_number, create_at)
   │  │
   │  └─ NO → Use AUTO:
   │           DISTSTYLE AUTO
   │           SORTKEY AUTO
```

### **Configuration Priority**

The system applies optimization in this order:

1. **Explicit Custom Config** (highest priority)
   - `redshift_keys.json` entries always take precedence
   - Full control over DISTKEY, SORTKEY, DISTSTYLE

2. **settle_orders Special Handling** (medium priority)
   - Only applies if table name contains "settle_orders"
   - Uses explicit DISTKEY(tracking_number) and SORTKEY optimization

3. **AUTO Optimization** 🆕 (default for all other tables)
   - `DISTSTYLE AUTO` - Redshift manages distribution automatically
   - `SORTKEY AUTO` - Redshift manages sort order automatically
   - Applies to all tables not covered by #1 or #2

---

### **When Default Logic Applies**

When tables are **NOT** listed in `redshift_keys.json`, the system applies default logic:

**Example Configuration:**
```json
{
  "settlement.settle_orders": {
    "distkey": "tracking_number",
    "sortkey": ["tracking_number", "create_at"]
  },

  "settlement.payment_records": {
    "diststyle": "EVEN",
    "sortkey": "AUTO"
  }

  // Tables NOT listed here use default logic:
  // - settlement.order_items → DISTSTYLE AUTO + SORTKEY AUTO
  // - kuaisong.shipments → DISTSTYLE AUTO + SORTKEY AUTO
  // - unidw.parcel_detail → DISTSTYLE AUTO + SORTKEY AUTO
}
```

**What happens for unlisted tables:**
1. System checks if table name contains "settle_orders"
2. If YES → Apply special handling (DISTKEY + SORTKEY)
3. If NO → Use `DISTSTYLE AUTO` and `SORTKEY AUTO`
4. System logs which strategy was applied

---

### **Override Auto-Detection for settle_orders**

To explicitly use AUTO even for settle_orders tables:

```json
{
  "settlement.settle_orders": {
    "diststyle": "AUTO",
    "sortkey": "AUTO"
  }
}
```

This tells Redshift to manage optimization automatically instead of using the special settle_orders handling.

**Use this when:**
- You want Redshift to learn patterns for settle_orders too
- Testing AUTO optimization on business-critical tables
- Migrating from manual to AUTO strategy

---

## Adding Custom Configurations

### **Step 1: Identify Table Columns**

Connect to Redshift and examine your table:

```sql
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'your_table_name'
ORDER BY ordinal_position;
```

### **Step 2: Analyze Query Patterns**

Identify frequently used columns:

```sql
-- Check common JOIN patterns
SELECT query, querytxt
FROM stl_query
WHERE querytxt LIKE '%your_table_name%'
  AND querytxt LIKE '%JOIN%';

-- Check common WHERE clauses
SELECT query, querytxt
FROM stl_query
WHERE querytxt LIKE '%your_table_name%'
  AND querytxt LIKE '%WHERE%';
```

### **Step 3: Add to Configuration**

Edit `redshift_keys.json`:

```json
{
  "existing_tables": "...",

  "your_schema.your_table": {
    "distkey": "frequently_joined_column",
    "sortkey": ["filter_column", "timestamp_column"]
  }
}
```

### **Step 4: Apply Configuration**

For new tables, just sync:
```bash
python -m src.cli.main sync -t your_schema.your_table
```

For existing tables, recreate:
```sql
-- Backup data first!
CREATE TABLE your_table_backup AS SELECT * FROM your_table;

-- Drop and recreate
DROP TABLE your_table;

-- Sync to recreate with new optimization
python -m src.cli.main sync -t your_schema.your_table

-- Verify and cleanup backup
SELECT COUNT(*) FROM your_table;
DROP TABLE your_table_backup;
```

---

## Examples and Scenarios

### **Example 1: High-Traffic Transactional Table**

```json
{
  "orders.customer_orders": {
    "distkey": "customer_id",
    "sortkey": ["order_date", "customer_id"]
  }
}
```

**Rationale:**
- `customer_id` DISTKEY - Frequent joins with customer table
- `order_date` primary SORTKEY - Time-range queries common
- `customer_id` secondary SORTKEY - Customer-specific filtering

---

### **Example 2: Analytics/Reporting Table**

```json
{
  "analytics.daily_metrics": {
    "distkey": "date_partition",
    "sortkey": ["date_partition", "metric_type"]
  }
}
```

**Rationale:**
- `date_partition` DISTKEY - Date-based joins common
- Date-first SORTKEY - Reports always filter by date range
- `metric_type` secondary - Drill-down by metric type

---

### **Example 3: Small Dimension Table**

```json
{
  "reference.product_catalog": {
    "diststyle": "ALL",
    "sortkey": ["product_category", "product_id"]
  }
}
```

**Rationale:**
- `DISTSTYLE ALL` - Small table replicated to all nodes
- No DISTKEY needed with ALL
- Category-based filtering common

---

### **Example 4: Large Fact Table (100M+ Rows)**

```json
{
  "warehouse.order_items": {
    "distkey": "order_id",
    "sortkey": ["order_date", "product_id"]
  }
}
```

**Rationale:**
- `order_id` DISTKEY - Frequent joins with orders table
- `order_date` primary SORTKEY - Time-series analysis
- `product_id` secondary - Product-level filtering

---

### **Example 5: Unknown Pattern (Use AUTO)**

```json
{
  "staging.new_table": {
    "diststyle": "AUTO",
    "sortkey": "AUTO"
  }
}
```

**Rationale:**
- New table with unknown query patterns
- Let Redshift learn from actual usage
- Review performance after 1-2 weeks

---

### **Example 6: Mixed AUTO and Manual**

```json
{
  "settlement.settle_orders": {
    "distkey": "tracking_number",
    "sortkey": "AUTO"
  }
}
```

**Rationale:**
- Known join pattern (tracking_number)
- Unknown sort patterns - let Redshift learn

---

### **Example 7: Complete Mixed Configuration**

Real-world example with multiple strategies:

```json
{
  "settlement.settle_orders": {
    "distkey": "tracking_number",
    "sortkey": ["tracking_number", "create_at"],
    "_comment": "Business-critical table, explicit config"
  },

  "settlement.settlement_normal_delivery_detail": {
    "diststyle": "EVEN",
    "sortkey": ["create_at", "update_at"],
    "_comment": "No clear join pattern, use EVEN distribution"
  },

  "unidw.dw_parcel_pricing_temp": {
    "diststyle": "AUTO",
    "sortkey": "AUTO",
    "_comment": "Temporary table, full AUTO"
  },

  "settlement.parcel_detail": {
    "distkey": "parcel_id",
    "interleaved_sortkey": ["parcel_id", "create_at", "status"],
    "_comment": "Complex filtering patterns need interleaved"
  },

  "example.reporting_table": {
    "diststyle": "ALL",
    "sortkey": "AUTO",
    "_comment": "Small dimension table with AUTO sortkey"
  },

  "example.fact_table": {
    "distkey": "order_id",
    "sortkey": ["order_date", "customer_id"],
    "_comment": "Manual optimization for well-understood patterns"
  }
}
```

---

## Best Practices

### **DISTKEY Selection**

✅ **Do:**
- Choose columns with high cardinality (many unique values)
- Use columns in frequent JOIN conditions
- Match DISTKEY on both sides of large table joins
- Verify even data distribution

❌ **Don't:**
- Use columns with skewed data (80/20 distribution)
- Choose low cardinality columns (e.g., status flags)
- Use columns not in JOIN conditions

**Example:**
```
Good: customer_id, order_id, tracking_number
Bad: status, type, country (if skewed)
```

---

### **SORTKEY Selection**

✅ **Do:**
- Choose columns in WHERE clauses (filters)
- Use timestamp columns for time-series data
- Limit to 1-2 columns for best performance
- Put most selective column first

❌ **Don't:**
- Add many columns to SORTKEY (overhead increases)
- Use columns never in WHERE clauses
- Use random or unique columns

**Example:**
```
Good: ["created_at", "customer_id"]
Bad: ["id", "random_field", "unused_column"]
```

---

### **When to Use AUTO**

✅ **Use AUTO for:**
- New tables with unknown query patterns
- Development and staging environments
- Tables serving multiple use cases
- Tables with changing access patterns
- Tables where workload isn't established yet
- When you want Redshift to adapt to changing workloads

⚠️ **Use Manual for:**
- Production tables with established patterns
- Performance-critical tables (100M+ rows)
- Tables with specific join requirements
- Well-understood analytics workloads
- Tables with consistent, predictable access patterns
- Tables with specific join patterns (use matching DISTKEY)
- Dimension tables (use `DISTSTYLE ALL`)

---

### **Migration Path**

If you have existing configurations, they continue to work. You can gradually migrate to AUTO:

**Existing Configuration (still works):**
```json
{
  "settlement.settle_orders": {
    "distkey": "tracking_number",
    "sortkey": ["tracking_number", "create_at"]
  }
}
```

**Gradual Migration to AUTO:**
```json
{
  "settlement.settle_orders": {
    "distkey": "tracking_number",
    "sortkey": "AUTO"  // Let Redshift learn sort patterns
  }
}
```

**Full AUTO Migration:**
```json
{
  "settlement.settle_orders": {
    "diststyle": "AUTO",
    "sortkey": "AUTO"
  }
}
```

**Migration Strategy:**
1. Start with SORTKEY AUTO while keeping manual DISTKEY
2. Monitor query performance for 1-2 weeks
3. If performance is good, consider DISTSTYLE AUTO
4. Compare before/after metrics
5. Keep manual config for critical tables

---

### **DISTSTYLE Guidance**

| Table Size | Row Count | Recommendation |
|------------|-----------|----------------|
| Small | < 1M rows | `DISTSTYLE ALL` |
| Medium | 1M - 10M | `DISTSTYLE AUTO` or `KEY` |
| Large | 10M - 100M | `DISTKEY(join_column)` |
| Very Large | 100M+ | `DISTKEY(join_column)` + careful testing |

---

### **Performance Considerations**

1. **Monitor Query Performance**: Use Redshift query metrics
2. **Review Regularly**: Update optimizations based on workload changes
3. **Test Changes**: Always test with representative workloads
4. **Measure Impact**: Compare before/after query times
5. **Document Decisions**: Add comments in `redshift_keys.json`

**Example with comments:**
```json
{
  "warehouse.orders": {
    "distkey": "customer_id",
    "sortkey": ["order_date", "status"],
    "_comment": "customer_id matches customer table JOIN, order_date for time-range filters"
  }
}
```

---

## Troubleshooting

### **Issue 1: Column Not Found**

**Error:**
```
⚠️ Custom DISTKEY column 'column_name' not found in table schema
```

**Solution:**
- Check column name spelling (case-sensitive)
- Verify column exists in source MySQL table
- Check for typos in `redshift_keys.json`

---

### **Issue 2: JSON Syntax Error**

**Error:**
```
⚠️ Failed to load redshift_keys.json: JSON syntax error
```

**Solution:**
- Validate JSON syntax with online validator
- Check for missing commas, brackets
- Remove trailing commas in arrays

**Example fix:**
```json
// ❌ WRONG
{
  "table1": {...},
  "table2": {...},  // ← Trailing comma!
}

// ✅ CORRECT
{
  "table1": {...},
  "table2": {...}
}
```

---

### **Issue 3: Configuration Not Loading**

**Error:**
```
ℹ️ No redshift_keys.json file found, using default optimizations
```

**Solution:**
- Ensure file is in project root directory
- Check file permissions (readable)
- Verify exact filename (case-sensitive)

**Check location:**
```bash
ls -la /path/to/project/root/redshift_keys.json
```

---

### **Issue 4: Performance Not Improving**

**Symptoms:**
- Added custom DISTKEY/SORTKEY but queries still slow

**Diagnosis:**
```sql
-- Check if optimizations were applied
SELECT tablename, distkey, sortkey1, sortkey2
FROM pg_table_def
WHERE tablename = 'your_table';

-- Check data distribution skew
SELECT slice, COUNT(*)
FROM stv_blocklist
WHERE name = 'your_table'
GROUP BY slice
ORDER BY COUNT(*) DESC;
```

**Solution:**
- Verify optimizations were applied to actual table
- Check for data skew (uneven distribution)
- Review query execution plans (EXPLAIN)
- Consider different DISTKEY/SORTKEY columns

---

### **Issue 5: Interleaved SORTKEY Too Slow**

**Symptoms:**
- Table loads taking longer than expected
- VACUUM operations timing out

**Solution:**
- Switch from INTERLEAVED to COMPOUND sortkey
- Reduce number of sort columns
- Use AUTO instead

**Migration:**
```json
// Before (slow)
{
  "table": {
    "interleaved_sortkey": ["col1", "col2", "col3", "col4"]
  }
}

// After (faster)
{
  "table": {
    "sortkey": ["col1", "col2"]
  }
}
```

---

## Monitoring and Validation

### **Check Applied Optimizations**

```sql
-- View table distribution and sort keys
SELECT
    schemaname,
    tablename,
    diststyle,
    distkey,
    sortkey1,
    sortkey2
FROM pg_table_def
WHERE schemaname = 'your_schema'
  AND tablename = 'your_table';
```

**Expected output:**
```
schemaname | tablename | diststyle | distkey | sortkey1    | sortkey2
-----------+-----------+-----------+---------+-------------+---------
settlement | orders    | KEY       | 1       | order_date  | status
```

---

### **Check Data Distribution**

```sql
-- Verify even distribution across nodes
SELECT
    slice,
    COUNT(*) as blocks
FROM stv_blocklist
WHERE name = 'your_table'
GROUP BY slice
ORDER BY slice;
```

**Good distribution:**
```
slice | blocks
------|-------
0     | 1234
1     | 1235
2     | 1233
3     | 1234
```

**Bad distribution (skewed):**
```
slice | blocks
------|-------
0     | 4500  ← Problem!
1     | 100
2     | 98
3     | 102
```

---

### **Query Performance Analysis**

```sql
-- Analyze query execution plan
EXPLAIN
SELECT * FROM your_table
WHERE filter_column = 'value'
ORDER BY sort_column;
```

**Look for:**
- `DS_DIST_NONE` - No data redistribution (good!)
- `DS_DIST_ALL_INNER` - Broadcasting table (may need optimization)
- `DS_DIST_BOTH` - Redistributing both tables (problem!)

---

### **Monitor Query Times**

```sql
-- Check query performance over time
SELECT
    DATE_TRUNC('hour', starttime) as hour,
    COUNT(*) as queries,
    AVG(duration)/1000000.0 as avg_seconds,
    MAX(duration)/1000000.0 as max_seconds
FROM stl_query
WHERE querytxt LIKE '%your_table%'
  AND starttime > CURRENT_DATE - 7
GROUP BY 1
ORDER BY 1 DESC;
```

---

### **Validation Checklist**

After applying new optimizations:

- [ ] Verify DDL contains expected DISTKEY/SORTKEY
- [ ] Check data distribution is even (< 10% variance)
- [ ] Compare query times before/after
- [ ] Review EXPLAIN plans for problematic queries
- [ ] Monitor table load times
- [ ] Check VACUUM performance
- [ ] Document results in comments

---

## Complete Configuration Examples

### **Production Configuration Example**

```json
{
  "settlement.settle_orders": {
    "distkey": "tracking_number",
    "sortkey": ["tracking_number", "create_at"],
    "_comment": "Business-critical table, explicit config"
  },

  "settlement.settlement_normal_delivery_detail": {
    "distkey": "ant_parcel_no",
    "sortkey": ["billing_num", "create_at"],
    "_comment": "High volume, optimized for billing queries"
  },

  "settlement.dim_product": {
    "diststyle": "ALL",
    "sortkey": ["product_id"],
    "_comment": "Small dimension table, replicate to all nodes"
  },

  "warehouse.fact_sales": {
    "distkey": "order_id",
    "sortkey": ["sale_date", "product_id"],
    "_comment": "100M+ rows, optimized for date-range queries"
  },

  "staging.temp_import": {
    "diststyle": "AUTO",
    "sortkey": "AUTO",
    "_comment": "Temporary table, let Redshift optimize"
  },

  "analytics.customer_metrics": {
    "distkey": "customer_id",
    "sortkey": "AUTO",
    "_comment": "Known join pattern, unknown sort patterns"
  }
}
```

---

## Additional Resources

### **Log Messages to Look For**

**Success:**
```
✅ Loaded custom Redshift optimizations for X tables
✅ Using custom Redshift optimizations for table_name
✅ Applied custom DISTKEY: column_name
✅ Applied custom SORTKEY: column1, column2
✅ Using AUTO optimization (DISTSTYLE AUTO, SORTKEY AUTO)
```

**Warnings:**
```
⚠️ Custom DISTKEY column 'column_name' not found in table schema
⚠️ Custom SORTKEY column 'column_name' not found in table schema
⚠️ Failed to load redshift_keys.json: [error details]
```

---

## Feature Highlights

This optimization system provides:

✅ **Independent Control**: Set DISTKEY and SORTKEY separately
✅ **Backward Compatible**: All existing configurations still work
✅ **Flexible**: Mix AUTO and manual settings as needed
✅ **Simple**: Just use `"AUTO"` as a string value
✅ **Production Ready**: Validated and tested with AWS features
✅ **Adaptive**: Redshift learns from actual query patterns
✅ **Predictable**: Clear defaults for all table types
✅ **Battle-tested**: AWS-proven at massive scale

---

## Summary

This optimization system provides:

1. **Flexibility** - Choose AUTO, manual, or mixed approaches
2. **Simplicity** - Sensible defaults for new tables
3. **Control** - Full customization when needed
4. **Compatibility** - Works with existing configurations
5. **Performance** - Battle-tested AWS optimization features

**Quick Decision Guide:**

| Situation | Recommendation |
|-----------|----------------|
| New table | Let AUTO handle it |
| Known join pattern | Manual DISTKEY + AUTO SORTKEY |
| 100M+ rows | Full manual configuration |
| Small dimension | `DISTSTYLE ALL` |
| Changing workload | Use AUTO |
| Production critical | Manual with testing |

---

**Need Help?**

- Check logs for optimization messages
- Review query execution plans
- Monitor data distribution
- Test with representative workloads
- Document decisions in `redshift_keys.json` comments
