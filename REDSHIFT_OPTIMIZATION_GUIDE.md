# Redshift Table Optimization Configuration Guide

This guide shows how to configure Redshift table optimizations (DISTSTYLE, DISTKEY, SORTKEY) for maximum query performance, including dimension tables with `DISTSTYLE ALL`.

## Quick Start: Dimension Table Configuration

Create `redshift_keys.json` in your project root:

```json
{
  "settlement.dim_product": {
    "diststyle": "ALL",
    "sortkey": ["product_id"],
    "table_type": "dimension"
  }
}
```

The tool automatically generates optimized DDL:
```sql
CREATE TABLE settlement.dim_product (
    product_id BIGINT,
    product_name VARCHAR(510),
    category VARCHAR(200)
)
DISTSTYLE ALL              -- Replicated to all nodes
SORTKEY(product_id);       -- Fast primary key lookups
```

## Configuration File: `redshift_keys.json`

### Location
```
/home/qi_chen/s3-redshift-backup/redshift_keys.json
```

### Format
```json
{
    "table_name": {
        "distkey": "column_name",
        "sortkey": ["column1", "column2"]
    }
}
```

### Current Configuration
```json
{
    "settlement.settlement_normal_delivery_detail": {
        "distkey": "ant_parcel_no",
        "sortkey": ["billing_num", "create_at"]
    },
    "settlement.settle_orders": {
        "distkey": "tracking_number",
        "sortkey": ["tracking_number", "create_at"]
    }
}
```

## How It Works

1. **Automatic Loading**: The `FlexibleSchemaManager` automatically loads the `redshift_keys.json` file during initialization
2. **Table Matching**: Uses exact table name matching (e.g., `settlement.settle_orders`)
3. **Column Validation**: Verifies that specified columns exist in the table schema before applying
4. **Fallback**: If no custom configuration is found, uses intelligent defaults

## Configuration Options

### DISTKEY
- **Purpose**: Determines how data is distributed across Redshift nodes
- **Format**: Single column name (string)
- **Example**: `"distkey": "tracking_number"`
- **Best Practice**: Choose a column that's frequently used in JOIN conditions

### SORTKEY
- **Purpose**: Optimizes query performance by pre-sorting data
- **Format**: Array of column names (up to 2 columns)
- **Example**: `"sortkey": ["tracking_number", "create_at"]`
- **Best Practice**: Choose columns frequently used in WHERE clauses and ORDER BY

## Adding New Table Optimizations

### Step 1: Identify Table Columns
First, check what columns are available in your table:
```bash
# Connect to Redshift and examine table structure
SELECT column_name, data_type 
FROM information_schema.columns 
WHERE table_name = 'your_table_name'
ORDER BY ordinal_position;
```

### Step 2: Add Configuration
Edit `redshift_keys.json` and add your table configuration:
```json
{
    "existing_tables": "...",
    "your_schema.your_table": {
        "distkey": "your_distkey_column",
        "sortkey": ["your_primary_sortkey", "your_secondary_sortkey"]
    }
}
```

### Step 3: Recreate Table (if needed)
For existing tables, you may need to recreate them to apply new optimizations:
```sql
-- Drop existing table (backup data first!)
DROP TABLE your_table_name;

-- Run sync again to recreate with new optimizations
python -m src.cli.main sync -t your_schema.your_table
```

## Examples

### High-Traffic Transactional Table
```json
{
    "orders.customer_orders": {
        "distkey": "customer_id",
        "sortkey": ["order_date", "customer_id"]
    }
}
```

### Analytics/Reporting Table
```json
{
    "analytics.daily_metrics": {
        "distkey": "date_partition",
        "sortkey": ["date_partition", "metric_type"]
    }
}
```

### Lookup/Reference Table
```json
{
    "reference.product_catalog": {
        "distkey": "product_id",
        "sortkey": ["product_category", "product_id"]
    }
}
```

## Validation and Troubleshooting

### Check if Configuration is Loaded
Look for these log messages during table creation:
```
✅ "Loaded custom Redshift optimizations for X tables"
✅ "Using custom Redshift optimizations for table_name"
✅ "Applied custom DISTKEY: column_name"
✅ "Applied custom SORTKEY: column1, column2"
```

### Common Issues

1. **Column Not Found**
   ```
   ⚠️ "Custom DISTKEY column 'column_name' not found in table schema"
   ```
   **Solution**: Check column name spelling and case sensitivity

2. **JSON Syntax Error**
   ```
   ⚠️ "Failed to load redshift_keys.json: JSON syntax error"
   ```
   **Solution**: Validate JSON syntax using an online JSON validator

3. **File Not Found**
   ```
   ℹ️ "No redshift_keys.json file found, using default optimizations"
   ```
   **Solution**: Ensure file is in project root directory

## Best Practices

### DISTKEY Selection
- **High Cardinality**: Choose columns with many unique values
- **JOIN Columns**: Prefer columns used in frequent JOINs
- **Even Distribution**: Avoid columns with skewed data distribution
- **Examples**: `customer_id`, `order_id`, `tracking_number`

### SORTKEY Selection
- **Query Patterns**: Choose columns in WHERE clauses and ORDER BY
- **Time Series**: Date/timestamp columns are excellent sort keys
- **Hierarchical**: Use compound sortkeys for drill-down queries
- **Examples**: `["created_at", "customer_id"]`, `["date_partition", "region"]`

### Performance Considerations
- **Monitor Query Performance**: Use Redshift query metrics to validate optimizations
- **Update Regularly**: Review and update optimizations based on query patterns
- **Test Changes**: Always test optimization changes with representative workloads

## Monitoring Results

### Check Applied Optimizations
```sql
-- View current table distribution and sort keys
SELECT 
    schemaname,
    tablename,
    distkey,
    sortkey1,
    sortkey2
FROM pg_table_def 
WHERE tablename = 'your_table_name';
```

### Query Performance Analysis
```sql
-- Check query execution plans
EXPLAIN (ANALYZE, BUFFERS) 
SELECT * FROM your_table 
WHERE your_distkey_column = 'value' 
ORDER BY your_sortkey_column;
```

This configuration system provides precise control over Redshift table optimizations while maintaining intelligent defaults for unconfigured tables.