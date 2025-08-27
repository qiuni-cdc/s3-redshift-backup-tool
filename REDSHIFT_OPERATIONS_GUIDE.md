# 📊 Redshift Operations Guide

**Complete guide for managing views, queries, and data operations in Amazon Redshift after backup system deployment.**

---

## 🎯 **Quick Reference**

### **Essential View Commands**
```sql
-- Create basic view
CREATE VIEW schema.view_name AS SELECT * FROM schema.table_name WHERE condition;

-- Create materialized view
CREATE MATERIALIZED VIEW schema.mv_name AS SELECT agg_data FROM schema.table GROUP BY col;

-- Refresh materialized view
REFRESH MATERIALIZED VIEW schema.mv_name;

-- Drop view
DROP VIEW schema.view_name;
```

### **Performance Commands**
```sql
-- Analyze table statistics
ANALYZE schema.table_name;

-- Vacuum table
VACUUM schema.table_name;

-- Check table info
SELECT * FROM pg_table_def WHERE tablename = 'your_table';
```

---

## 📋 **Table of Contents**

1. [View Creation Patterns](#view-creation-patterns)
2. [Post-Backup Data Operations](#post-backup-data-operations)
3. [Performance Optimization](#performance-optimization)
4. [Integration with Backup System](#integration-with-backup-system)
5. [Common Use Cases](#common-use-cases)
6. [Troubleshooting](#troubleshooting)
7. [Best Practices](#best-practices)

---

## 🔧 **View Creation Patterns**

### **1. Basic Views for Data Filtering**

#### **Simple Data Filtering**
```sql
-- Recent settlement transactions (last 30 days)
CREATE VIEW settlement.recent_transactions AS
SELECT 
    id,
    transaction_date,
    amount,
    status,
    updated_at
FROM settlement.settlement_normal_delivery_detail 
WHERE updated_at >= CURRENT_DATE - INTERVAL '30 days'
  AND status = 'completed';
```

#### **Data Quality Views**
```sql
-- Identify data quality issues
CREATE VIEW settlement.data_quality_check AS
SELECT 
    'missing_amounts' as issue_type,
    COUNT(*) as record_count,
    MIN(updated_at) as earliest_date,
    MAX(updated_at) as latest_date
FROM settlement.settlement_normal_delivery_detail 
WHERE amount IS NULL OR amount = 0

UNION ALL

SELECT 
    'invalid_dates' as issue_type,
    COUNT(*) as record_count,
    MIN(updated_at) as earliest_date,
    MAX(updated_at) as latest_date
FROM settlement.settlement_normal_delivery_detail 
WHERE updated_at > CURRENT_DATE;
```

#### **Business Logic Views**
```sql
-- Calculate daily settlement summaries
CREATE VIEW settlement.daily_summary AS
SELECT 
    DATE(updated_at) as settlement_date,
    COUNT(*) as transaction_count,
    SUM(amount) as total_amount,
    AVG(amount) as avg_amount,
    COUNT(DISTINCT customer_id) as unique_customers
FROM settlement.settlement_normal_delivery_detail 
WHERE status = 'completed'
GROUP BY DATE(updated_at)
ORDER BY settlement_date DESC;
```

### **2. Complex Analytical Views**

#### **Multi-Table Joins**
```sql
-- Customer settlement analysis
CREATE VIEW settlement.customer_analysis AS
SELECT 
    c.customer_id,
    c.customer_name,
    COUNT(s.id) as total_transactions,
    SUM(s.amount) as total_settled,
    AVG(s.amount) as avg_transaction,
    MIN(s.updated_at) as first_settlement,
    MAX(s.updated_at) as last_settlement,
    DATEDIFF(day, MIN(s.updated_at), MAX(s.updated_at)) as customer_lifetime_days
FROM settlement.settlement_normal_delivery_detail s
LEFT JOIN customer.customer_details c ON s.customer_id = c.customer_id
WHERE s.status = 'completed'
GROUP BY c.customer_id, c.customer_name
HAVING COUNT(s.id) > 5  -- Active customers only
ORDER BY total_settled DESC;
```

#### **Time-Series Analysis**
```sql
-- Monthly settlement trends
CREATE VIEW settlement.monthly_trends AS
SELECT 
    DATE_TRUNC('month', updated_at) as month,
    COUNT(*) as transactions,
    SUM(amount) as total_amount,
    LAG(SUM(amount)) OVER (ORDER BY DATE_TRUNC('month', updated_at)) as prev_month_amount,
    ROUND(
        (SUM(amount) - LAG(SUM(amount)) OVER (ORDER BY DATE_TRUNC('month', updated_at))) 
        / LAG(SUM(amount)) OVER (ORDER BY DATE_TRUNC('month', updated_at)) * 100, 2
    ) as growth_rate_percent
FROM settlement.settlement_normal_delivery_detail 
WHERE status = 'completed'
  AND updated_at >= '2024-01-01'
GROUP BY DATE_TRUNC('month', updated_at)
ORDER BY month;
```

### **3. Materialized Views for Performance**

#### **Large Dataset Aggregations**
```sql
-- Pre-calculate expensive aggregations
CREATE MATERIALIZED VIEW settlement.daily_stats_mv AS
SELECT 
    DATE(updated_at) as date,
    status,
    payment_method,
    COUNT(*) as transaction_count,
    SUM(amount) as total_amount,
    AVG(amount) as avg_amount,
    STDDEV(amount) as amount_stddev,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY amount) as median_amount
FROM settlement.settlement_normal_delivery_detail 
GROUP BY DATE(updated_at), status, payment_method;

-- Refresh strategy
-- Set up auto-refresh or manual refresh based on data update frequency
REFRESH MATERIALIZED VIEW settlement.daily_stats_mv;
```

#### **Complex Calculations**
```sql
-- Customer lifetime value calculation (expensive)
CREATE MATERIALIZED VIEW settlement.customer_ltv_mv AS
SELECT 
    customer_id,
    COUNT(*) as total_orders,
    SUM(amount) as lifetime_value,
    AVG(amount) as avg_order_value,
    DATEDIFF(day, MIN(updated_at), MAX(updated_at)) as customer_lifespan_days,
    SUM(amount) / NULLIF(DATEDIFF(day, MIN(updated_at), MAX(updated_at)), 0) as daily_value,
    CASE 
        WHEN SUM(amount) > 10000 THEN 'High Value'
        WHEN SUM(amount) > 5000 THEN 'Medium Value'
        ELSE 'Low Value'
    END as customer_segment
FROM settlement.settlement_normal_delivery_detail 
WHERE status = 'completed'
GROUP BY customer_id
HAVING COUNT(*) >= 3;  -- Only customers with multiple transactions
```

---

## 🔄 **Post-Backup Data Operations**

### **1. Data Validation After Backup**

#### **Row Count Verification**
```sql
-- Compare source vs destination row counts
CREATE VIEW settlement.backup_validation AS
SELECT 
    'settlement_normal_delivery_detail' as table_name,
    COUNT(*) as redshift_rows,
    -- Add source MySQL count through external verification
    CURRENT_TIMESTAMP as validation_time;
```

#### **Data Freshness Check**
```sql
-- Monitor data freshness
CREATE VIEW settlement.data_freshness AS
SELECT 
    table_name,
    MAX(updated_at) as latest_record,
    DATEDIFF(hour, MAX(updated_at), CURRENT_TIMESTAMP) as hours_behind,
    CASE 
        WHEN DATEDIFF(hour, MAX(updated_at), CURRENT_TIMESTAMP) <= 24 THEN 'Fresh'
        WHEN DATEDIFF(hour, MAX(updated_at), CURRENT_TIMESTAMP) <= 72 THEN 'Acceptable'
        ELSE 'Stale'
    END as freshness_status
FROM (
    SELECT 'settlement_normal_delivery_detail' as table_name, updated_at 
    FROM settlement.settlement_normal_delivery_detail
) t
GROUP BY table_name;
```

### **2. Schema Validation**

#### **Column Consistency Check**
```sql
-- Verify table schema matches expectations
CREATE VIEW settlement.schema_validation AS
SELECT 
    table_name,
    column_name,
    data_type,
    is_nullable,
    column_default
FROM information_schema.columns 
WHERE table_schema = 'settlement'
  AND table_name LIKE '%settlement%'
ORDER BY table_name, ordinal_position;
```

### **3. Data Quality Monitoring**

#### **Automated Quality Checks**
```sql
-- Comprehensive data quality dashboard
CREATE VIEW settlement.quality_dashboard AS
SELECT 
    'Null Check' as check_type,
    'amount' as column_name,
    COUNT(*) as fail_count,
    COUNT(*) * 100.0 / (SELECT COUNT(*) FROM settlement.settlement_normal_delivery_detail) as fail_percentage
FROM settlement.settlement_normal_delivery_detail 
WHERE amount IS NULL

UNION ALL

SELECT 
    'Range Check' as check_type,
    'amount' as column_name,
    COUNT(*) as fail_count,
    COUNT(*) * 100.0 / (SELECT COUNT(*) FROM settlement.settlement_normal_delivery_detail) as fail_percentage
FROM settlement.settlement_normal_delivery_detail 
WHERE amount < 0 OR amount > 1000000

UNION ALL

SELECT 
    'Date Check' as check_type,
    'updated_at' as column_name,
    COUNT(*) as fail_count,
    COUNT(*) * 100.0 / (SELECT COUNT(*) FROM settlement.settlement_normal_delivery_detail) as fail_percentage
FROM settlement.settlement_normal_delivery_detail 
WHERE updated_at > CURRENT_TIMESTAMP OR updated_at < '2020-01-01';
```

---

## ⚡ **Performance Optimization**

### **1. When to Use Materialized Views**

#### **Decision Matrix**
```sql
-- Use materialized views when:
-- 1. Query complexity is high (joins, aggregations, window functions)
-- 2. Data volume is large (millions of rows)
-- 3. Query frequency is high (multiple times per day)
-- 4. Underlying data changes infrequently

-- Example: Complex aggregation that takes >30 seconds
CREATE MATERIALIZED VIEW settlement.complex_metrics_mv AS
SELECT 
    customer_id,
    payment_method,
    DATE_TRUNC('week', updated_at) as week,
    COUNT(*) as weekly_transactions,
    SUM(amount) as weekly_total,
    SUM(SUM(amount)) OVER (
        PARTITION BY customer_id, payment_method 
        ORDER BY DATE_TRUNC('week', updated_at) 
        ROWS UNBOUNDED PRECEDING
    ) as running_total,
    ROW_NUMBER() OVER (
        PARTITION BY customer_id 
        ORDER BY SUM(amount) DESC
    ) as payment_method_rank
FROM settlement.settlement_normal_delivery_detail 
WHERE status = 'completed'
GROUP BY customer_id, payment_method, DATE_TRUNC('week', updated_at);
```

### **2. Distribution and Sort Key Optimization**

#### **Analyze Table Distribution**
```sql
-- Check table distribution
SELECT 
    slice,
    COUNT(*) as row_count
FROM settlement.settlement_normal_delivery_detail 
GROUP BY slice
ORDER BY slice;

-- Check sort key effectiveness
SELECT 
    table_name,
    sortkey1,
    min_value,
    max_value,
    compression_ratio
FROM pg_table_def 
WHERE tablename = 'settlement_normal_delivery_detail';
```

#### **Optimize Distribution Strategy**
```sql
-- For large tables with frequent joins
-- Consider distribution key on join columns
CREATE TABLE settlement.settlement_optimized 
DISTKEY(customer_id)  -- Distribute by frequently joined column
SORTKEY(updated_at)   -- Sort by time series data
AS 
SELECT * FROM settlement.settlement_normal_delivery_detail;

-- For lookup tables
-- Use ALL distribution for small reference tables
CREATE TABLE settlement.payment_methods 
DISTSTYLE ALL
AS 
SELECT DISTINCT payment_method FROM settlement.settlement_normal_delivery_detail;
```

### **3. Query Optimization Techniques**

#### **Window Function Optimization**
```sql
-- Efficient window function usage
CREATE VIEW settlement.customer_metrics_optimized AS
SELECT 
    customer_id,
    updated_at,
    amount,
    -- Multiple window functions with same partition for efficiency
    SUM(amount) OVER (PARTITION BY customer_id ORDER BY updated_at) as running_total,
    COUNT(*) OVER (PARTITION BY customer_id ORDER BY updated_at) as transaction_sequence,
    LAG(amount) OVER (PARTITION BY customer_id ORDER BY updated_at) as prev_amount,
    -- Avoid separate window function calls
    AVG(amount) OVER (PARTITION BY customer_id ORDER BY updated_at ROWS 2 PRECEDING) as moving_avg_3
FROM settlement.settlement_normal_delivery_detail 
WHERE status = 'completed';
```

#### **Join Optimization**
```sql
-- Use INNER JOIN when possible, avoid CROSS JOIN
-- Place smaller tables on the right side of joins
CREATE VIEW settlement.optimized_customer_summary AS
SELECT 
    s.customer_id,
    c.customer_name,  -- smaller customer table
    COUNT(*) as transaction_count,
    SUM(s.amount) as total_amount
FROM settlement.settlement_normal_delivery_detail s  -- larger table on left
INNER JOIN customer.customer_details c ON s.customer_id = c.customer_id
WHERE s.status = 'completed'
  AND s.updated_at >= '2024-01-01'  -- Filter early
GROUP BY s.customer_id, c.customer_name;
```

---

## 🔗 **Integration with Backup System**

### **1. Automated View Refresh After Data Loads**

#### **Post-Load View Refresh Script**
```sql
-- Create procedure to refresh views after backup loads
CREATE OR REPLACE PROCEDURE settlement.refresh_all_views()
AS $$
BEGIN
    -- Refresh materialized views in dependency order
    REFRESH MATERIALIZED VIEW settlement.daily_stats_mv;
    REFRESH MATERIALIZED VIEW settlement.customer_ltv_mv;
    REFRESH MATERIALIZED VIEW settlement.complex_metrics_mv;
    
    -- Update table statistics
    ANALYZE settlement.settlement_normal_delivery_detail;
    
    -- Log refresh completion
    INSERT INTO settlement.view_refresh_log (refresh_time, status)
    VALUES (CURRENT_TIMESTAMP, 'SUCCESS');
    
EXCEPTION WHEN OTHERS THEN
    INSERT INTO settlement.view_refresh_log (refresh_time, status, error_message)
    VALUES (CURRENT_TIMESTAMP, 'FAILED', SQLERRM);
    RAISE;
END;
$$ LANGUAGE plpgsql;
```

#### **Integration with Backup Pipeline**
```bash
# Add to your backup automation after successful S3 → Redshift load
# In your sync pipeline or flexible progressive backup script

# After successful Redshift loading:
psql -h your-redshift-cluster -d your-database -c "CALL settlement.refresh_all_views();"
```

### **2. Schema Validation Workflows**

#### **Automated Schema Drift Detection**
```sql
-- Create view to monitor schema changes
CREATE VIEW settlement.schema_drift AS
SELECT 
    table_name,
    column_name,
    data_type,
    is_nullable,
    CURRENT_TIMESTAMP as check_time,
    -- Compare against expected schema
    CASE 
        WHEN data_type != expected_type THEN 'Type Mismatch'
        WHEN is_nullable != expected_nullable THEN 'Nullable Mismatch'
        ELSE 'OK'
    END as validation_status
FROM (
    SELECT 
        table_name,
        column_name,
        data_type,
        is_nullable,
        -- Define expected schema here
        CASE column_name 
            WHEN 'id' THEN 'bigint'
            WHEN 'amount' THEN 'numeric'
            WHEN 'updated_at' THEN 'timestamp without time zone'
            ELSE data_type
        END as expected_type,
        CASE column_name 
            WHEN 'id' THEN 'NO'
            WHEN 'amount' THEN 'NO'
            ELSE is_nullable
        END as expected_nullable
    FROM information_schema.columns 
    WHERE table_schema = 'settlement'
      AND table_name = 'settlement_normal_delivery_detail'
) schema_check;
```

### **3. Data Lineage Tracking**

#### **Track Data Movement Through Views**
```sql
-- Create lineage tracking for audit purposes
CREATE VIEW settlement.data_lineage AS
SELECT 
    'settlement.settlement_normal_delivery_detail' as source_table,
    'settlement.daily_summary' as target_view,
    'aggregation' as transformation_type,
    'Daily settlement summaries' as description,
    CURRENT_TIMESTAMP as created_at

UNION ALL

SELECT 
    'settlement.settlement_normal_delivery_detail' as source_table,
    'settlement.customer_analysis' as target_view,
    'join_aggregation' as transformation_type,
    'Customer-level analysis with joins' as description,
    CURRENT_TIMESTAMP as created_at;
```

---

## 💡 **Common Use Cases**

### **1. Reporting and Analytics**

#### **Executive Dashboard Views**
```sql
-- Key performance indicators
CREATE VIEW settlement.executive_kpis AS
SELECT 
    'Today' as period,
    COUNT(*) as transactions,
    SUM(amount) as revenue,
    COUNT(DISTINCT customer_id) as active_customers,
    AVG(amount) as avg_transaction_value
FROM settlement.settlement_normal_delivery_detail 
WHERE DATE(updated_at) = CURRENT_DATE
  AND status = 'completed'

UNION ALL

SELECT 
    'This Month' as period,
    COUNT(*) as transactions,
    SUM(amount) as revenue,
    COUNT(DISTINCT customer_id) as active_customers,
    AVG(amount) as avg_transaction_value
FROM settlement.settlement_normal_delivery_detail 
WHERE DATE_TRUNC('month', updated_at) = DATE_TRUNC('month', CURRENT_DATE)
  AND status = 'completed'

UNION ALL

SELECT 
    'Last 30 Days' as period,
    COUNT(*) as transactions,
    SUM(amount) as revenue,
    COUNT(DISTINCT customer_id) as active_customers,
    AVG(amount) as avg_transaction_value
FROM settlement.settlement_normal_delivery_detail 
WHERE updated_at >= CURRENT_DATE - INTERVAL '30 days'
  AND status = 'completed';
```

#### **Cohort Analysis**
```sql
-- Customer cohort analysis
CREATE VIEW settlement.cohort_analysis AS
SELECT 
    cohort_month,
    period_number,
    users_in_cohort,
    total_users,
    ROUND(users_in_cohort * 100.0 / total_users, 2) as retention_rate
FROM (
    SELECT 
        cohort_month,
        period_number,
        COUNT(DISTINCT customer_id) as users_in_cohort,
        FIRST_VALUE(COUNT(DISTINCT customer_id)) OVER (
            PARTITION BY cohort_month 
            ORDER BY period_number 
            ROWS UNBOUNDED PRECEDING
        ) as total_users
    FROM (
        SELECT 
            customer_id,
            DATE_TRUNC('month', first_transaction) as cohort_month,
            DATEDIFF(month, 
                DATE_TRUNC('month', first_transaction),
                DATE_TRUNC('month', updated_at)
            ) as period_number
        FROM (
            SELECT 
                customer_id,
                updated_at,
                MIN(updated_at) OVER (PARTITION BY customer_id) as first_transaction
            FROM settlement.settlement_normal_delivery_detail 
            WHERE status = 'completed'
        ) customer_periods
    ) cohort_data
    GROUP BY cohort_month, period_number
) cohort_table
ORDER BY cohort_month, period_number;
```

### **2. Data Monitoring and Alerts**

#### **Anomaly Detection Views**
```sql
-- Detect transaction volume anomalies
CREATE VIEW settlement.volume_anomalies AS
SELECT 
    transaction_date,
    daily_count,
    avg_count,
    stddev_count,
    CASE 
        WHEN daily_count > avg_count + (2 * stddev_count) THEN 'High Anomaly'
        WHEN daily_count < avg_count - (2 * stddev_count) THEN 'Low Anomaly'
        ELSE 'Normal'
    END as anomaly_status
FROM (
    SELECT 
        DATE(updated_at) as transaction_date,
        COUNT(*) as daily_count,
        AVG(COUNT(*)) OVER (
            ORDER BY DATE(updated_at) 
            ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING
        ) as avg_count,
        STDDEV(COUNT(*)) OVER (
            ORDER BY DATE(updated_at) 
            ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING
        ) as stddev_count
    FROM settlement.settlement_normal_delivery_detail 
    WHERE status = 'completed'
      AND updated_at >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY DATE(updated_at)
) daily_stats
WHERE avg_count IS NOT NULL
ORDER BY transaction_date DESC;
```

#### **Data Quality Alerts**
```sql
-- Monitor data quality issues
CREATE VIEW settlement.quality_alerts AS
SELECT 
    alert_type,
    severity,
    record_count,
    threshold,
    CASE WHEN record_count > threshold THEN 'ALERT' ELSE 'OK' END as status,
    CURRENT_TIMESTAMP as check_time
FROM (
    SELECT 'Missing Amounts' as alert_type, 'HIGH' as severity, 
           COUNT(*) as record_count, 100 as threshold
    FROM settlement.settlement_normal_delivery_detail 
    WHERE amount IS NULL AND updated_at >= CURRENT_DATE - INTERVAL '1 day'
    
    UNION ALL
    
    SELECT 'Negative Amounts' as alert_type, 'MEDIUM' as severity,
           COUNT(*) as record_count, 10 as threshold
    FROM settlement.settlement_normal_delivery_detail 
    WHERE amount < 0 AND updated_at >= CURRENT_DATE - INTERVAL '1 day'
    
    UNION ALL
    
    SELECT 'Future Dates' as alert_type, 'HIGH' as severity,
           COUNT(*) as record_count, 0 as threshold
    FROM settlement.settlement_normal_delivery_detail 
    WHERE updated_at > CURRENT_TIMESTAMP
) quality_checks;
```

### **3. Business Intelligence Views**

#### **Customer Segmentation**
```sql
-- RFM Analysis (Recency, Frequency, Monetary)
CREATE VIEW settlement.customer_rfm AS
SELECT 
    customer_id,
    recency_score,
    frequency_score,
    monetary_score,
    CASE 
        WHEN recency_score >= 4 AND frequency_score >= 4 AND monetary_score >= 4 THEN 'Champions'
        WHEN recency_score >= 3 AND frequency_score >= 3 AND monetary_score >= 3 THEN 'Loyal Customers'
        WHEN recency_score >= 3 AND frequency_score <= 2 THEN 'Potential Loyalists'
        WHEN recency_score <= 2 AND frequency_score >= 3 THEN 'At Risk'
        WHEN recency_score <= 2 AND frequency_score <= 2 AND monetary_score >= 3 THEN 'Cannot Lose Them'
        ELSE 'Others'
    END as customer_segment
FROM (
    SELECT 
        customer_id,
        NTILE(5) OVER (ORDER BY days_since_last_transaction) as recency_score,
        NTILE(5) OVER (ORDER BY transaction_frequency DESC) as frequency_score,
        NTILE(5) OVER (ORDER BY total_spent DESC) as monetary_score
    FROM (
        SELECT 
            customer_id,
            DATEDIFF(day, MAX(updated_at), CURRENT_DATE) as days_since_last_transaction,
            COUNT(*) as transaction_frequency,
            SUM(amount) as total_spent
        FROM settlement.settlement_normal_delivery_detail 
        WHERE status = 'completed'
        GROUP BY customer_id
        HAVING COUNT(*) >= 2  -- Only customers with multiple transactions
    ) customer_metrics
) rfm_scores;
```

---

## 🔧 **Troubleshooting**

### **1. Common View Creation Issues**

#### **Permission Errors**
```sql
-- Check user permissions
SELECT 
    table_schema,
    table_name,
    privilege_type
FROM information_schema.table_privileges 
WHERE grantee = CURRENT_USER
  AND table_schema = 'settlement';

-- Grant necessary permissions
GRANT SELECT ON settlement.settlement_normal_delivery_detail TO your_user;
GRANT CREATE ON SCHEMA settlement TO your_user;
```

#### **Memory and Performance Issues**
```sql
-- Check query performance
EXPLAIN SELECT * FROM settlement.complex_view;

-- Monitor long-running queries
SELECT 
    query,
    starttime,
    duration/1000000 as duration_seconds,
    query_id
FROM stl_query 
WHERE duration > 60000000  -- More than 60 seconds
ORDER BY starttime DESC
LIMIT 10;
```

### **2. Materialized View Issues**

#### **Refresh Failures**
```sql
-- Check materialized view status
SELECT 
    schema_name,
    table_name,
    status
FROM pg_tables 
WHERE table_type = 'MATERIALIZED VIEW'
  AND schema_name = 'settlement';

-- Manual refresh with error handling
BEGIN;
REFRESH MATERIALIZED VIEW settlement.daily_stats_mv;
COMMIT;
-- If this fails, check the underlying data and queries
```

#### **Storage and Performance**
```sql
-- Check materialized view size
SELECT 
    table_name,
    size_in_mb,
    row_count
FROM (
    SELECT 
        tablename as table_name,
        (SUM(used_space) / 1024 / 1024) as size_in_mb,
        SUM(row_count) as row_count
    FROM pg_table_def 
    WHERE tablename LIKE '%_mv'
    GROUP BY tablename
) mv_stats
ORDER BY size_in_mb DESC;
```

### **3. Performance Debugging**

#### **Slow View Queries**
```sql
-- Analyze slow queries on views
SELECT 
    query_id,
    query,
    total_time/1000 as total_seconds,
    rows_returned,
    rows_returned/NULLIF(total_time/1000, 0) as rows_per_second
FROM (
    SELECT 
        query_id,
        LISTAGG(text) as query,
        SUM(duration)/1000000 as total_time,
        MAX(returned_rows) as rows_returned
    FROM stl_query q
    JOIN stl_querytext qt ON q.query_id = qt.query_id
    WHERE q.starttime >= CURRENT_DATE - INTERVAL '1 day'
      AND qt.text ILIKE '%settlement%'
    GROUP BY q.query_id
) query_stats
WHERE total_time > 30  -- Queries taking more than 30 seconds
ORDER BY total_time DESC;
```

---

## 📋 **Best Practices**

### **1. View Naming Conventions**

```sql
-- Naming convention examples:
-- Views: schema.descriptive_name_v
-- Materialized Views: schema.descriptive_name_mv
-- Aggregation Views: schema.table_agg_v
-- Daily Summary Views: schema.table_daily_v

-- Good examples:
CREATE VIEW settlement.transactions_recent_v AS ...
CREATE MATERIALIZED VIEW settlement.customer_metrics_mv AS ...
CREATE VIEW settlement.daily_settlement_summary_v AS ...
```

### **2. Documentation and Comments**

```sql
-- Always include comments in complex views
CREATE VIEW settlement.customer_analysis_v AS
-- Purpose: Comprehensive customer analysis including transaction patterns
-- Dependencies: settlement.settlement_normal_delivery_detail, customer.customer_details
-- Update Frequency: Real-time (regular view)
-- Owner: Analytics Team
-- Created: 2025-08-19
SELECT 
    c.customer_id,
    c.customer_name,
    -- Transaction metrics
    COUNT(s.id) as total_transactions,
    SUM(s.amount) as lifetime_value,
    -- Time-based metrics  
    MIN(s.updated_at) as first_transaction,
    MAX(s.updated_at) as last_transaction
FROM settlement.settlement_normal_delivery_detail s
JOIN customer.customer_details c ON s.customer_id = c.customer_id
WHERE s.status = 'completed'
GROUP BY c.customer_id, c.customer_name;
```

### **3. Change Management**

```sql
-- Version control for view changes
-- 1. Always backup current view definition
CREATE VIEW settlement.customer_analysis_v_backup AS 
SELECT * FROM settlement.customer_analysis_v;

-- 2. Test new view with different name first
CREATE VIEW settlement.customer_analysis_v_new AS 
-- new definition here

-- 3. Compare results
SELECT COUNT(*) FROM settlement.customer_analysis_v;
SELECT COUNT(*) FROM settlement.customer_analysis_v_new;

-- 4. Replace when confident
DROP VIEW settlement.customer_analysis_v;
ALTER VIEW settlement.customer_analysis_v_new RENAME TO customer_analysis_v;
```

### **4. Monitoring and Maintenance**

#### **Regular Maintenance Schedule**
```sql
-- Daily tasks
REFRESH MATERIALIZED VIEW settlement.daily_stats_mv;
ANALYZE settlement.settlement_normal_delivery_detail;

-- Weekly tasks  
VACUUM settlement.settlement_normal_delivery_detail;
REFRESH MATERIALIZED VIEW settlement.customer_ltv_mv;

-- Monthly tasks
-- Review and optimize slow-performing views
-- Update table statistics
-- Clean up unused views
```

#### **Performance Monitoring**
```sql
-- Create monitoring view for view performance
CREATE VIEW settlement.view_performance_monitor AS
SELECT 
    schemaname,
    tablename,
    n_tup_ins as inserts,
    n_tup_upd as updates,
    n_tup_del as deletes,
    last_vacuum,
    last_analyze
FROM pg_stat_user_tables 
WHERE schemaname = 'settlement'
ORDER BY n_tup_ins + n_tup_upd + n_tup_del DESC;
```

---

## 🎯 **Integration Examples**

### **Complete Workflow Example**

```bash
#!/bin/bash
# complete_redshift_workflow.sh
# Complete workflow from backup to analytics

echo "🔄 Starting complete Redshift workflow..."

# 1. Run backup (your existing system)
echo "📥 Running backup..."
python -m src.cli.main sync -t settlement.settlement_normal_delivery_detail

# 2. Verify backup completion
echo "✅ Verifying backup..."
python -m src.cli.main watermark get -t settlement.settlement_normal_delivery_detail

# 3. Refresh materialized views
echo "🔄 Refreshing materialized views..."
psql -h $REDSHIFT_HOST -d $REDSHIFT_DB -c "
    REFRESH MATERIALIZED VIEW settlement.daily_stats_mv;
    REFRESH MATERIALIZED VIEW settlement.customer_ltv_mv;
    ANALYZE settlement.settlement_normal_delivery_detail;
"

# 4. Run data quality checks
echo "🔍 Running data quality checks..."
psql -h $REDSHIFT_HOST -d $REDSHIFT_DB -c "
    SELECT * FROM settlement.quality_alerts WHERE status = 'ALERT';
"

# 5. Generate business reports
echo "📊 Generating business reports..."
psql -h $REDSHIFT_HOST -d $REDSHIFT_DB -c "
    SELECT * FROM settlement.executive_kpis;
    SELECT * FROM settlement.volume_anomalies WHERE anomaly_status != 'Normal';
"

echo "✅ Workflow completed successfully!"
```

---

## 📚 **Additional Resources**

### **Related Documentation**
- **[USER_MANUAL.md](./USER_MANUAL.md)** - Complete backup system usage
- **[WATERMARK_CLI_GUIDE.md](./WATERMARK_CLI_GUIDE.md)** - Watermark management
- **[LARGE_TABLE_GUIDELINES.md](./LARGE_TABLE_GUIDELINES.md)** - Handling large datasets

### **External References**
- [Amazon Redshift SQL Reference](https://docs.aws.amazon.com/redshift/latest/dg/c_SQL_commands.html)
- [Redshift Best Practices](https://docs.aws.amazon.com/redshift/latest/dg/best-practices.html)
- [Redshift Performance Tuning](https://docs.aws.amazon.com/redshift/latest/dg/c-optimizing-query-performance.html)

### **Support and Troubleshooting**
- For backup system issues: Check **[TROUBLESHOOTING.md](./TROUBLESHOOTING.md)**
- For Redshift connectivity: Verify SSH tunnel configuration in `.env`
- For performance issues: Use the monitoring queries provided in this guide

---

*This guide complements your S3-Redshift backup system by providing comprehensive Redshift operations and view management capabilities. Use it to build robust analytics and reporting layers on top of your backed-up data.*