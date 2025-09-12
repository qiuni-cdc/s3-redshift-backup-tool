# MySQL to Redshift: Key Differences and Best Practices

## 🎯 For Teams Familiar with MySQL Moving to Redshift

This guide helps MySQL experts understand Redshift's different optimization approach and avoid common mistakes.

## 📊 Fundamental Differences

| Aspect | MySQL | Redshift |
|--------|-------|----------|
| **Storage** | Row-based (OLTP) | Column-based (OLAP) |
| **Primary Use** | Transactions | Analytics |
| **Query Pattern** | Point lookups, updates | Large aggregations, scans |
| **Optimization** | Indexes on columns | Distribution + Sort keys |
| **Scaling** | Vertical (bigger server) | Horizontal (more nodes) |
| **Best For** | CRUD operations | Reporting, BI, data warehouse |

## 🏗️ Architecture Differences

### MySQL (Row-Based Storage)
```
Row 1: [id=1, name="John", age=25, city="NYC", salary=50000]
Row 2: [id=2, name="Jane", age=30, city="SF", salary=60000]
Row 3: [id=3, name="Bob", age=35, city="LA", salary=70000]

Index on 'age': Points to specific rows
```

### Redshift (Column-Based Storage)
```
Column 'id':     [1, 2, 3, ...]
Column 'name':   ["John", "Jane", "Bob", ...]  
Column 'age':    [25, 30, 35, ...]
Column 'city':   ["NYC", "SF", "LA", ...]
Column 'salary': [50000, 60000, 70000, ...]

Sort Key on 'age': Pre-sorts entire column
```

**Why This Matters:**
- **MySQL**: Fast single-row lookups (OLTP)
- **Redshift**: Fast column aggregations (OLAP)

## 🔍 Query Optimization: MySQL vs Redshift

### MySQL Index Strategy
```sql
-- MySQL: Create indexes for fast lookups
CREATE INDEX idx_partner_date ON orders (partner_id, order_date);
CREATE INDEX idx_customer ON orders (customer_id);
CREATE INDEX idx_status ON orders (status);

-- Query uses index
SELECT * FROM orders 
WHERE partner_id = 85 
AND order_date >= '2025-01-01';
-- Uses idx_partner_date for fast lookup
```

### Redshift Equivalent Strategy
```sql
-- Redshift: Design table structure for analytics
CREATE TABLE orders (
    order_id BIGINT,
    partner_id INTEGER,
    customer_id BIGINT DISTKEY,     -- Most frequent JOIN column
    order_date DATE,
    status VARCHAR(50)
)
COMPOUND SORTKEY(partner_id, order_date, status);  -- Most filtered columns

-- Same query benefits from table design
SELECT * FROM orders 
WHERE partner_id = 85 
AND order_date >= '2025-01-01';
-- Uses sort key zone maps to skip data blocks
```

## 🎯 Key Concept: No Traditional Indexes in Redshift

### What MySQL Developers Expect (❌ Not Available in Redshift)
```sql
-- ❌ These don't exist in Redshift
CREATE INDEX idx_partner ON orders (partner_id);
CREATE UNIQUE INDEX idx_order_id ON orders (order_id);
CREATE INDEX idx_date_range ON orders (order_date);

-- ❌ Index hints don't work
SELECT /*+ USE INDEX (orders, idx_partner) */ * FROM orders;
```

### What Redshift Uses Instead (✅ Table Design)
```sql
-- ✅ Define at table creation time
CREATE TABLE orders (
    columns...
)
DISTKEY(customer_id)                           -- Distribution strategy
COMPOUND SORTKEY(partner_id, order_date);      -- Query optimization
```

## 🔧 Redshift Optimization Strategies

### 1. Distribution Keys (DISTKEY) - Join Optimization

**Purpose**: Controls which node stores each row

#### MySQL Mindset:
```sql
-- MySQL: Join any tables anytime
SELECT o.*, c.name 
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id;
-- Database handles join strategy automatically
```

#### Redshift Best Practice:
```json
{
  "orders_table": {
    "distkey": "customer_id"
  },
  "customers_table": {
    "distkey": "customer_id"  
  }
}
```

**Result**: Data with same `customer_id` stored on same node = **50-90% faster JOINs**

### 2. Sort Keys (SORTKEY) - Filter Optimization

**Purpose**: Pre-sorts data for fast WHERE clause filtering

#### MySQL Approach:
```sql
-- MySQL: Index commonly filtered columns
CREATE INDEX idx_date_partner ON orders (order_date, partner_id);

SELECT * FROM orders 
WHERE order_date >= '2025-01-01' 
AND partner_id = 85;
-- Index scan finds matching rows quickly
```

#### Redshift Equivalent:
```json
{
  "orders_table": {
    "sortkey": ["order_date", "partner_id"]
  }
}
```

**How Sort Keys Work:**
- Data pre-sorted by `order_date`, then `partner_id`
- **Zone Maps**: Statistics track min/max values per data block
- **Block Skipping**: Query skips entire blocks outside date range
- **Result**: 70-95% faster WHERE clauses

### 3. Compression (ENCODE) - Storage Optimization

#### MySQL:
```sql
-- MySQL: Row-level compression (limited options)
CREATE TABLE orders (...) ENGINE=InnoDB ROW_FORMAT=COMPRESSED;
```

#### Redshift:
```sql
-- Redshift: Column-level compression (automatic)
CREATE TABLE orders (
    order_id BIGINT ENCODE delta,           -- Good for sequential IDs
    partner_id INTEGER ENCODE bytedict,     -- Good for low cardinality
    order_date DATE ENCODE delta32k,        -- Good for dates
    description TEXT ENCODE lzo             -- Good for text
);
-- Modern Redshift does this automatically
```

## 📋 Converting Your MySQL Optimization Knowledge

### Common MySQL Patterns → Redshift Equivalents

#### Pattern 1: Primary Key Lookups
```sql
-- MySQL
CREATE TABLE products (
    id INT PRIMARY KEY,        -- Clustered index
    name VARCHAR(255),
    INDEX idx_name (name)      -- Secondary index
);

-- Redshift equivalent
CREATE TABLE products (
    id INTEGER,
    name VARCHAR(510)          -- Note: doubled for safety
)
SORTKEY(id);                  -- Fast ID-based queries
```

#### Pattern 2: Range Queries on Dates
```sql
-- MySQL
CREATE INDEX idx_created ON orders (created_at);
SELECT * FROM orders WHERE created_at >= '2025-01-01';

-- Redshift equivalent
CREATE TABLE orders (...)
SORTKEY(created_at);          -- Pre-sorted by date
-- Same query scans only relevant data blocks
```

#### Pattern 3: Multi-Column Indexes
```sql
-- MySQL
CREATE INDEX idx_composite ON orders (status, created_at, customer_id);

-- Redshift equivalent (order matters!)
CREATE TABLE orders (...)
COMPOUND SORTKEY(status, created_at, customer_id);
-- Query pattern: WHERE status=? AND created_at>=? ORDER BY customer_id
```

#### Pattern 4: Unique Constraints
```sql
-- MySQL
CREATE UNIQUE INDEX idx_unique ON orders (order_number);

-- Redshift: NO UNIQUE CONSTRAINTS
-- Handle at application level or use:
SELECT order_number, COUNT(*) 
FROM orders 
GROUP BY order_number 
HAVING COUNT(*) > 1;  -- Find duplicates manually
```

## 🚀 Practical Example: Your `dw_parcel_detail_tool`

### Current MySQL Table (Your Reality)
```sql
-- MySQL (optimized for transactional queries)
CREATE TABLE dw_parcel_detail_tool (
    order_id BIGINT,
    partner_id INTEGER,
    `190_time` VARCHAR(255),
    Broker_Handover_Time TIMESTAMP,
    -- ... many more columns
    INDEX idx_partner (partner_id),
    INDEX idx_handover (Broker_Handover_Time),  
    INDEX idx_order (order_id)
);

-- Fast single-row lookups
SELECT * FROM dw_parcel_detail_tool WHERE order_id = 12345;
```

### Optimized Redshift Table (Your Target)
```sql
-- Redshift (optimized for analytical queries)
CREATE TABLE dw_parcel_detail_tool (
    order_id BIGINT,
    partner_id INTEGER,
    col_190_time VARCHAR(510),              -- Auto-sanitized and doubled
    col_broker_handover_time TIMESTAMP,     -- Sanitized name
    -- ... all other columns
)
DISTKEY(order_id)                           -- JOIN optimization
COMPOUND SORTKEY(partner_id, col_broker_handover_time, order_id);  -- Filter optimization
```

### Query Performance Comparison

#### Your Frequent Query:
```sql
SELECT order_id, tno, reference, partner_id, col_broker_handover_time
FROM dw_parcel_detail_tool t1
LEFT JOIN dw_ecs_order_invoice t2 ON t1.order_id = t2.order_id
WHERE partner_id = 85 
AND col_broker_handover_time BETWEEN '2025-08-04' AND '2025-08-18'
AND order_id > 0
ORDER BY order_id;
```

**MySQL Performance (Row-based)**:
- Uses `idx_partner` index to find partner_id=85
- Then filters by date range
- JOIN uses `order_id` index
- **Typical Time**: 10-30 seconds for millions of rows

**Redshift Performance (Column-based)**:
- Sort key zone maps skip ~95% of data blocks (partner_id filter)
- Date range filter eliminates additional blocks
- Co-located JOIN (same order_id on same node)
- **Expected Time**: 2-8 seconds for same data

## 💡 MySQL Developer Best Practices for Redshift

### ✅ DO's

#### 1. **Think Distribution First**
```sql
-- MySQL mindset: "Add indexes later"
-- Redshift mindset: "Design distribution at creation"

-- Most important decision:
CREATE TABLE fact_table (...) DISTKEY(most_joined_column);
CREATE TABLE dim_table (...) DISTSTYLE ALL;  -- Small lookup tables
```

#### 2. **Sort by Query Patterns**
```sql
-- MySQL: CREATE INDEX on frequently filtered columns
-- Redshift: SORTKEY on frequently filtered columns

-- If you query: WHERE date >= X AND status = Y
-- Use: SORTKEY(date, status)  -- Order matters!
```

#### 3. **Embrace Column Selection**
```sql
-- MySQL: SELECT * is often OK
-- Redshift: SELECT only needed columns

-- ✅ Good for Redshift
SELECT order_id, amount, date FROM large_table WHERE date >= '2025-01-01';

-- ❌ Bad for Redshift (reads all columns)
SELECT * FROM large_table WHERE date >= '2025-01-01';
```

#### 4. **Use LIMIT for Large Results**
```sql
-- Always use LIMIT for development/testing
SELECT * FROM huge_table 
WHERE complex_conditions
ORDER BY date DESC 
LIMIT 1000;  -- Prevents accidental massive result sets
```

### ❌ DON'Ts

#### 1. **Don't Try to Create Indexes**
```sql
-- ❌ These commands don't exist in Redshift
CREATE INDEX idx_name ON table (column);
DROP INDEX idx_name;
SHOW INDEXES FROM table;
```

#### 2. **Don't Use Row-by-Row Operations**
```sql
-- ❌ Slow in Redshift (designed for bulk operations)
UPDATE table SET status = 'processed' WHERE id = 123;

-- ✅ Better: Bulk operations
INSERT INTO processed_table 
SELECT * FROM source_table WHERE needs_processing;
```

#### 3. **Don't Ignore Distribution**
```sql
-- ❌ Bad: No distribution strategy
CREATE TABLE orders (order_id, customer_id, amount);

-- ✅ Good: Thoughtful distribution
CREATE TABLE orders (...)
DISTKEY(customer_id);  -- Join optimization
```

## 🎓 Mental Model Transition

### From MySQL Thinking to Redshift Thinking

#### MySQL: "Database Handles Everything"
- Create tables with any structure
- Add indexes as needed later
- Database optimizes queries automatically
- Focus on transactional integrity

#### Redshift: "Design for Your Queries"
- Table structure IS your optimization
- Can't change distribution/sort after creation
- Must understand your query patterns first
- Focus on analytical performance

## 📈 Performance Comparison Examples

### Example 1: Large Table Scan
```sql
-- Query: SELECT COUNT(*) FROM orders WHERE order_date >= '2025-01-01';
```

**MySQL (10M rows)**:
- Uses date index if available
- Scans index + fetches rows
- **Time**: 5-15 seconds

**Redshift (10M rows)**:
- Sort key on `order_date`
- Zone maps skip old data blocks
- Columnar scan (COUNT only reads date column)
- **Time**: 1-3 seconds

### Example 2: JOIN Operations
```sql
-- Query: JOIN orders with customers
SELECT o.order_id, c.name, o.amount
FROM orders o 
JOIN customers c ON o.customer_id = c.customer_id
WHERE o.order_date >= '2025-01-01';
```

**MySQL**:
- Uses indexes on join columns
- Nested loop or hash join
- **Time**: 10-30 seconds

**Redshift (Optimized)**:
```sql
-- Both tables: DISTKEY(customer_id)
-- orders table: SORTKEY(order_date, customer_id)
```
- Co-located join (same customer data on same node)
- Sort key eliminates old date blocks
- **Time**: 2-5 seconds

## 🛠️ Practical Migration Guide

### Step 1: Analyze Your MySQL Query Patterns

#### Common MySQL Queries in Your System:
```sql
-- Pattern 1: Partner + Date filtering
SELECT * FROM dw_parcel_detail_tool 
WHERE partner_id = 85 
AND Broker_Handover_Time >= '2025-08-01';

-- Pattern 2: Order lookups
SELECT * FROM dw_parcel_detail_tool 
WHERE order_id IN (1234, 5678, 9012);

-- Pattern 3: JOINs with invoice data
SELECT p.*, i.amount 
FROM dw_parcel_detail_tool p
JOIN dw_ecs_order_invoice i ON p.order_id = i.order_id
WHERE p.partner_id = 85;
```

### Step 2: Design Redshift Table Structure

#### For Your Tables:
```json
{
  "unidw.dw_parcel_detail_tool": {
    "distkey": "order_id",                           // JOIN optimization
    "sortkey": ["partner_id", "col_broker_handover_time", "order_id"],  // Filter optimization
    "reasoning": "partner_id filtered most, date ranges common, order_id for JOINs"
  },
  "unidw.dw_ecs_order_invoice": {
    "distkey": "order_id",                           // Co-locate with parcel data
    "sortkey": ["order_id"],                         // JOIN and ORDER BY optimization  
    "reasoning": "Co-located JOINs, fast ORDER BY"
  }
}
```

### Step 3: Query Pattern Optimization

#### Optimized Query Patterns for Redshift:
```sql
-- ✅ Good: Sort key order matches WHERE clause
SELECT order_id, partner_id, col_broker_handover_time
FROM dw_parcel_detail_tool 
WHERE partner_id = 85                              -- Sort key 1: Fast filter
AND col_broker_handover_time >= '2025-08-01'      -- Sort key 2: Range scan
ORDER BY order_id;                                 -- Sort key 3: Pre-sorted

-- ✅ Good: Co-located JOIN
SELECT p.order_id, p.tno, i.amount
FROM dw_parcel_detail_tool p
JOIN dw_ecs_order_invoice i ON p.order_id = i.order_id  -- Both DISTKEY(order_id)
WHERE p.partner_id = 85;

-- ✅ Good: Column selection (not SELECT *)
SELECT order_id, tno, partner_id, amount          -- Only needed columns
FROM dw_parcel_detail_tool 
WHERE partner_id = 85;
```

## 📊 Performance Tuning Equivalents

### MySQL Performance Tools → Redshift Equivalents

#### Query Analysis
```sql
-- MySQL
EXPLAIN FORMAT=JSON SELECT ...;
SHOW STATUS LIKE 'Last_query_cost';

-- Redshift  
EXPLAIN SELECT ...;
SELECT * FROM stl_query_metrics WHERE query = query_id;
```

#### Index Analysis
```sql
-- MySQL
SHOW INDEX FROM table_name;
SELECT * FROM information_schema.statistics WHERE table_name = 'orders';

-- Redshift
SELECT tablename, distkey, sortkey1, sortkey2 
FROM pg_table_def WHERE tablename = 'orders';
```

#### Performance Monitoring
```sql
-- MySQL
SELECT * FROM performance_schema.events_statements_summary_by_digest;

-- Redshift
SELECT query, total_time, rows, bytes 
FROM stl_query_metrics 
ORDER BY total_time DESC;
```

## 🎯 Common MySQL → Redshift Mistakes to Avoid

### ❌ Mistake 1: Trying to Create Indexes
```sql
-- ❌ Won't work
CREATE INDEX idx_partner ON dw_parcel_detail_tool (partner_id);

-- ✅ Correct approach
-- Add to redshift_keys.json:
{
  "unidw.dw_parcel_detail_tool": {
    "sortkey": ["partner_id"]
  }
}
```

### ❌ Mistake 2: Ignoring Distribution
```sql
-- ❌ Bad: No distribution strategy
CREATE TABLE orders (order_id, customer_id, amount);

-- ✅ Good: JOIN-optimized distribution
CREATE TABLE orders (...) DISTKEY(customer_id);
```

### ❌ Mistake 3: Wrong Sort Key Order
```sql
-- ❌ Bad: Rarely filtered column first
SORTKEY(order_id, partner_id, order_date)

-- ✅ Good: Most selective filter first
SORTKEY(partner_id, order_date, order_id)
-- For queries: WHERE partner_id=85 AND order_date>='2025-01-01'
```

### ❌ Mistake 4: Over-normalization
```sql
-- ❌ MySQL mindset: Many small normalized tables
customers (id, name)
customer_addresses (customer_id, address) 
customer_phones (customer_id, phone)

-- ✅ Redshift mindset: Fewer wider tables  
customer_data (id, name, address, phone)  -- Denormalized for analytics
```

## 🏃‍♀️ Quick Start for MySQL Developers

### 1. **Forget About Indexes** 
Think about table structure instead:
- What columns do you JOIN on? → DISTKEY
- What columns do you filter on? → SORTKEY  
- How big is the table? → DISTSTYLE

### 2. **Design Questions to Ask**
Before creating any Redshift table:
- What's your most common WHERE clause?
- What tables do you JOIN this with?
- Do you need fast lookups or fast aggregations?
- Is this a small lookup table or large fact table?

### 3. **Start with These Patterns**

#### Small Reference Tables (< 100K rows):
```json
{
  "schema.small_lookup": {
    "diststyle": "ALL",
    "sortkey": ["primary_key"]
  }
}
```

#### Large Fact Tables:
```json
{
  "schema.large_facts": {
    "distkey": "most_joined_foreign_key", 
    "sortkey": ["most_filtered_column", "date_column"]
  }
}
```

#### Time-Series Data:
```json
{
  "schema.events": {
    "distkey": "entity_id",
    "sortkey": ["timestamp", "entity_id"]
  }
}
```

## 💻 Hands-On Exercise for Your Team

### Before You Start
Review your most common queries and identify:
1. Most frequently filtered columns
2. Most common JOIN patterns  
3. Date range query patterns

### Exercise: Optimize Your Main Table
```bash
# 1. Create redshift_keys.json with your pattern
{
  "unidw.dw_parcel_detail_tool": {
    "distkey": "order_id",
    "sortkey": ["partner_id", "col_broker_handover_time"]
  }
}

# 2. Test sync
python -m src.cli.main sync -t unidw.dw_parcel_detail_tool --limit 10000

# 3. Test your frequent query
-- Run your common queries and measure performance

# 4. Compare before/after performance
```

## 📚 Further Reading

### **For Your Team:**
- **AWS Redshift Best Practices**: Official AWS documentation
- **Redshift Sort Key Design**: AWS performance tuning guide  
- **Distribution Key Strategies**: AWS architecture patterns

### **In This Repository:**
- `REDSHIFT_OPTIMIZATION_GUIDE.md` - Detailed optimization examples
- `USER_MANUAL.md` - Complete command reference
- `redshift_keys.json.example` - Configuration examples

## 🚫 Critical Limitation: One Sort Key Per Table

### Your MySQL Reality (Multiple Indexes)
Looking at your actual `dw_parcel_detail_tool` table, you have **9 different indexes**:

```sql
-- MySQL: Multiple indexes for different query patterns ✅
KEY `dw_parcel_detail_tool_partner_id_IDX` (`partner_id`),
KEY `dw_parcel_detail_tool_190_time_IDX` (`190_time`),
KEY `dw_parcel_detail_tool_order_id_IDX` (`order_id`),
KEY `idx_partner_last_event_order` (`partner_id`,`Last_Event`,`order_id`),
KEY `idx_partner_190_time` (`partner_id`,`190_time`),
KEY `idx_partner_broker_handover_time` (`partner_id`,`Broker_Handover_Time`),
-- ... and more
```

**Each index supports different queries efficiently.**

### Redshift Reality (One Sort Key Only)
```sql
-- Redshift: Only ONE sort key choice ❌
CREATE TABLE dw_parcel_detail_tool (...)
COMPOUND SORTKEY(partner_id, col_broker_handover_time, order_id);  -- ONE choice only
```

## 🎯 Strategies for Multiple Query Patterns

Since you can only have ONE sort key, you need to prioritize and use different strategies:

### Strategy 1: Optimize for Most Frequent Query
**Choose the 80/20 rule** - optimize for your most common query pattern:

```json
{
  "unidw.dw_parcel_detail_tool": {
    "distkey": "order_id",
    "sortkey": ["partner_id", "col_broker_handover_time", "order_id"],
    "reasoning": "Optimizes most frequent: partner + time range queries"
  }
}
```

**Trade-off**: 
- ✅ Super fast: `WHERE partner_id = 85 AND Broker_Handover_Time >= 'date'`
- ⚠️ Slower: `WHERE 190_time >= 'date'` (not using sort key)

### Strategy 2: Multiple Table Versions (Advanced)

For completely different access patterns, create specialized versions:

```sql
-- Version 1: Optimized for partner queries
CREATE TABLE dw_parcel_detail_tool_by_partner (...)
SORTKEY(partner_id, col_broker_handover_time);

-- Version 2: Optimized for time-based queries  
CREATE TABLE dw_parcel_detail_tool_by_time (...)
SORTKEY(col_190_time, order_id);

-- Version 3: Optimized for order lookups
CREATE TABLE dw_parcel_detail_tool_by_order (...)
SORTKEY(order_id);
```

**Use Case**: Different teams/reports use different optimized versions.

### Strategy 3: Query Pattern Consolidation
**Redesign queries** to use the chosen sort key:

```sql
-- ❌ Old query (doesn't use sort key)
SELECT * FROM dw_parcel_detail_tool WHERE 190_time >= '2025-08-01';

-- ✅ Modified query (uses sort key)  
SELECT * FROM dw_parcel_detail_tool 
WHERE partner_id IN (85, 90, 95)  -- Use sort key first
AND col_190_time >= '2025-08-01';
```

### Strategy 4: Interleaved Sort Keys (Equal Priority)
If your queries have equal importance on different columns:

```json
{
  "unidw.dw_parcel_detail_tool": {
    "distkey": "order_id",
    "interleaved_sortkey": ["partner_id", "col_190_time", "col_broker_handover_time", "order_id"],
    "reasoning": "Equal weight queries on partner, time fields"
  }
}
```

**Trade-off**:
- ✅ Good performance across multiple patterns
- ❌ Not as fast as compound sort for specific pattern

## 📊 Decision Framework for Your Team

### Step 1: Analyze Your Query Frequency
Based on your indexes, prioritize:

```sql
-- Query Pattern Analysis
-- 1. Most frequent pattern (choose for sort key)
WHERE partner_id = ? AND Broker_Handover_Time BETWEEN ? AND ?

-- 2. Second most frequent  
WHERE partner_id = ? AND Last_Event = ? AND order_id > ?

-- 3. Less frequent patterns
WHERE 190_time >= ?
WHERE order_id = ?
WHERE tno = ?
```

### Step 2: Choose Primary Optimization

#### Option A: Partner-Time Queries (Recommended)
```json
{
  "unidw.dw_parcel_detail_tool": {
    "distkey": "order_id",
    "sortkey": ["partner_id", "col_broker_handover_time", "order_id"],
    "optimizes": "Partner + time range queries (your most frequent)"
  }
}
```

#### Option B: Equal Weight Optimization
```json
{
  "unidw.dw_parcel_detail_tool": {
    "distkey": "order_id", 
    "interleaved_sortkey": ["partner_id", "col_190_time", "col_broker_handover_time"],
    "optimizes": "Multiple time-based queries equally"
  }
}
```

### Step 3: Handle Secondary Patterns

#### For Non-Optimized Queries:
```sql
-- Query: WHERE 190_time >= '2025-08-01'
-- Performance: Slower (full table scan)
-- Mitigation: Add additional filters when possible

-- ✅ Better version:
WHERE partner_id IN (85, 90, 95)  -- Use sort key
AND col_190_time >= '2025-08-01';  -- Additional filter
```

## 💡 Practical Recommendations for Your Table

### Recommended Configuration
Based on your frequent query pattern:

```json
{
  "unidw.dw_parcel_detail_tool": {
    "distkey": "order_id",
    "sortkey": ["partner_id", "col_broker_handover_time", "order_id"],
    "table_type": "fact",
    "description": "Optimized for partner + time range queries with JOIN support"
  },
  "unidw.dw_ecs_order_invoice": {
    "distkey": "order_id",
    "sortkey": ["order_id"], 
    "table_type": "fact",
    "description": "Co-located with parcel data for fast JOINs"
  }
}
```

### Query Performance Expectations

#### ✅ **Super Fast** (Uses sort key):
```sql
WHERE partner_id = 85 AND Broker_Handover_Time BETWEEN '2025-08-04' AND '2025-08-18'
-- Expected: 2-5 seconds (vs 30-60 seconds without optimization)
```

#### ⚠️ **Moderate Speed** (Partial sort key use):
```sql
WHERE partner_id = 85 AND Last_Event = 1  
-- Expected: 5-15 seconds (better than full scan, not as fast as time range)
```

#### ❌ **Slower** (No sort key use):
```sql
WHERE 190_time >= '2025-08-01'  -- No partner_id filter
-- Expected: 20-45 seconds (full column scan)
```

## 🎯 Key Takeaways

1. **Redshift ≠ MySQL**: Different architecture requires different thinking
2. **Table Design = Performance**: No indexes means table structure IS your optimization
3. **Distribution Matters**: Plan for JOINs from day one
4. **Sort Keys = MySQL Indexes**: But designed at table creation, not added later
5. **Analytics First**: Optimize for aggregations and scans, not point lookups
6. **⚠️ ONE Sort Key Limit**: Unlike MySQL's multiple indexes, choose your primary pattern
7. **Query Adaptation**: Sometimes modify queries to use available sort keys

**Bottom Line**: Your MySQL expertise is valuable, but Redshift requires upfront design thinking rather than reactive index tuning. **You must prioritize your most important query pattern** since you can only optimize for one pattern per table.

---

*Remember: In MySQL you add indexes to fix slow queries. In Redshift, you design ONE table optimization to prevent slow queries.*