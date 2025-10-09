# Redshift AUTO Optimization Feature

## Overview

The system now supports **independent AUTO optimization** for DISTSTYLE and SORTKEY, giving you granular control over Redshift table optimization.

## Configuration Options

### 1. **AUTO for Both DISTSTYLE and SORTKEY**

Use Redshift's automatic optimization for both distribution and sorting:

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
CREATE TABLE ... 
DISTSTYLE AUTO
SORTKEY AUTO;
```

---

### 2. **Manual DISTKEY, AUTO for SORTKEY**

Specify distribution column, let Redshift choose sort order:

```json
{
  "settlement.settle_orders": {
    "distkey": "tracking_number",
    "sortkey": "AUTO"
  }
}
```

**Generates:**
```sql
CREATE TABLE ... 
DISTKEY(tracking_number)
SORTKEY AUTO;
```

---

### 3. **Full Manual Control (Original Behavior)**

Explicitly define both:

```json
{
  "settlement.parcel_detail": {
    "distkey": "parcel_id",
    "sortkey": ["parcel_id", "create_at"]
  }
}
```

**Generates:**
```sql
CREATE TABLE ... 
DISTKEY(parcel_id)
COMPOUND SORTKEY(parcel_id, create_at);
```

---

## All Configuration Options

### DISTKEY Options

| Option | Value | Result |
|--------|-------|--------|
| `diststyle` | `"AUTO"` | `DISTSTYLE AUTO` |
| `diststyle` | `"ALL"` | `DISTSTYLE ALL` |
| `diststyle` | `"EVEN"` | `DISTSTYLE EVEN` |
| `distkey` | `"column_name"` | `DISTKEY(column_name)` |

### SORTKEY Options

| Option | Value | Result |
|--------|-------|--------|
| `sortkey` | `"AUTO"` | `SORTKEY AUTO` |
| `sortkey` | `"column_name"` | `SORTKEY(column_name)` |
| `sortkey` | `["col1", "col2"]` | `COMPOUND SORTKEY(col1, col2)` |
| `interleaved_sortkey` | `["col1", "col2"]` | `INTERLEAVED SORTKEY(col1, col2)` |

---

## Complete Example Configuration

```json
{
  "settlement.settle_orders": {
    "distkey": "tracking_number",
    "sortkey": ["tracking_number", "create_at"]
  },

  "settlement.settlement_normal_delivery_detail": {
    "diststyle": "EVEN",
    "sortkey": ["create_at", "update_at"]
  },

  "unidw.dw_parcel_pricing_temp": {
    "diststyle": "AUTO",
    "sortkey": "AUTO"
  },

  "settlement.parcel_detail": {
    "distkey": "parcel_id",
    "interleaved_sortkey": ["parcel_id", "create_at", "status"]
  },

  "example.reporting_table": {
    "diststyle": "ALL",
    "sortkey": "AUTO",
    "_comment": "Small dimension tables with AUTO sortkey"
  },

  "example.fact_table": {
    "distkey": "order_id",
    "sortkey": ["order_date", "customer_id"],
    "_comment": "Manual optimization for well-understood fact tables"
  }
}
```

---

## When to Use AUTO

### ✅ **Use AUTO for:**
- Tables with unknown or unpredictable query patterns
- New tables where workload isn't established yet
- Tables that serve multiple different use cases
- When you want Redshift to adapt to changing workloads
- Development/staging environments

### ⚠️ **Use Manual for:**
- Production tables with well-understood query patterns
- Tables with consistent, predictable access patterns
- Performance-critical tables where you need full control
- Tables with specific join patterns (use matching DISTKEY)
- Dimension tables (use `DISTSTYLE ALL`)

---

## Migration Path

If you have existing configurations, they continue to work:

**Before (still works):**
```json
{
  "settlement.settle_orders": {
    "distkey": "tracking_number",
    "sortkey": ["tracking_number", "create_at"]
  }
}
```

**After (with AUTO):**
```json
{
  "settlement.settle_orders": {
    "distkey": "tracking_number",
    "sortkey": "AUTO"
  }
}
```

---

## Feature Highlights

✅ **Independent Control**: Set DISTKEY and SORTKEY separately  
✅ **Backward Compatible**: All existing configurations still work  
✅ **Flexible**: Mix AUTO and manual settings as needed  
✅ **Simple**: Just use `"AUTO"` as a string value  
✅ **Production Ready**: Validated and tested


---

## Default Behavior (No Custom Config)

If a table is **NOT** in `redshift_keys.json`, the system uses **intelligent auto-detection**:

### **DISTKEY Auto-Detection**

The system automatically chooses distribution keys based on table patterns:

| Table Pattern | DISTKEY Choice | Rationale |
|---------------|----------------|-----------|
| `settle_orders` | `tracking_number` | Primary business key for order tables |
| Tables with `id`/`pk` columns | First primary key column | Natural distribution on unique ID |
| `parcel*` tables | First column containing "parcel" | Parcel-centric distribution |
| Other tables | First primary key candidate | Fallback to auto_increment or id column |

**Example:**
```python
# For table: settlement.settle_orders
# Auto-generates: DISTKEY(tracking_number)

# For table: settlement.order_details  
# Auto-generates: DISTKEY(id)
```

---

### **SORTKEY Auto-Detection**

The system automatically chooses sort keys based on table patterns:

| Table Pattern | SORTKEY Choice | Rationale |
|---------------|----------------|-----------|
| `settle_orders` | `tracking_number, create_at` | Business key + timestamp for time-range queries |
| Tables with timestamps | `create_at, update_at` (up to 2) | Optimizes time-range queries |
| Other tables | No sortkey | Avoids overhead on tables without clear patterns |

**Example:**
```python
# For table: settlement.settle_orders
# Auto-generates: SORTKEY(tracking_number, create_at)

# For table: settlement.payment_records
# Auto-generates: SORTKEY(create_at, update_at)
```

---

### **When Default Logic Applies**

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
  
  // Tables NOT listed here use auto-detection:
  // - settlement.order_items → Uses default logic
  // - kuaisong.shipments → Uses default logic
}
```

For the unlisted tables, the system will:
1. Analyze table name and column metadata
2. Apply pattern-based rules (settle_orders, parcel tables, etc.)
3. Fallback to sensible defaults (id columns, timestamps)
4. Generate optimized DDL automatically

---

### **Decision Tree**

```
Table optimization logic:
│
├─ In redshift_keys.json? 
│  ├─ YES → Use custom config (diststyle/distkey/sortkey)
│  └─ NO ↓
│
└─ Use Auto-Detection:
   │
   ├─ DISTKEY Selection:
   │  ├─ Is table "settle_orders"? → tracking_number
   │  ├─ Has primary key columns? → First PK
   │  ├─ Is "parcel*" table? → First parcel column
   │  └─ Default → First id/auto_increment column
   │
   └─ SORTKEY Selection:
      ├─ Is table "settle_orders"? → tracking_number, create_at
      ├─ Has timestamp columns? → create_at, update_at (max 2)
      └─ Default → No sortkey
```

---

### **Override Auto-Detection**

To explicitly disable auto-detection defaults, use `DISTSTYLE AUTO`:

```json
{
  "settlement.order_items": {
    "diststyle": "AUTO",
    "sortkey": "AUTO"
  }
}
```

This tells Redshift to manage optimization automatically instead of using our pattern-based rules.

---

## Configuration Priority

The system applies optimization in this order:

1. **Explicit Custom Config** (highest priority)
   - `redshift_keys.json` entries take precedence
   
2. **Pattern-Based Auto-Detection** 
   - `settle_orders` → tracking_number + timestamps
   - `parcel*` → parcel columns
   - Tables with timestamps → timestamp sortkeys
   
3. **Column Analysis**
   - Primary keys, auto_increment columns
   - Timestamp columns (create_at, update_at)
   
4. **No Optimization** (lowest priority)
   - If no patterns match and no PK found
   - Table created without DISTKEY/SORTKEY

---

## Examples: Custom vs Auto-Detection

### **Scenario 1: Rely on Auto-Detection**

```json
{
  // redshift_keys.json is empty or doesn't include this table
}
```

**Result for `settlement.settle_orders`:**
```sql
CREATE TABLE settle_orders (...)
DISTKEY(tracking_number)
SORTKEY(tracking_number, create_at);
```

---

### **Scenario 2: Override with Custom Config**

```json
{
  "settlement.settle_orders": {
    "distkey": "customer_id",
    "sortkey": ["order_date", "customer_id"]
  }
}
```

**Result:**
```sql
CREATE TABLE settle_orders (...)
DISTKEY(customer_id)
COMPOUND SORTKEY(order_date, customer_id);
```

---

### **Scenario 3: Mix AUTO and Manual**

```json
{
  "settlement.settle_orders": {
    "distkey": "tracking_number",
    "sortkey": "AUTO"
  }
}
```

**Result:**
```sql
CREATE TABLE settle_orders (...)
DISTKEY(tracking_number)
SORTKEY AUTO;
```

---

## Best Practices

### ✅ **For Production:**
- Start with auto-detection (no config)
- Monitor query performance
- Add custom configs for problem tables
- Use AUTO for unpredictable workloads

### ✅ **For New Tables:**
- Use `"diststyle": "AUTO"` and `"sortkey": "AUTO"` initially
- Let Redshift learn your patterns
- Switch to manual after understanding workload

### ✅ **For Critical Tables:**
- Define explicit DISTKEY based on joins
- Define explicit SORTKEY based on WHERE/ORDER BY clauses
- Avoid AUTO for performance-critical tables

