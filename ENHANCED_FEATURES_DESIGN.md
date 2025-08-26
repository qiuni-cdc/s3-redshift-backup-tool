
# Design Proposal: Next-Generation Data Pipeline Features (V3)

## 1. Introduction: Why These Features Are Needed

The S3-Redshift Backup Tool has proven to be a robust utility for its core purpose. To evolve it from a specific-purpose tool into a flexible, enterprise-grade data integration platform, we must address key architectural limitations. This document proposes solutions for three critical features that will significantly enhance the tool's power, flexibility, and reliability.

The current design has three main constraints:
1.  **Rigid Schemas:** The tool is coupled to a single, pre-configured MySQL source schema and a single Redshift target schema. This prevents it from being easily reused across different projects or for syncing data from multiple source databases.
2.  **Fact-Table Focus:** The architecture is optimized for append-only fact tables. It lacks the ability to handle dimensional data, which is fundamental to data warehousing, especially the need to track historical changes to entities like customers or products.
3.  **Brittle Change Data Capture (CDC):** The reliance on a hardcoded `update_at` column for incremental logic is inflexible and can lead to data integrity issues if tables use different column names or require more sophisticated change tracking.

Addressing these points will elevate the tool into a generic, powerful, and highly reliable data pipeline solution.

---

## 2. Feature 1: Flexible Database Schemas

### Requirement
The tool must support connecting to user-specified source (MySQL) and target (Redshift) schemas for any given operation, rather than relying on a single, static configuration.

### Recommended Solution
The recommended approach is to make the core application schema-aware by **parameterizing the schemas at the CLI level for each run**. This provides maximum flexibility while keeping the core logic clean.

### Key Code Snippets

**1. Enhanced CLI in `src/cli/main.py`**

The `sync` command will be updated to accept the source and target schemas, and the `--tables` option will be simplified.

```python
# src/cli/main.py

@cli.command()
@click.option('--source-db', required=True, help='Source database schema in MySQL')
@click.option('--target-schema', required=True, help='Target schema in Redshift')
@click.option('--tables', '-t', multiple=True, required=True, 
              help='Tables to sync (table names only, no schema)')
def sync(ctx, source_db: str, target_schema: str, tables: List[str], ...):
    # ...
    # Pass the new parameters down to the execution logic
    backup_strategy.execute(
        source_db=source_db,
        target_schema=target_schema,
        tables=list(tables), 
        ...
    )
```

**2. Data Isolation in S3 (`src/core/s3_manager.py`)**

To prevent data collisions, the S3 key generation logic must be updated to include the schema names.

```python
# src/core/s3_manager.py

def generate_s3_key(
    self, 
    source_db: str, 
    target_schema: str, 
    table_name: str, 
    timestamp: str, 
    batch_id: int
) -> str:
    """Generates a unique S3 key with schema isolation."""
    base_path = self.config.s3.incremental_path.strip('/')
    date_partition = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S').strftime('year=%Y/month=%m/day=%d')
    
    # New path includes source and target schemas for isolation
    # e.g., incremental/sales_db/reporting_sales/customers/year=.../customers_....parquet
    return f"{base_path}/{source_db}/{target_schema}/{table_name}/{date_partition}/{table_name}_{timestamp.replace(' ', '_')}_{batch_id}.parquet"
```

---

## 3. Feature 2: Dimensional Table Support (SCD Type 2)

### Requirement
The tool must be able to handle dimensional data and track its historical changes. The industry-standard pattern for this is the **Slowly Changing Dimension (SCD) Type 2**.

### Recommended Solution
The most robust and efficient implementation is the **Staging Table & MERGE Strategy**. This follows a modern ELT (Extract, Load, Transform) pattern, using Redshift's own processing power for the complex data transformation, which is superior to handling it in Python.

### Key Code Snippets

**1. Redshift SCD Type 2 Table DDL**

This is the target table structure required in Redshift. It includes a new surrogate primary key and columns to track the history of each record.

```sql
-- DDL for the target dimension table in Redshift
CREATE TABLE reporting_schema.dim_customers (
    customer_sk INT IDENTITY(1,1),          -- Surrogate Key (new, auto-incrementing PK)
    customer_id INT NOT NULL,               -- Natural Key (the ID from MySQL)
    customer_name VARCHAR(255),
    customer_address VARCHAR(255),
    customer_tier VARCHAR(50),
    -- SCD Type 2 Columns --
    start_date DATE NOT NULL,
    end_date DATE,
    is_current BOOLEAN NOT NULL,
    PRIMARY KEY(customer_sk)
);
```

**2. Redshift `MERGE` Logic for Transformation**

This is the core SQL logic that runs in Redshift to apply the changes from the staging table to the final dimension table. It should be executed after the daily full extract has been loaded into the staging table.

```sql
-- This single MERGE statement handles both new customers and changed customers.
MERGE INTO reporting_schema.dim_customers target
USING reporting_schema.staging_customers stage
ON target.customer_id = stage.customer_id AND target.is_current = TRUE

-- Condition 1: A customer exists and their details have changed.
WHEN MATCHED AND (target.customer_address <> stage.customer_address OR target.customer_tier <> stage.customer_tier) THEN
    -- Action: Expire the old record by setting its end_date and current flag.
    UPDATE SET
        end_date = GETDATE(),
        is_current = FALSE

-- Condition 2: The customer from the staging table does not exist in the dimension table.
WHEN NOT MATCHED THEN
    -- Action: Insert the brand new customer record.
    INSERT (customer_id, customer_name, customer_address, customer_tier, start_date, end_date, is_current)
    VALUES (stage.customer_id, stage.customer_name, stage.customer_address, stage.customer_tier, GETDATE(), NULL, TRUE);

-- After the MERGE, this second statement inserts the *new version* of the updated records.
INSERT INTO reporting_schema.dim_customers (customer_id, customer_name, customer_address, customer_tier, start_date, end_date, is_current)
SELECT
    s.customer_id,
    s.customer_name,
    s.customer_address,
    s.customer_tier,
    GETDATE() as start_date,
    NULL as end_date,
    TRUE as is_current
FROM reporting_schema.staging_customers s
-- This join condition finds the records we just expired in the MERGE statement.
JOIN reporting_schema.dim_customers d ON s.customer_id = d.customer_id AND d.end_date = GETDATE();
```

---

## 4. Feature 3: Flexible Change Data Capture (CDC)

### Requirement
The tool must move beyond a hardcoded `update_at` column and support multiple, configurable methods for detecting changes in source tables.

### Recommended Solution
The solution is to implement a **per-table configurable CDC strategy** via a new `conf/tables.yml` file. This provides maximum flexibility and makes the behavior for each table explicit and clear.

### Supported CDC Strategies

Below are the strategies that will be supported, implemented within a dynamic query generation method in `src/backup/base.py`.

#### 1. `hybrid` (Recommended Default)
*   **Description:** This is the most robust incremental strategy. It uses a combination of a timestamp column and a unique, sequential ID column to reliably capture both new and updated rows, preventing issues with same-second updates.
*   **Required Columns:** An update timestamp column (e.g., `update_at`) and a unique sequential ID (e.g., `ID`).
*   **Best For:** Any transactional table where reliably capturing all changes is critical. This should be the default choice.
*   **Code Snippet:**
    ```python
    # src/backup/base.py -> get_incremental_query
    if strategy == "hybrid":
        ts_col = table_config["cdc_timestamp_column"]
        id_col = table_config["cdc_id_column"]
        last_ts = watermark.last_mysql_data_timestamp or '1970-01-01 00:00:00'
        last_id = watermark.last_processed_id or 0

        return f"""
            SELECT * FROM {table_name}
            WHERE ({ts_col} > '{last_ts}') OR ({ts_col} = '{last_ts}' AND {id_col} > {last_id})
            ORDER BY {ts_col}, {id_col}
        """
    ```

#### 2. `full_sync` (Safe Fallback)
*   **Description:** This strategy does not perform incremental logic. Instead, it takes a complete copy of the source table during every run and uses it to completely overwrite the data at the destination.
*   **Required Columns:** None.
*   **Best For:** The recommended strategy for any table that lacks a reliable update timestamp, or for small-to-medium sized dimension tables where a full daily refresh is acceptable.
*   **Code Snippet:**
    ```python
    # src/backup/base.py -> get_incremental_query
    elif strategy == "full_sync":
        return f"SELECT * FROM {table_name}"
    ```

#### 3. `id_only` (For Append-Only Tables)
*   **Description:** This strategy only uses a sequential ID column to find new rows. **Warning:** It is only safe to use if you can guarantee that rows in the source table are never updated, only inserted, as it will not capture updates.
*   **Required Columns:** A unique, sequential ID column.
*   **Best For:** Immutable, append-only tables, such as event logs or logging tables.
*   **Code Snippet:**
    ```python
    # src/backup/base.py -> get_incremental_query
    elif strategy == "id_only":
        id_col = table_config["cdc_id_column"]
        last_id = watermark.last_processed_id or 0
        return f"SELECT * FROM {table_name} WHERE {id_col} > {last_id} ORDER BY {id_col}"
    ```

#### 4. `timestamp_only` (Legacy/Simple)
*   **Description:** This is the original, less reliable incremental strategy. It uses only a timestamp column to find new and updated rows.
*   **Required Columns:** An update timestamp column.
*   **Best For:** Simple use cases where the small risk of missing updates that occur within the same second is acceptable. The `hybrid` strategy is superior in almost all cases.
*   **Code Snippet:**
    ```python
    # src/backup/base.py -> get_incremental_query
    else: # Default to timestamp-only if no strategy is defined
        last_ts = watermark.last_mysql_data_timestamp or '1970-01-01 00:00:00'
        return f"SELECT * FROM {table_name} WHERE update_at > '{last_ts}' ORDER BY update_at"
    ```
