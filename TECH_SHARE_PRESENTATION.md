# S3-Redshift Backup Tool - Tech Share

## Presentation Overview

**Duration**: 30-45 minutes
**Audience**: Engineers, Data Team, DevOps
**Goal**: Share architecture decisions, key features, and lessons learned

---

## 1. Introduction (5 min)

### What Problem Does This Solve?
Currently MySQL database system cannot support high frequent, large volume data loading and extraction, therefore we need to migrate our data sources from MySQL data warehouse to Redshift to support our settlement needs. This tool provides a robust, incremental data pipeline to efficiently sync MySQL data into Redshift via S3 staging. 

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   MySQL     │ ──► │     S3      │ ──► │  Redshift   │
│  (Source)   │     │  (Staging)  │     │   (DW)      │
└─────────────┘     └─────────────┘     └─────────────┘
     ↑                    ↑                    ↑
  Incremental         Parquet            COPY Command
   Chunking          Compression          + Manifest
```

### Key Stats

| Metric | Value |
|--------|-------|
| Codebase Size | ~15,000 lines Python |
| Production Connections | 20+ MySQL sources |
| Records Processed | 20M+ rows verified |
| Performance Gain | Up to 10x query speed improvement |

---

## 2. Architecture Deep Dive (10 min)

### 2.1 Modular Design

```
s3-redshift-backup-tool/
│
├── config/
│   ├── connections.yml          # MySQL, S3, Redshift connection configs
│   └── pipelines/               # YAML pipeline definitions
│
├── src/
│   ├── backup/                  # Backup Strategies
│   │   ├── row_based.py         # Row-based chunking with sparse detection
│   │   ├── sequential.py        # Single-threaded table processing, actively used
│   │   └── parallel.py          # Multi-threaded processing, no longer used
│   │
│   ├── core/                    # Core Infrastructure
│   │   ├── connections.py       # SSH tunnels, DB connection pooling
│   │   ├── connection_registry.py   # Multi-source routing
│   │   ├── s3_manager.py        # Parquet uploads, compression
│   │   ├── simple_watermark_manager.py  # Incremental state tracking
│   │   ├── cdc_strategy_engine.py   # 5 CDC strategies
│   │   ├── flexible_schema_manager.py   # Dynamic schema discovery
│   │   ├── redshift_loader.py   # S3→Redshift COPY operations
│   │   └── column_mapper.py     # Column name compatibility
│   │
│   ├── cli/                     # Command Line Interface
│   │   └── main.py              # Click-based CLI entry point
│   │
│   └── utils/                   # Utilities
│       ├── exceptions.py        # Custom error types
│       ├── logging.py           # Structured logging
│       └── validation.py        # Data validation
│
└── parcel_download_tool_etl/    # ETL for incremental parcel data
```

### 2.2 Backup Module Hierarchy

The backup system uses a layered architecture with clear separation of concerns:

```
┌─────────────────────────────────────────────────────────────────┐
│                    SequentialBackupStrategy                     │  ← Entry Point (CLI calls this)
│                    (sequential.py)                              │
│  • Orchestrates table-by-table processing                       │
│  • Handles retry logic & error recovery                         │
│  • Tracks per-table metrics                                     │
└─────────────────────────────────────────────────────────────────┘
                              │
                              │ extends
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    RowBasedBackupStrategy                       │  ← Core Logic
│                    (row_based.py)                               │
│  • Timestamp + ID pagination (exact row counts)                 │
│  • Sparse sequence detection & early termination                │
│  • Watermark ceiling protection                                 │
│  • Chunk processing with configurable batch_size                │
└─────────────────────────────────────────────────────────────────┘
                              │
                              │ extends
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    BaseBackupStrategy                           │  ← Foundation
│                    (base.py)                                    │
│  • Abstract base class (ABC)                                    │
│  • Connection management (database_session context)             │
│  • S3 upload operations (_process_batch_with_retries)           │
│  • Memory management & garbage collection                       │
│  • Metrics collection (BackupMetrics)                           │
└─────────────────────────────────────────────────────────────────┘
                              │
                              │ uses
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    CDCBackupIntegration                         │  ← CDC Bridge
│                    (cdc_backup_integration.py)                  │
│  • Bridges backup strategies with CDC Strategy Engine           │
│  • Version compatibility (v1.0 → v1.1 → v1.2)                   │
│  • Strategy caching per table                                   │
│  • Pipeline config → CDC config translation                     │
└─────────────────────────────────────────────────────────────────┘
```

**Module Responsibilities:**

| Module | Lines | Responsibility |
|--------|-------|----------------|
| `base.py` | ~1,500 | Foundation: connections, S3, memory, metrics |
| `row_based.py` | ~1,900 | Core chunking logic, sparse detection, watermarks |
| `sequential.py` | ~400 | Table orchestration, error handling |
| `cdc_backup_integration.py` | ~340 | CDC strategy selection & compatibility |

**Key Optimizations in RowBasedBackupStrategy:**

*Early Termination (Sparse Sequence Detection):*
```
Problem: Tables with ID gaps waste queries
  ID: 1, 2, 3, ... 1000, [GAP], 50000, 50001, ...

Solution: Monitor chunk efficiency, stop when sparse
  efficiency = rows_found / chunk_size
  if efficiency < 1%: STOP  ← Don't waste more queries

Result: 96% query reduction (26 queries → 1 query)
```

*Watermark Ceiling Protection:*
```
Problem: Continuous data injection = infinite sync
  While syncing: new rows keep arriving → never finishes

Solution: Capture max ID at sync start, never exceed
  ceiling_id = SELECT MAX(id) FROM table  # At sync START

  During sync: only process WHERE id <= ceiling_id
  New rows (id > ceiling_id) → wait for next sync

Result: Guaranteed sync completion, no infinite loops
```

*⚠️ Known Limitation: Large GAP Scenario*
```
Scenario: ID 1-1000, [GAP 1001-49999], 50000-100000

  watermark_ceiling = 100000 (MAX(id) at sync start)

  Chunk 1: SELECT * WHERE id > 0 LIMIT 50000
           返回: 1000 行 (id 1-1000)
           效率: 2% → 继续

  Chunk 2: SELECT * WHERE id > 1000 LIMIT 50000
           返回: 0 行 (GAP区域没数据!)

           ❌ rows_in_chunk == 0 → break (NO_MORE_DATA)

  问题: id 50000-100000 的数据被跳过!
        watermark_ceiling 检查只在有数据时执行

Why this happens (row_based.py:363-369):
  if rows_in_chunk == 0:
      break  # NO_MORE_DATA - exits before ceiling check

Current workaround:
  - Run sync multiple times until no more data
  - Or use full_sync mode for tables with extreme gaps

Future fix consideration:
  - Check ceiling before breaking on zero rows
  - If last_id < ceiling: continue with larger chunk range
```

**Module Relationship Diagram:**

```
                              CLI Command
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                        SequentialBackupStrategy                             │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │  execute(tables):                                                     │  │
│  │      for table in tables:                                             │  │
│  │          _process_single_table_row_based(table) ──────────────────────┼──┼──┐
│  └───────────────────────────────────────────────────────────────────────┘  │  │
└─────────────────────────────────────────────────────────────────────────────┘  │
              │ extends                                                          │
              ▼                                                                  │
┌─────────────────────────────────────────────────────────────────────────────┐  │
│                        RowBasedBackupStrategy                               │  │
│  ┌───────────────────────────────────────────────────────────────────────┐  │  │
│  │  _process_single_table_row_based(table):  ◄─────────────────────────────┼──┘
│  │      cdc = cdc_integration.get_cdc_strategy(table) ───────────────────┼──┼──┐
│  │      query = cdc.build_incremental_query(watermark)                   │  │  │
│  │      while has_data:                                                  │  │  │
│  │          chunk = _get_next_chunk(cursor, chunk_size)                  │  │  │
│  │          if efficiency < 1%: break  # Sparse detection                │  │  │
│  │          _process_batch_with_retries(chunk) ──────────────────────────┼──┼──┼──┐
│  └───────────────────────────────────────────────────────────────────────┘  │  │  │
└─────────────────────────────────────────────────────────────────────────────┘  │  │
              │ extends                                                          │  │
              ▼                                                                  │  │
┌─────────────────────────────────────────────────────────────────────────────┐  │  │
│                        BaseBackupStrategy                                   │  │  │
│  ┌───────────────────────────────────────────────────────────────────────┐  │  │  │
│  │  _process_batch_with_retries(batch):  ◄─────────────────────────────────┼──┼──┘
│  │      df = pd.DataFrame(batch)                                         │  │  │
│  │      s3_manager.upload_parquet(df)  ──────────────────────────────────┼──┼──┼──┐
│  │                                                                       │  │  │  │
│  │  database_session():  # MySQL connection lifecycle                    │  │  │  │
│  │  BackupMetrics:       # Rows, bytes, duration tracking                │  │  │  │
│  │  MemoryManager:       # GC triggers, memory limits                    │  │  │  │
│  └───────────────────────────────────────────────────────────────────────┘  │  │  │
└─────────────────────────────────────────────────────────────────────────────┘  │  │
                                                                                 │  │
              ┌──────────────────────────────────────────────────────────────────┘  │
              ▼                                                                     │
┌─────────────────────────────────────────────────────────────────────────────┐     │
│                        CDCBackupIntegration                                 │     │
│  ┌───────────────────────────────────────────────────────────────────────┐  │     │
│  │  get_cdc_strategy(table):                                             │  │     │
│  │      # Read pipeline config → Determine CDC type → Return strategy    │  │     │
│  │      return CDCStrategyFactory.create(hybrid|timestamp|id_only|...)   │  │     │
│  └───────────────────────────────────────────────────────────────────────┘  │     │
└─────────────────────────────────────────────────────────────────────────────┘     │
                                                                                    │
              ┌─────────────────────────────────────────────────────────────────────┘
              ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           S3Manager                                         │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │  upload_parquet(df):                                                  │  │
│  │      # DataFrame → Parquet → S3 (multipart, compression)              │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────┘
```

**Simplified View:**

```
┌──────────────────────────────────────────────────────────────────┐
│                     INHERITANCE CHAIN                            │
│                                                                  │
│   Sequential ──extends──► RowBased ──extends──► Base             │
│       │                      │                    │              │
│   (orchestrate)          (chunking)          (S3 upload)         │
└──────────────────────────────────────────────────────────────────┘
        │                      │                    │
        │                      │ uses               │ uses
        │                      ▼                    ▼
        │               ┌─────────────┐      ┌─────────────┐
        │               │    CDC      │      │     S3      │
        │               │ Integration │      │   Manager   │
        │               └─────────────┘      └─────────────┘
        │
        ▼
┌──────────────────────────────────────────────────────────────────┐
│                      DATA FLOW                                   │
│                                                                  │
│   CLI → Sequential → RowBased → Base → S3Manager → S3 Bucket     │
│              │            │                                      │
│              │            └──► CDCIntegration (get query)        │
│              │                                                   │
│              └──► Loop through tables one by one                 │
└──────────────────────────────────────────────────────────────────┘
```

**extends** = inheritance ("is a") - child class inherits parent's methods
**uses** = composition ("has a") - class contains instance of another class

**Connection Summary:**

| From | To | Type | Purpose |
|------|-----|------|---------|
| `Sequential` | `RowBased` | extends | Inherits chunking logic |
| `RowBased` | `Base` | extends | Inherits S3/connection handling |
| `RowBased` | `CDCIntegration` | uses | Gets query strategy per table |
| `Base` | `S3Manager` | uses | Uploads Parquet files |

**Data Flow Example:**

```python
# 1. CLI invokes SequentialBackupStrategy
strategy = SequentialBackupStrategy(config, pipeline_config)
strategy.execute(tables=["settlement.settle_orders"])

# 2. Sequential loops through tables, calls RowBased for each
for table in tables:
    self._process_single_table_row_based(db_conn, table, ...)

# 3. RowBased uses CDC integration to determine query strategy
cdc_strategy = self.cdc_integration.get_cdc_strategy(table_name)
query = cdc_strategy.build_query(watermark_data)

# 4. RowBased chunks data, Base handles S3 upload
while has_more_data:
    chunk_data = self._get_next_chunk(cursor, ...)  # RowBased
    self._process_batch_with_retries(chunk_data)     # Base → S3
```

**Running Multiple Syncs:**

**Single Pipeline, Multiple Tables** (Sequential within pipeline)
```bash
# Syncs all tables in pipeline one by one
python -m src.cli.main sync pipeline -p us_dw_pipeline

# Flow:
# Table 1: settle_orders   ✓ Done
# Table 2: settle_payments ✓ Done
# Table 3: settle_refunds  ✓ Done
```

**Multiple Pipelines in Parallel** (Safe - isolated sources/paths)
```bash
# Run different pipelines simultaneously
python -m src.cli.main sync pipeline -p us_dw_pipeline &
python -m src.cli.main sync pipeline -p ca_dw_pipeline &
python -m src.cli.main sync pipeline -p kuaisong_pipeline &
wait
```

**Single Table Sync** (Quick test or targeted sync)
```bash
# Sync specific table only
python -m src.cli.main sync pipeline -p us_dw_pipeline -t settlement.settle_orders
```

### 2.3 Configuration System

The system uses two main configuration files:

```
config/
├── connections.yml      # WHERE to connect (sources + targets)
└── pipelines/           # WHAT to sync and HOW
    ├── us_dw_pipeline.yml
    ├── ca_dw_pipeline.yml
    └── ... (19 pipelines)
```

**connections.yml - Connection Registry:**

Defines all database connections (sources and targets):

```yaml
connections:
  # ===== SOURCE CONNECTIONS (MySQL) =====
  sources:
    # SSH Tunnel Connection (secure, for remote access)
    US_DW_RO_SSH:
      host: us-west-2.ro.db.analysis.uniuni.com.internal
      port: 3306
      database: settlement
      username: "${DB_USER}"           # From .env
      password: "${DB_US_DW_RO_PASSWORD}"
      ssh_tunnel:
        enabled: true
        host: 35.83.114.196            # Bastion host
        username: "${SSH_BASTION_USER}"
        private_key_path: "${SSH_BASTION_KEY_PATH}"
        local_port: 0                  # Auto-assign

    # Direct Connection (faster, for internal network)
    US_DW_RO_DIRECT:
      host: us-west-2.ro.db.analysis.uniuni.com.internal
      port: 3306
      database: settlement
      ssh_tunnel:
        enabled: false                 # No tunnel needed

  # ===== TARGET CONNECTIONS (Redshift) =====
  targets:
    redshift_default:
      host: redshift-dw.qa.uniuni.com
      port: 5439
      database: dw
      schema: public
      ssh_tunnel:
        enabled: true
        host: 35.82.216.244

# ===== S3 CONFIGURATIONS =====
s3_configs:
  s3_qa:
    bucket_name: redshift-dw-qa-uniuni-com
    region: us-west-2
    incremental_path: incremental/
```

**Available Connections:**

| Type | SSH Tunnel | Direct | Purpose |
|------|------------|--------|---------|
| US_DW | `US_DW_RO_SSH` | `US_DW_RO_DIRECT` | Settlement data |
| US_PROD | `US_PROD_RO_SSH` | `US_PROD_RO_DIRECT` | Production kuaisong |
| CA_DW | `CA_DW_RO_SSH` | - | Canada data warehouse |
| US_QA | `US_QA_SSH` | `US_QA_RO_DIRECT` | QA environment |
| Redshift | `redshift_default` | `redshift_default_direct` | Target DW |

**Pipeline YAML - Sync Configuration:**

Defines what tables to sync and how:

```yaml
# config/pipelines/us_dw_hybrid_v1_2.yml
pipeline:
  name: "us_dw_hybrid_cdc_v1.2"
  source: "US_DW_RO_SSH"           # Reference to connections.yml
  target: "redshift_default"       # Reference to connections.yml
  version: "1.2.0"

  # Processing settings
  processing:
    strategy: "sequential"         # One table at a time
    batch_size: 50000              # Rows per chunk
    timeout_minutes: 240           # 4 hour max

  # S3 staging settings
  s3:
    isolation_prefix: "us_dw_hybrid/"
    partition_strategy: "datetime"
    compression: "snappy"

# Table-specific configurations
tables:
  settlement.settle_orders:
    cdc_strategy: "hybrid"              # Timestamp + ID
    cdc_timestamp_column: "created_at"
    cdc_id_column: "id"
    target_name: "settle_orders"        # Redshift table name

    processing:
      batch_size: 100000                # Override pipeline default

    validation:
      enable_data_quality_checks: true
      max_null_percentage: 2.0
```

**Pipeline Configuration Hierarchy:**

```
┌─────────────────────────────────────────────────────────────┐
│  Pipeline Level (defaults)                                  │
│  ├── processing.batch_size: 50000                          │
│  ├── processing.strategy: "sequential"                     │
│  └── s3.compression: "snappy"                              │
├─────────────────────────────────────────────────────────────┤
│  Table Level (overrides)                                    │
│  └── settlement.settle_orders:                             │
│      └── processing.batch_size: 100000  ← Overrides 50000  │
└─────────────────────────────────────────────────────────────┘
```

**How Configuration Flows:**

```
┌──────────────┐    ┌──────────────┐    ┌──────────────┐
│ .env         │    │connections.yml│   │pipeline.yml  │
│              │    │              │    │              │
│ DB_USER=xxx  │───►│ US_DW_RO_SSH │───►│ source:      │
│ DB_PASS=xxx  │    │   username:  │    │  US_DW_RO_SSH│
│              │    │   ${DB_USER} │    │              │
└──────────────┘    └──────────────┘    └──────────────┘
     Secrets           Connections         Sync Logic
```

### 2.4 Key Components

#### Watermark System (KISS Design)

**What is a Watermark?**
A JSON file stored in S3 that tracks the state and progress of each table's sync operation.

**Why Watermarks Matter:**
- Enable **incremental syncs** (only process new/changed data)
- **Resume from failure** (continue where you left off)
- **Prevent duplicates** (track which files have been loaded)
- **Audit trail** (how much data transferred, when)

**Watermark Structure:**

```json
{
  "table_name": "settlement.settle_orders",
  "last_mysql_data_timestamp": "2025-11-27T15:30:00Z",
  "last_processed_id": 1000000,
  "mysql_state": {
    "status": "success",
    "total_rows": 5000000,
    "last_session_rows": 50000
  },
  "s3_state": {
    "backup_s3_files": [],
    "processed_s3_files": ["chunk_001.parquet", "chunk_002.parquet"]
  },
  "redshift_state": {
    "status": "success",
    "rows_loaded": 5000000
  }
}
```

**Key Fields Explained:**

| Field | Purpose | Example |
|-------|---------|---------|
| `last_timestamp` | Starting point for next sync | `2025-11-27 15:30:00` |
| `last_id` | Handle records with same timestamp | `1000000` |
| `total_rows` | Lifetime cumulative rows extracted | `5,000,000` |
| `last_session_rows` | Rows in current/last session only | `50,000` |
| `backup_s3_files` | Files awaiting Redshift load | `[file1.parquet]` |
| `processed_s3_files` | Files already loaded to Redshift | `[file2.parquet]` |

**Data Flow Through Watermark:**

```
┌─────────────────────────────────────────────────────────────────┐
│                    WATERMARK LIFECYCLE                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  1. BEFORE SYNC: Read watermark                                 │
│     └── Get last_timestamp, last_id → Build WHERE clause        │
│                                                                 │
│  2. DURING SYNC (MySQL → S3):                                   │
│     ├── Update last_timestamp, last_id after each chunk         │
│     ├── Add files to backup_s3_files[]                          │
│     └── Increment total_rows, last_session_rows                 │
│                                                                 │
│  3. DURING LOAD (S3 → Redshift):                                │
│     ├── Load files from backup_s3_files[]                       │
│     ├── Move loaded files → processed_s3_files[]                │
│     └── Update redshift_state.rows_loaded                       │
│                                                                 │
│  4. AFTER SYNC: Save watermark to S3                            │
│     └── Ready for next incremental sync                         │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

**Common Scenarios:**

| Scenario | Watermark State | Action |
|----------|-----------------|--------|
| Successful sync | `mysql_status: success`, `backup_s3_files: []` | Ready for next sync |
| Backup done, load pending | `backup_s3_files: [5 files]` | Run `--redshift-only` |
| Interrupted backup | `mysql_status: in_progress` | Resume sync (continues from watermark) |
| Row count mismatch | `total_rows ≠ rows_loaded` | Investigate, possibly reset |

**Watermark Commands:**

```bash
# View watermark
python -m src.cli.main watermark get -t <table> -p <pipeline>

# View with file details
python -m src.cli.main watermark get -t <table> -p <pipeline> --show-files

# Reset watermark (triggers full resync)
python -m src.cli.main watermark reset -t <table> -p <pipeline>

# Load pending files only
python -m src.cli.main sync pipeline -p <pipeline> -t <table> --redshift-only
```

**Storage Location:**
```
s3://bucket/watermarks/v2/
├── settlement.settle_orders.json
├── settlement.settle_payments.json
└── ...
```

#### CDC Strategy Engine
Five flexible change detection strategies:

| Strategy | Use Case | Example |
|----------|----------|---------|
| `timestamp_only` | Simple audit tables | `WHERE updated_at > watermark` |
| `hybrid` | Most production tables | Timestamp + ID ordering |
| `id_only` | Sequential inserts | `WHERE id > last_id` |
| `full_sync` | Dimension tables | Replace/Append modes |
| `custom_sql` | Complex logic | User-defined queries |

#### Flexible Schema Manager

**Purpose:** Automatically discovers and converts MySQL schemas to PyArrow/Redshift formats.

**Key Features:**
- **Auto Schema Discovery**: Reads MySQL `INFORMATION_SCHEMA` to get column types
- **Dynamic Type Mapping**: 40+ MySQL → PyArrow type conversions
- **Redshift DDL Generation**: Auto-generates CREATE TABLE statements
- **Schema Caching**: 1-hour TTL cache to reduce database queries (see below)
- **Column Name Sanitization**: Handles Redshift reserved words and case sensitivity

**Type Mapping Examples:**

| MySQL Type | PyArrow Type | Redshift Type |
|------------|--------------|---------------|
| `INT` | `int32` | `INTEGER` |
| `BIGINT` | `int64` | `BIGINT` |
| `VARCHAR(255)` | `string` | `VARCHAR(255)` |
| `DATETIME` | `timestamp[us]` | `TIMESTAMP` |
| `DECIMAL(10,2)` | `decimal128` | `DECIMAL(10,2)` |
| `TEXT` | `string` | `VARCHAR(65535)` |

**Why PyArrow Schema?**

PyArrow is the **bridge** that preserves data types between MySQL and Redshift:

```
Without PyArrow (CSV):  MySQL DECIMAL(10,2) → "99.99" string → 99.989999 (precision lost!)
With PyArrow (Parquet): MySQL DECIMAL(10,2) → decimal128 → DECIMAL(10,2) ✓ (exact match)
```

| Benefit | Explanation |
|---------|-------------|
| Type Preservation | INT stays INT, DATETIME stays TIMESTAMP |
| Native Parquet | Writes Parquet directly (no pandas overhead) |
| Compression | 60-80% smaller than CSV |
| Redshift Compatible | Parquet files work with Redshift COPY |

**How It Works:**

```
MySQL Table                    PyArrow Schema               Redshift Table
┌─────────────────┐           ┌─────────────────┐          ┌─────────────────┐
│ id INT          │           │ id: int32       │          │ id INTEGER      │
│ name VARCHAR    │  ──────►  │ name: string    │  ──────► │ name VARCHAR    │
│ created DATETIME│           │ created: ts[us] │          │ created TIMESTAMP│
└─────────────────┘           └─────────────────┘          └─────────────────┘
     DESCRIBE                   Schema Discovery              DDL Generation
```

**Schema Caching (TTL = Time To Live):**

```
┌─────────────────────────────────────────────────────────────────┐
│                    SCHEMA CACHE LIFECYCLE                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  FIRST REQUEST (cache empty):                                   │
│    1. Check cache → Miss                                        │
│    2. Query MySQL INFORMATION_SCHEMA (~100-500ms)               │
│    3. Store schema in cache with timestamp                      │
│    4. Return schema                                             │
│                                                                 │
│  SUBSEQUENT REQUESTS (within 1 hour):                           │
│    1. Check cache → Hit!                                        │
│    2. Return cached schema instantly (no DB query)              │
│                                                                 │
│  AFTER 1 HOUR (TTL expired):                                    │
│    1. Check cache → Expired                                     │
│    2. Query MySQL again (refresh cache)                         │
│    3. Return updated schema                                     │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘

Benefit: 100 syncs = 1 schema query (instead of 100 queries)
```

#### S3 Manager

**Purpose:** Handles all S3 operations - Parquet uploads, partitioning, and file management.

**Why Parquet (not CSV)?**

| Aspect | CSV | Parquet |
|--------|-----|---------|
| **Size** | 100 MB | 20-40 MB (60-80% smaller) |
| **Type Safety** | Lost (all strings) | Preserved (INT, DECIMAL, TIMESTAMP) |
| **Read Speed** | Scan entire file | Columnar (read only needed columns) |
| **Redshift COPY** | Works but slower | Native support, faster |
| **Schema** | No schema | Embedded schema |

```
CSV:     "99.99" → might become 99.989999 (precision lost)
Parquet: DECIMAL(10,2) → stays 99.99 exactly ✓
```

**Key Features:**
- **Parquet Conversion**: DataFrame → PyArrow Table → Parquet file
- **Metadata Cleaning**: Removes pandas/pyarrow metadata (prevents Redshift errors)
- **Compression**: Snappy (default), gzip, lz4, brotli
- **Multipart Upload**: 50MB chunks, 10 concurrent threads
- **Partition Strategies**: datetime, table, or hybrid

**Partition Strategies:**

```
datetime:
s3://bucket/incremental/year=2025/month=11/day=28/hour=14/
    └── settlement_settle_orders_20251128_140000_batch_0001.parquet

table:
s3://bucket/incremental/table=settlement_settle_orders/year=2025/month=11/day=28/
    └── settlement_settle_orders_20251128_140000_batch_0001.parquet

hybrid:
s3://bucket/incremental/year=2025/month=11/table=settlement_settle_orders/day=28/hour=14/
    └── settlement_settle_orders_20251128_140000_batch_0001.parquet
```

**Upload Flow:**

```
DataFrame (pandas)
      │
      ▼
┌─────────────────────────────────────┐
│  1. Convert to PyArrow Table        │
│  2. Clean metadata (critical!)      │
│  3. Write to Parquet buffer         │
│  4. Compress (snappy)               │
│  5. Multipart upload to S3          │
└─────────────────────────────────────┘
      │
      ▼
S3 Parquet File
```

**Why Metadata Cleaning Matters:**
```
Without cleaning:
  Parquet file contains pandas index, pyarrow version metadata
  → Redshift COPY fails: "incompatible Parquet schema"

With cleaning:
  Parquet file has clean schema, no extra metadata
  → Redshift COPY succeeds ✓
```

#### Gemini Redshift Loader (Current)

**Purpose:** Loads S3 Parquet files directly into Redshift using native COPY PARQUET.

**Why "Gemini"?** This is the newer, optimized loader that replaces the old CSV-based approach.

**Key Features:**
- **Direct Parquet COPY**: No CSV conversion needed (faster, type-safe)
- **Dynamic Schema Discovery**: Uses FlexibleSchemaManager for auto table creation
- **Auto Table Creation**: Creates Redshift table if not exists
- **CDC Support**: Handles `full_sync` replace mode (truncate before load)
- **SSH Tunnel Support**: Connects through bastion host when needed
- **Watermark Integration**: Updates `processed_s3_files` after successful load

**Old vs New Approach:**

| Aspect | Old (RedshiftLoader) | New (GeminiRedshiftLoader) |
|--------|---------------------|---------------------------|
| Method | Parquet → CSV → COPY CSV | Parquet → COPY PARQUET directly |
| Speed | Slower (conversion overhead) | Faster (no conversion) |
| Type Safety | May lose precision | Types preserved |
| Table Creation | Manual | Automatic |
| Status | ❌ Legacy | ✅ **Production** |

**Load Flow:**

```
┌─────────────────────────────────────────────────────────────────┐
│                 GEMINI REDSHIFT LOAD PROCESS                    │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  1. SCHEMA DISCOVERY                                            │
│     └── FlexibleSchemaManager.get_table_schema(table)           │
│                                                                 │
│  2. ENSURE TABLE EXISTS                                         │
│     └── CREATE TABLE IF NOT EXISTS (auto DDL generation)        │
│                                                                 │
│  3. GET FILES FROM WATERMARK                                    │
│     └── backup_s3_files[] (files awaiting load)                 │
│                                                                 │
│  4. FOR EACH PARQUET FILE:                                      │
│     ├── COPY FROM 's3://...' FORMAT AS PARQUET (direct!)        │
│     └── Move file: backup_s3_files → processed_s3_files         │
│                                                                 │
│  5. UPDATE WATERMARK                                            │
│     └── redshift_state.rows_loaded += loaded_rows               │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

**Connection Methods:**

```
Direct Connection (internal network):
  App ──────────────────────────► Redshift:5439

SSH Tunnel (external/secure):
  App ──► SSH Tunnel ──► Bastion ──► Redshift:5439
          localhost:random_port
```

**COPY Command (Direct Parquet):**
```sql
COPY target_table
FROM 's3://bucket/path/file.parquet'
IAM_ROLE 'arn:aws:iam::ACCOUNT:role/redshift-role'
FORMAT AS PARQUET;
```

**No more CSV conversion = Faster + Type-safe!**

---

## 3. Performance Optimizations (10 min)

### 3.1 Sparse Sequence Detection

**Problem**: Tables with gaps in ID sequences waste queries

```
ID Sequence: 1, 2, 3, ... 1000, [GAP], 50000, 50001, ...
Traditional: Query every chunk (slow)
Optimized: Detect gaps, skip empty ranges
```

**Result**: 96% query reduction (26 queries → 1 query)

### 3.2 Early Termination

```python
# Monitor chunk efficiency
efficiency = rows_found / chunk_size

if efficiency < 0.01:  # Less than 1%
    logger.info("Sparse data detected, terminating early")
    break
```

**Real Impact**: 9+ minutes → 40 seconds

### 3.3 Watermark Ceiling Protection

**Problem**: Continuous data injection = infinite sync

**Solution**: Capture max ID at sync start, never exceed it

```python
ceiling_id = SELECT MAX(id) FROM table  # At sync start
# Only process: id <= ceiling_id
```

### 3.4 S3 Upload Optimization

- **Multipart uploads**: 50MB chunks, 10 concurrent threads
- **Compression**: Snappy (default) for speed
- **Timeout protection**: 30-minute operations
- **Clean metadata**: Remove pandas/pyarrow contamination

---

## 4. CLI Module (5 min)

### Module Structure

```
src/cli/
├── main.py                    # Entry point, command groups
├── airflow_integration.py     # SyncExecutionTracker, JSON output
├── cli_integration.py         # v1.1.0 multi-schema integration
├── multi_schema_commands.py   # Pipeline/connection commands
├── column_mapping_commands.py # Column mapping management
└── completion_marker_utils.py # S3 completion markers
```

### Main Commands

| Command | Purpose |
|---------|---------|
| `sync` | Full MySQL → S3 → Redshift pipeline |
| `backup` | MySQL → S3 only |
| `watermark` | View/reset sync progress |
| `s3clean` | Manage S3 backup files |
| `status` | Show system status |
| `config` | View configuration |
| `connections` | Test database connections |

### Command Examples

```bash
# Pipeline sync (recommended)
python -m src.cli.main sync pipeline -p us_dw_pipeline -t settlement.settle_orders

# With options
python -m src.cli.main sync pipeline -p pipeline -t table \
    --json-output result.json \
    --backup-only \
    --limit 10000

# Watermark operations
python -m src.cli.main watermark get -t table -p pipeline
python -m src.cli.main watermark reset -t table -p pipeline --yes

# S3 cleanup
python -m src.cli.main s3clean list -t table -p pipeline
python -m src.cli.main s3clean clean -t table -p pipeline --force

# Connection test
python -m src.cli.main connections test -c US_DW_RO_SSH
```

### JSON Output (CI/CD Integration)

```bash
python -m src.cli.main sync pipeline -p pipeline --json-output
```

```json
{
  "execution_id": "sync_20251128_140000_abc123",
  "status": "success",
  "pipeline": "us_dw_pipeline",
  "tables_requested": ["settlement.settle_orders"],
  "table_results": {
    "settlement.settle_orders": {
      "status": "success",
      "rows_processed": 50000,
      "files_created": 5,
      "duration_seconds": 45
    }
  }
}
```

### Global Options

```bash
python -m src.cli.main --help

Options:
  --debug         Enable debug logging
  --quiet, -q     Only show errors/warnings
  --config-file   Custom config path
  --log-file      Log to file
  --json-logs     JSON format logs
```

## 5. Hourly Parcel Sync ETL

A specialized hourly pipeline for parcel detail data using temp table pattern.

### Pipeline Flow

```
┌─────────────────────────────────────────────────────────────────────┐
│  1. DOWNLOAD (Pentaho ETL)                                          │
│     External source → MySQL temp table                              │
│     download_tool.sh → kitchen.sh → download_tool_job.kjb           │
└─────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│  2. SYNC (S3-Redshift Backup Tool)                                  │
│     MySQL temp → S3 parquet → Redshift                              │
│     python -m src.cli.main sync pipeline -p ... -t ... --json-output│
└─────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│  3. VALIDATE                                                        │
│     MySQL COUNT(*) == rows_processed == Redshift COUNT(*)           │
└─────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│  4. CLEANUP (on success)                                            │
│     DELETE FROM temp_table → s3clean → watermark reset              │
└─────────────────────────────────────────────────────────────────────┘
```

### Key Commands

```bash
# Hourly cron job
./parcel_download_hourly_run.sh

# Manual full pipeline (download + sync)
python parcel_download_and_sync.py -d "" --hours "-1"

# Sync only (skip download)
python parcel_download_and_sync.py --sync-only
```

### Why Temp Table Pattern?

| Aspect | Benefit |
|--------|---------|
| Clean slate | Empty temp table → no duplicate data |
| Validation | Can compare exact counts before/after |
| Safe cleanup | Delete temp data after confirmed sync |
| Atomic operation | Either all data syncs or none |

---

## Quick Reference

### File Locations

| Item | Path |
|------|------|
| Pipeline configs | `config/pipelines/*.yaml` |
| Column mappings | `column_mappings/*.json` |
| Watermarks (S3) | `s3://bucket/watermarks/v2/` |
| CLI entry point | `src/cli/main.py` |

### Key Commands

```bash
# List pipelines
python -m src.cli.main config list-pipelines

# Test connection
python -m src.cli.main connections test -c US_DW_RO_SSH

# View watermark
python -m src.cli.main watermark get -t table_name

# Full sync
python -m src.cli.main sync pipeline -p pipeline_name
```

---

## Resources

- **Main Docs**: `CLAUDE.md`
- **CLI Reference**: `CLI_QUICK_REFERENCE.md`
- **Deployment Guide**: `PRODUCTION_DEPLOYMENT_GUIDE.md`
- **Performance Guide**: `PERFORMANCE_OPTIMIZATION_GUIDE.md`

---

## Summary

### What We Built

A production-grade, incremental data pipeline featuring:

- **5 CDC strategies** for flexible change capture
- **Multi-schema support** with 20+ connections
- **Performance optimizations** delivering 87-96% improvements
- **Enterprise security** with 6-layer protection
- **CI/CD integration** with JSON output

### Key Takeaways

1. **KISS Principle**: Simple watermark design > complex state machine
2. **Verify Everything**: Logs lie, row counts don't
3. **Optimize Smart**: Sparse detection > brute force
4. **Plan for Failure**: Resume-from-watermark saves hours

---

*Questions? Feedback? Let's discuss!*
