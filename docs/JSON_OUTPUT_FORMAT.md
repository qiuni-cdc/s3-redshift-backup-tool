# JSON Output Format Documentation

## Overview

The S3-Redshift backup system provides unified JSON output format for automation and integration purposes. Use the `--json-output` flag with sync commands to get structured, machine-readable results with per-table tracking and workflow orchestration support.

## Usage

```bash
# Enable JSON output for any sync command
python -m src.cli.main sync -t table_name --json-output

# Example: Sync with pipeline specification
python -m src.cli.main sync -p pipeline_name -t table_name --json-output

# Example: Multiple tables
python -m src.cli.main sync -p pipeline -t table1 -t table2 --json-output
```

## JSON Output Structure

### Basic Structure

```json
{
  "execution_id": string,           // Unique execution identifier
  "start_time": string,             // ISO 8601 timestamp
  "pipeline": string,               // Pipeline name
  "tables_requested": array,        // List of tables to sync
  "table_results": object,          // Per-table results
  "status": string,                 // Overall status
  "end_time": string,              // ISO 8601 timestamp
  "duration_seconds": float,        // Total execution time
  "summary": object                 // Aggregate summary
}
```

### Complete Example

```json
{
  "execution_id": "sync_20250926_195438_2f01922d",
  "start_time": "2025-09-26T19:54:38.135401+00:00",
  "pipeline": "us_dw_unidw_2_public_pipeline",
  "tables_requested": [
    "unidw.dw_parcel_detail_tool_temp",
    "unidw.dw_orders"
  ],
  "total_tables": 2,
  "table_results": {
    "unidw.dw_parcel_detail_tool_temp": {
      "status": "success",
      "rows_processed": 164260,
      "files_created": 4,
      "duration_seconds": 30.0,
      "completed_at": "2025-09-26T19:56:37.466043+00:00"
    },
    "unidw.dw_orders": {
      "status": "success",
      "rows_processed": 50000,
      "files_created": 2,
      "duration_seconds": 15.5,
      "completed_at": "2025-09-26T19:56:52.123456+00:00"
    }
  },
  "status": "success",
  "end_time": "2025-09-26T19:56:52.466569+00:00",
  "duration_seconds": 134.331168,
  "summary": {
    "success_count": 2,
    "failure_count": 0,
    "total_rows_processed": 214260,
    "total_files_created": 6
  }
}
```

### Error Response Example

```json
{
  "execution_id": "sync_20250926_200512_a3f8e1b2",
  "start_time": "2025-09-26T20:05:12.234567+00:00",
  "pipeline": "us_dw_pipeline",
  "tables_requested": [
    "settlement.orders",
    "settlement.payments"
  ],
  "total_tables": 2,
  "table_results": {
    "settlement.orders": {
      "status": "failed",
      "error_message": "Connection timeout to MySQL database",
      "failed_at": "2025-09-26T20:05:45.123456+00:00"
    },
    "settlement.payments": {
      "status": "success",
      "rows_processed": 12000,
      "files_created": 2,
      "duration_seconds": 18.5,
      "completed_at": "2025-09-26T20:06:03.456789+00:00"
    }
  },
  "status": "partial_success",
  "end_time": "2025-09-26T20:06:03.567890+00:00",
  "duration_seconds": 51.333323,
  "summary": {
    "success_count": 1,
    "failure_count": 1,
    "total_rows_processed": 12000,
    "total_files_created": 2
  }
}
```

## Field Definitions

### Root Level Fields

| Field | Type | Description |
|-------|------|-------------|
| `execution_id` | string | Unique execution identifier (format: `sync_YYYYMMDD_HHMMSS_<uuid>`) |
| `start_time` | string | ISO 8601 timestamp when execution started |
| `pipeline` | string | Pipeline name (or "default" if not specified) |
| `tables_requested` | array | List of table names to sync |
| `total_tables` | integer | Number of tables in this execution |
| `table_results` | object | Per-table execution results (see below) |
| `status` | string | Overall status: "success", "partial_success", or "failed" |
| `end_time` | string | ISO 8601 timestamp when execution completed |
| `duration_seconds` | float | Total execution time in seconds |
| `summary` | object | Aggregate summary statistics (see below) |

### Table Results Object

Each table in `table_results` contains:

#### Success Case
| Field | Type | Description |
|-------|------|-------------|
| `status` | string | "success" |
| `rows_processed` | integer | Number of rows extracted/synced |
| `files_created` | integer | Number of S3 files created |
| `duration_seconds` | float | Time taken to process this table |
| `completed_at` | string | ISO 8601 timestamp of completion |

#### Failure Case
| Field | Type | Description |
|-------|------|-------------|
| `status` | string | "failed" |
| `error_message` | string | Description of the error |
| `failed_at` | string | ISO 8601 timestamp of failure |

### Summary Object
| Field | Type | Description |
|-------|------|-------------|
| `success_count` | integer | Number of successfully processed tables |
| `failure_count` | integer | Number of failed tables |
| `total_rows_processed` | integer | Sum of all rows across successful tables |
| `total_files_created` | integer | Sum of all files across successful tables |

## Integration Examples

### CI/CD Pipeline Integration

#### Bash/Shell Script
```bash
#!/bin/bash

# Run sync and capture JSON output
sync_result=$(python -m src.cli.main sync -p prod_pipeline -t users --json-output)
exit_code=$?

# Parse results using jq
execution_id=$(echo "$sync_result" | jq -r '.execution_id')
status=$(echo "$sync_result" | jq -r '.status')
duration=$(echo "$sync_result" | jq -r '.duration_seconds')
rows_processed=$(echo "$sync_result" | jq -r '.summary.total_rows_processed')
success_count=$(echo "$sync_result" | jq -r '.summary.success_count')
failure_count=$(echo "$sync_result" | jq -r '.summary.failure_count')

echo "Execution ID: $execution_id"
echo "Status: $status"
echo "Duration: ${duration}s"
echo "Rows processed: $rows_processed"
echo "Tables: $success_count succeeded, $failure_count failed"

# Set pipeline variables for downstream jobs
echo "SYNC_EXECUTION_ID=$execution_id" >> $GITHUB_ENV
echo "SYNC_STATUS=$status" >> $GITHUB_ENV
echo "SYNC_DURATION=$duration" >> $GITHUB_ENV
echo "SYNC_ROWS=$rows_processed" >> $GITHUB_ENV

exit $exit_code
```

#### Python Script
```python
import subprocess
import json
import sys

def run_sync_with_json(pipeline, table):
    """Run sync command and return parsed JSON results"""
    cmd = [
        "python", "-m", "src.cli.main", "sync",
        "-p", pipeline, "-t", table, "--json-output"
    ]

    result = subprocess.run(cmd, capture_output=True, text=True)

    try:
        data = json.loads(result.stdout)
        return data, result.returncode
    except json.JSONDecodeError:
        return {"error": "Invalid JSON output", "stdout": result.stdout}, result.returncode

# Usage
data, exit_code = run_sync_with_json("prod_pipeline", "users")

if data.get("status") == "success":
    summary = data.get("summary", {})
    print(f"✅ Sync successful: {summary.get('success_count')} tables in {data['duration_seconds']:.1f}s")
    print(f"   Rows processed: {summary.get('total_rows_processed'):,}")
    print(f"   Files created: {summary.get('total_files_created')}")
elif data.get("status") == "partial_success":
    print(f"⚠️  Partial success: {data.get('summary', {}).get('success_count')} succeeded, "
          f"{data.get('summary', {}).get('failure_count')} failed")
else:
    # Get first failure message
    for table, result in data.get("table_results", {}).items():
        if result.get("status") == "failed":
            print(f"❌ Sync failed on {table}: {result.get('error_message', 'Unknown error')}")
            break

sys.exit(exit_code)
```

### Monitoring Integration

#### Prometheus Metrics
```python
import json
import subprocess
from prometheus_client import Gauge, Counter, Histogram

# Metrics
sync_duration = Histogram('sync_duration_seconds', 'Sync operation duration')
sync_success = Counter('sync_operations_total', 'Total sync operations', ['status', 'pipeline'])
sync_rows = Gauge('sync_rows_processed', 'Number of rows processed', ['pipeline'])
sync_tables = Gauge('sync_tables_total', 'Number of tables synced', ['pipeline', 'status'])

def collect_sync_metrics(pipeline, table):
    cmd = ["python", "-m", "src.cli.main", "sync",
           "-p", pipeline, "-t", table, "--json-output"]

    result = subprocess.run(cmd, capture_output=True, text=True)
    data = json.loads(result.stdout)

    # Record metrics
    sync_duration.observe(data['duration_seconds'])
    sync_success.labels(status=data['status'], pipeline=pipeline).inc()
    sync_rows.labels(pipeline=pipeline).set(data['summary'].get('total_rows_processed', 0))

    # Per-table metrics
    for table_name, table_result in data.get('table_results', {}).items():
        if table_result.get('status') == 'success':
            sync_tables.labels(pipeline=pipeline, status='success').inc()
        else:
            sync_tables.labels(pipeline=pipeline, status='failed').inc()
```

#### Datadog Integration
```python
from datadog import initialize, statsd
import json
import subprocess

options = {
    'api_key': 'your_api_key',
    'app_key': 'your_app_key'
}

initialize(**options)

def send_sync_metrics(pipeline, table):
    cmd = ["python", "-m", "src.cli.main", "sync",
           "-p", pipeline, "-t", table, "--json-output"]

    result = subprocess.run(cmd, capture_output=True, text=True)
    data = json.loads(result.stdout)

    # Send metrics to Datadog
    tags = [f'pipeline:{pipeline}', f'execution_id:{data["execution_id"]}']

    statsd.histogram('sync.duration', data['duration_seconds'], tags=tags)
    statsd.increment('sync.operations', tags=tags + [f'status:{data["status"]}'])

    # Summary metrics
    summary = data.get('summary', {})
    statsd.gauge('sync.rows_processed', summary.get('total_rows_processed', 0), tags=tags)
    statsd.gauge('sync.tables.success', summary.get('success_count', 0), tags=tags)
    statsd.gauge('sync.tables.failed', summary.get('failure_count', 0), tags=tags)

    # Per-table metrics
    for table_name, table_result in data.get('table_results', {}).items():
        table_tags = tags + [f'table:{table_name}']
        if table_result.get('status') == 'success':
            statsd.gauge(f'sync.table.rows', table_result.get('rows_processed', 0), tags=table_tags)
            statsd.gauge(f'sync.table.duration', table_result.get('duration_seconds', 0), tags=table_tags)
```

## Exit Codes

When using `--json-output`:

- **Exit Code 0**: Operation successful (`"status": "success"`)
- **Exit Code 1**: Operation failed or partial success (`"status": "failed"` or `"partial_success"`)

The JSON output is always printed to stdout regardless of success/failure status.

## Best Practices

1. **Always Check Exit Code**: Don't rely solely on JSON parsing - check the exit code
2. **Handle JSON Parse Errors**: Wrap JSON parsing in try-catch blocks
3. **Validate Expected Fields**: Check for presence of expected fields before accessing
4. **Store Results**: Consider logging/storing JSON results for audit trails with `execution_id`
5. **Monitor Performance**: Use duration and per-table metrics for performance monitoring
6. **Track Executions**: Use `execution_id` to correlate logs across systems
7. **Check Per-Table Results**: Always examine `table_results` for individual table failures

## Troubleshooting

### Common Issues

**Issue**: JSON output mixed with regular logs
**Solution**: The `--json-output` flag ensures only JSON is printed to stdout. All other logs go to stderr or log files.

**Issue**: Cannot parse JSON response
**Solution**: Check stderr for error messages, ensure command completed successfully. Validate JSON with `jq` or similar tool.

**Issue**: Missing per-table metrics
**Solution**: Per-table metrics depend on the backup strategy. Some strategies process tables in batches, so metrics may be averaged across tables.

**Issue**: `execution_id` not unique
**Solution**: The execution_id uses timestamp + UUID, ensuring uniqueness even for parallel runs. If you see duplicates, check your system clock.

### Debug Mode

For debugging, you can combine JSON output with verbose logging:

```bash
# JSON output with debug logs to stderr
python -m src.cli.main sync -p test -t table --json-output --verbose 2>debug.log
```

This sends JSON to stdout and debug info to stderr/file.

## Format Overview

This JSON format provides:
- **Unique Execution IDs**: Track and correlate sync operations across systems
- **Per-Table Results**: Detailed success/failure information for each table
- **Session-Based Metrics**: Clear view of what each sync accomplished (not cumulative)
- **Status Granularity**: `success`, `partial_success`, or `failed` states
- **Workflow Integration**: Designed for CI/CD pipelines and monitoring systems