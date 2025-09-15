# JSON Output Format Documentation

## Overview

The S3-Redshift backup system provides JSON output format for automation and integration purposes. Use the `--json-output` flag with sync commands to get structured, machine-readable results.

## Usage

```bash
# Enable JSON output for any sync command
python -m src.cli.main sync [OPTIONS] --json-output

# Example: Pipeline sync with JSON output
python -m src.cli.main sync pipeline -p pipeline_name -t table_name --json-output

# Example: Connection-based sync with JSON output  
python -m src.cli.main sync connections -s source_conn -r target_conn -t table_name --json-output
```

## JSON Output Structure

### Basic Structure

```json
{
  "success": boolean,               // Overall operation success
  "duration_seconds": float,        // Total execution time
  "tables_processed": integer,      // Number of tables processed
  "strategy": string,               // Backup strategy used
  "stages": {                       // Stage-by-stage results
    "backup": { ... },
    "redshift": { ... }
  },
  "options": { ... }               // Runtime configuration
}
```

### Complete Example

```json
{
  "success": true,
  "duration_seconds": 45.23,
  "tables_processed": 2,
  "strategy": "sequential",
  "stages": {
    "backup": {
      "executed": true,
      "success": true,
      "summary": {
        "tables_processed": 2,
        "total_rows": 15000,
        "total_batches": 3,
        "total_bytes": 2048000,
        "total_mb": 1.95,
        "successful_tables": 2,
        "failed_tables": 0,
        "avg_rows_per_second": 331.8
      }
    },
    "redshift": {
      "executed": true,
      "success": true,
      "summary": {
        "tables_loaded": 2,
        "total_rows_loaded": 15000,
        "files_processed": 3,
        "load_duration_seconds": 12.4
      }
    }
  },
  "options": {
    "backup_only": false,
    "redshift_only": false,
    "chunk_size": 50000,
    "max_total_rows": null,
    "verify_data": false
  }
}
```

### Error Response Example

```json
{
  "success": false,
  "duration_seconds": 12.1,
  "tables_processed": 1,
  "strategy": "sequential",
  "stages": {
    "backup": {
      "executed": true,
      "success": false,
      "summary": {
        "error": "Connection timeout to MySQL database",
        "tables_processed": 0,
        "failed_tables": 1
      }
    },
    "redshift": {
      "executed": false,
      "success": null,
      "summary": null
    }
  },
  "options": {
    "backup_only": false,
    "redshift_only": false,
    "chunk_size": 50000,
    "max_total_rows": 100000,
    "verify_data": true
  }
}
```

## Field Definitions

### Root Level Fields

| Field | Type | Description |
|-------|------|-------------|
| `success` | boolean | True if overall operation succeeded |
| `duration_seconds` | float | Total execution time in seconds |
| `tables_processed` | integer | Number of tables that were processed |
| `strategy` | string | Backup strategy: "sequential", "inter-table", etc. |
| `stages` | object | Stage-by-stage execution results |
| `options` | object | Runtime configuration options |

### Stages Object

#### Backup Stage
| Field | Type | Description |
|-------|------|-------------|
| `executed` | boolean | Whether backup stage was executed |
| `success` | boolean/null | Stage success status (null if not executed) |
| `summary` | object/null | Detailed backup metrics |

#### Redshift Stage  
| Field | Type | Description |
|-------|------|-------------|
| `executed` | boolean | Whether Redshift stage was executed |
| `success` | boolean/null | Stage success status (null if not executed) |
| `summary` | object/null | Detailed loading metrics |

### Options Object
| Field | Type | Description |
|-------|------|-------------|
| `backup_only` | boolean | Whether only backup stage was requested |
| `redshift_only` | boolean | Whether only Redshift stage was requested |
| `chunk_size` | integer | Batch size used for processing |
| `max_total_rows` | integer/null | Maximum rows limit (null if unlimited) |
| `verify_data` | boolean | Whether data verification was enabled |

## Integration Examples

### CI/CD Pipeline Integration

#### Bash/Shell Script
```bash
#!/bin/bash

# Run sync and capture JSON output
sync_result=$(python -m src.cli.main sync pipeline -p prod_pipeline -t users --json-output)
exit_code=$?

# Parse results using jq
success=$(echo "$sync_result" | jq -r '.success')
duration=$(echo "$sync_result" | jq -r '.duration_seconds')
rows_processed=$(echo "$sync_result" | jq -r '.stages.backup.summary.total_rows // 0')

echo "Sync completed: success=$success, duration=${duration}s, rows=$rows_processed"

# Set pipeline variables for downstream jobs
echo "SYNC_SUCCESS=$success" >> $GITHUB_ENV
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
        "python", "-m", "src.cli.main", "sync", "pipeline",
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

if data.get("success"):
    print(f"✅ Sync successful: {data['tables_processed']} tables in {data['duration_seconds']:.1f}s")
else:
    print(f"❌ Sync failed: {data.get('stages', {}).get('backup', {}).get('summary', {}).get('error', 'Unknown error')}")

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
sync_success = Counter('sync_operations_total', 'Total sync operations', ['status'])
sync_rows = Gauge('sync_rows_processed', 'Number of rows processed')

def collect_sync_metrics(pipeline, table):
    cmd = ["python", "-m", "src.cli.main", "sync", "pipeline", 
           "-p", pipeline, "-t", table, "--json-output"]
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    data = json.loads(result.stdout)
    
    # Record metrics
    sync_duration.observe(data['duration_seconds'])
    sync_success.labels(status='success' if data['success'] else 'failure').inc()
    
    if data['stages']['backup']['executed'] and data['stages']['backup']['summary']:
        sync_rows.set(data['stages']['backup']['summary'].get('total_rows', 0))
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
    cmd = ["python", "-m", "src.cli.main", "sync", "pipeline",
           "-p", pipeline, "-t", table, "--json-output"]
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    data = json.loads(result.stdout)
    
    # Send metrics to Datadog
    statsd.histogram('sync.duration', data['duration_seconds'], 
                    tags=[f'pipeline:{pipeline}', f'table:{table}'])
    
    statsd.increment('sync.operations', 
                    tags=[f'status:{"success" if data["success"] else "failure"}'])
    
    if data['stages']['backup']['executed']:
        backup_summary = data['stages']['backup']['summary']
        if backup_summary:
            statsd.gauge('sync.rows_processed', backup_summary.get('total_rows', 0))
            statsd.gauge('sync.throughput', backup_summary.get('avg_rows_per_second', 0))
```

## Exit Codes

When using `--json-output`:

- **Exit Code 0**: Operation successful (`"success": true`)
- **Exit Code 1**: Operation failed (`"success": false`)

The JSON output is always printed to stdout regardless of success/failure status.

## Best Practices

1. **Always Check Exit Code**: Don't rely solely on JSON parsing - check the exit code
2. **Handle JSON Parse Errors**: Wrap JSON parsing in try-catch blocks
3. **Validate Expected Fields**: Check for presence of expected fields before accessing
4. **Store Results**: Consider logging/storing JSON results for audit trails
5. **Monitor Performance**: Use duration and throughput metrics for performance monitoring

## Troubleshooting

### Common Issues

**Issue**: JSON output mixed with regular logs
**Solution**: The `--json-output` flag ensures only JSON is printed to stdout

**Issue**: Cannot parse JSON response  
**Solution**: Check stderr for error messages, ensure command completed successfully

**Issue**: Missing expected fields in JSON
**Solution**: Fields may be null/missing if stages weren't executed or failed early

### Debug Mode

For debugging, you can combine JSON output with verbose logging:

```bash
# JSON output with debug logs to stderr
python -m src.cli.main sync pipeline -p test -t table --json-output --verbose 2>debug.log
```

This sends JSON to stdout and debug info to stderr/file.