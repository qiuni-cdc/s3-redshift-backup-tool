# S3 Completion Markers Documentation

## Overview

S3 completion markers are files created in S3 to indicate successful completion of sync operations. They're primarily used for Airflow integration and workflow orchestration to track pipeline completion status.

## Usage

### Enabling S3 Completion Markers

```bash
# Enable completion markers for pipeline sync
python -m src.cli.main sync pipeline \
  -p pipeline_name \
  -t table_name \
  --json-output /path/to/output.json \
  --s3-completion-bucket your-completion-bucket

# The system will automatically create completion markers in the specified S3 bucket
```

### Configuration Options

| Option | Description | Default |
|--------|-------------|---------|
| `--s3-completion-bucket` | S3 bucket for completion markers | None (disabled) |
| `--json-output` | Path to write JSON execution metadata | None |

## Completion Marker Structure

### File Location Pattern

```
s3://{completion-bucket}/completion_markers/{pipeline_name}/{table_name}/{timestamp}/
```

### Example S3 Paths

```
s3://airflow-completion-markers/completion_markers/us_dw_hybrid_v1_2/settlement.settle_orders/20250912_143022/
├── execution_metadata.json
├── completion_marker.txt
└── table_metrics.json
```

### File Contents

#### 1. execution_metadata.json
Complete JSON output from the sync operation:

```json
{
  "execution_id": "sync_20250912_143022_a3f8e1b2",
  "start_time": "2025-09-12T14:30:22.135401+00:00",
  "pipeline": "us_dw_hybrid_v1_2",
  "tables_requested": ["settlement.settle_orders"],
  "total_tables": 1,
  "table_results": {
    "settlement.settle_orders": {
      "status": "success",
      "rows_processed": 15000,
      "files_created": 3,
      "duration_seconds": 45.2,
      "completed_at": "2025-09-12T14:31:07.466043+00:00"
    }
  },
  "status": "success",
  "end_time": "2025-09-12T14:31:07.567890+00:00",
  "duration_seconds": 45.43,
  "summary": {
    "success_count": 1,
    "failure_count": 0,
    "total_rows_processed": 15000,
    "total_files_created": 3
  }
}
```

#### 2. completion_marker.txt
Simple text marker for basic checks:

```
SYNC_COMPLETED
Pipeline: us_dw_hybrid_v1_2
Table: settlement.settle_orders
Status: SUCCESS
Timestamp: 2025-09-12T14:30:22Z
Duration: 45.23s
Rows: 15000
```

#### 3. table_metrics.json
Detailed table-specific metrics:

```json
{
  "table_name": "settlement.settle_orders", 
  "pipeline": "us_dw_hybrid_v1_2",
  "mysql_metrics": {
    "rows_extracted": 15000,
    "batches_processed": 3,
    "extraction_duration_seconds": 32.8,
    "avg_rows_per_second": 457.3
  },
  "s3_metrics": {
    "files_created": 3,
    "total_size_bytes": 2048000,
    "total_size_mb": 1.95,
    "compression_ratio": 0.73
  },
  "redshift_metrics": {
    "rows_loaded": 15000,
    "files_loaded": 3,
    "load_duration_seconds": 12.4,
    "copy_statements_executed": 3,
    "avg_load_rate_rows_per_second": 1209.7
  }
}
```

## Airflow Integration

### DAG Configuration

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.s3_key_sensor import S3KeySensor

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 9, 12),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'data_sync_pipeline',
    default_args=default_args,
    description='Data sync with completion markers',
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    catchup=False
)

# Sync task with completion markers
sync_task = BashOperator(
    task_id='sync_settlement_data',
    bash_command="""
    python -m src.cli.main sync pipeline \
      -p us_dw_hybrid_v1_2 \
      -t settlement.settle_orders \
      --json-output /tmp/sync_result_{{ ds }}.json \
      --s3-completion-bucket airflow-completion-markers
    """,
    dag=dag
)

# Wait for completion marker
wait_for_completion = S3KeySensor(
    task_id='wait_for_sync_completion',
    bucket_name='airflow-completion-markers',
    bucket_key='completion_markers/us_dw_hybrid_v1_2/settlement.settle_orders/{{ ds_nodash }}_*/completion_marker.txt',
    wildcard_match=True,
    timeout=3600,  # 1 hour timeout
    poke_interval=30,  # Check every 30 seconds
    dag=dag
)

# Downstream processing task
process_data = BashOperator(
    task_id='process_synced_data',
    bash_command="""
    # Read completion marker for metrics
    aws s3 cp s3://airflow-completion-markers/completion_markers/us_dw_hybrid_v1_2/settlement.settle_orders/{{ ds_nodash }}_*/execution_metadata.json /tmp/

    # Extract row count for validation
    ROWS_SYNCED=$(cat /tmp/execution_metadata.json | jq '.summary.total_rows_processed')
    echo "Processing $ROWS_SYNCED rows"
    
    # Continue with downstream processing...
    """,
    dag=dag
)

sync_task >> wait_for_completion >> process_data
```

### Multi-Table Pipeline

```python
from airflow.models import Variable

# Get list of tables to sync
TABLES_TO_SYNC = Variable.get("sync_tables", deserialize_json=True, default_var=[
    "settlement.settle_orders",
    "settlement.claim_details", 
    "unidw.dw_parcel_detail_tool"
])

for table in TABLES_TO_SYNC:
    # Create sync task for each table
    sync_task = BashOperator(
        task_id=f'sync_{table.replace(".", "_")}',
        bash_command=f"""
        python -m src.cli.main sync pipeline \
          -p us_dw_hybrid_v1_2 \
          -t {table} \
          --json-output /tmp/sync_{table.replace(".", "_")}_{{{{ ds }}}}.json \
          --s3-completion-bucket airflow-completion-markers
        """,
        dag=dag
    )
    
    # Create sensor for each table
    wait_task = S3KeySensor(
        task_id=f'wait_{table.replace(".", "_")}',
        bucket_name='airflow-completion-markers',
        bucket_key=f'completion_markers/us_dw_hybrid_v1_2/{table}/{{{{ ds_nodash }}}}_*/completion_marker.txt',
        wildcard_match=True,
        timeout=3600,
        dag=dag
    )
    
    sync_task >> wait_task
```

## Monitoring and Alerting

### CloudWatch Metrics

```python
import boto3
import json
from datetime import datetime

cloudwatch = boto3.client('cloudwatch')

def publish_sync_metrics(completion_marker_s3_path):
    """Read completion marker and publish CloudWatch metrics"""
    s3 = boto3.client('s3')
    
    # Download execution metadata
    bucket, key = completion_marker_s3_path.replace('s3://', '').split('/', 1)
    response = s3.get_object(Bucket=bucket, Key=f"{key}/execution_metadata.json")
    metadata = json.loads(response['Body'].read())
    
    # Extract metrics
    pipeline = metadata.get('pipeline', 'unknown')
    table = metadata.get('tables_requested', [])[0] if metadata.get('tables_requested') else 'unknown'
    status = metadata.get('status', 'unknown')
    success = (status == 'success')
    duration = metadata.get('duration_seconds', 0)

    summary = metadata.get('summary', {})
    rows_processed = summary.get('total_rows_processed', 0)
    
    # Publish metrics
    cloudwatch.put_metric_data(
        Namespace='DataSync',
        MetricData=[
            {
                'MetricName': 'SyncDuration',
                'Value': duration,
                'Unit': 'Seconds',
                'Dimensions': [
                    {'Name': 'Pipeline', 'Value': pipeline},
                    {'Name': 'Table', 'Value': table}
                ]
            },
            {
                'MetricName': 'SyncSuccess',
                'Value': 1 if success else 0,
                'Unit': 'Count',
                'Dimensions': [
                    {'Name': 'Pipeline', 'Value': pipeline},
                    {'Name': 'Table', 'Value': table}
                ]
            },
            {
                'MetricName': 'RowsProcessed',
                'Value': rows_processed,
                'Unit': 'Count',
                'Dimensions': [
                    {'Name': 'Pipeline', 'Value': pipeline},
                    {'Name': 'Table', 'Value': table}
                ]
            }
        ]
    )
```

### Slack Notifications

```python
import requests
import json

def send_sync_notification(completion_marker_s3_path, webhook_url):
    """Send Slack notification based on completion marker"""
    s3 = boto3.client('s3')
    
    # Download execution metadata
    bucket, key = completion_marker_s3_path.replace('s3://', '').split('/', 1)
    response = s3.get_object(Bucket=bucket, Key=f"{key}/execution_metadata.json")
    metadata = json.loads(response['Body'].read())
    
    # Format message
    pipeline = metadata.get('pipeline', 'Unknown')
    table = metadata.get('tables_requested', [])[0] if metadata.get('tables_requested') else 'Unknown'
    status = metadata.get('status', 'unknown')
    success = (status == 'success')
    duration = metadata.get('duration_seconds', 0)

    if success:
        summary = metadata.get('summary', {})
        rows = summary.get('total_rows_processed', 0)
        files = summary.get('total_files_created', 0)

        message = {
            "text": f"✅ Sync Completed Successfully",
            "attachments": [{
                "color": "good",
                "fields": [
                    {"title": "Pipeline", "value": pipeline, "short": True},
                    {"title": "Table", "value": table, "short": True},
                    {"title": "Duration", "value": f"{duration:.1f}s", "short": True},
                    {"title": "Rows", "value": f"{rows:,}", "short": True},
                    {"title": "Files", "value": f"{files}", "short": True}
                ]
            }]
        }
    else:
        message = {
            "text": f"❌ Sync Failed",
            "attachments": [{
                "color": "danger", 
                "fields": [
                    {"title": "Pipeline", "value": pipeline, "short": True},
                    {"title": "Table", "value": table, "short": True},
                    {"title": "Duration", "value": f"{duration:.1f}s", "short": True}
                ]
            }]
        }
    
    requests.post(webhook_url, json=message)
```

## Cleanup and Maintenance

### Automatic Cleanup

```bash
#!/bin/bash
# cleanup_completion_markers.sh

BUCKET="airflow-completion-markers"
RETENTION_DAYS=30

# Delete completion markers older than retention period
aws s3 ls s3://$BUCKET/completion_markers/ --recursive | \
while read -r line; do
    create_date=$(echo $line | awk '{print $1" "$2}')
    create_date_seconds=$(date -d "$create_date" +%s)
    cutoff_date_seconds=$(date -d "$RETENTION_DAYS days ago" +%s)
    
    if [[ $create_date_seconds -lt $cutoff_date_seconds ]]; then
        file_path=$(echo $line | awk '{print $4}')
        echo "Deleting old completion marker: $file_path"
        aws s3 rm s3://$BUCKET/$file_path
    fi
done
```

### S3 Lifecycle Policy

```json
{
    "Rules": [
        {
            "ID": "completion-markers-lifecycle",
            "Status": "Enabled",
            "Filter": {
                "Prefix": "completion_markers/"
            },
            "Transitions": [
                {
                    "Days": 30,
                    "StorageClass": "STANDARD_IA"
                },
                {
                    "Days": 90,
                    "StorageClass": "GLACIER"
                }
            ],
            "Expiration": {
                "Days": 365
            }
        }
    ]
}
```

## Security Considerations

### IAM Permissions

Minimum required permissions for completion markers:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow", 
            "Action": [
                "s3:PutObject",
                "s3:PutObjectAcl"
            ],
            "Resource": "arn:aws:s3:::your-completion-bucket/completion_markers/*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:ListBucket"
            ],
            "Resource": "arn:aws:s3:::your-completion-bucket",
            "Condition": {
                "StringLike": {
                    "s3:prefix": "completion_markers/*"
                }
            }
        }
    ]
}
```

### Data Privacy

- Completion markers may contain table names and row counts
- Consider encryption at rest for sensitive pipeline information
- Implement appropriate bucket policies to restrict access
- Audit completion marker access for compliance requirements

## Troubleshooting

### Common Issues

**Issue**: Completion markers not created  
**Solution**: Check S3 permissions and bucket configuration

**Issue**: Airflow sensor timeout  
**Solution**: Verify completion marker path pattern matches sensor configuration

**Issue**: Large completion marker files  
**Solution**: Consider excluding large summary data for high-volume pipelines

### Debug Mode

Enable debug logging for completion marker creation:

```bash
export DEBUG_COMPLETION_MARKERS=1
python -m src.cli.main sync pipeline -p test -t table --s3-completion-bucket debug-bucket
```

This provides detailed logging of completion marker creation process.