"""
Completion Marker Utility Commands
Optional utilities for advanced S3 completion marker management
"""

import json
import click
import sys
from pathlib import Path
from typing import Optional, List
from datetime import datetime

from src.cli.airflow_integration import S3CompletionMarker
from src.utils.logging import get_logger

logger = get_logger(__name__)


@click.group()
def completion_markers():
    """Utility commands for S3 completion marker management"""
    pass


@completion_markers.command()
@click.option('--json-metadata', type=click.Path(exists=True), required=True,
              help='Path to JSON metadata file from previous sync')
@click.option('--s3-completion-bucket', required=True,
              help='S3 bucket for completion markers')
@click.option('--completion-prefix', default='completion_markers',
              help='S3 prefix for completion markers')
@click.option('--dry-run', is_flag=True, help='Show what would be created without actually creating')
def from_json(json_metadata: str, s3_completion_bucket: str, completion_prefix: str, dry_run: bool):
    """Create completion markers from existing JSON metadata file"""
    
    try:
        # Load JSON metadata
        with open(json_metadata, 'r') as f:
            metadata = json.load(f)
        
        pipeline = metadata.get('pipeline')
        execution_id = metadata.get('execution_id')
        overall_status = metadata.get('status', 'unknown')
        
        if not pipeline or not execution_id:
            click.echo("❌ Invalid JSON metadata: missing pipeline or execution_id")
            sys.exit(1)
        
        click.echo(f"📄 Processing metadata: {json_metadata}")
        click.echo(f"🔧 Execution ID: {execution_id}")
        click.echo(f"📋 Pipeline: {pipeline}")
        click.echo(f"📊 Status: {overall_status}")
        
        if dry_run:
            click.echo("🔍 DRY RUN - Markers that would be created:")
            
            # Show table markers that would be created
            for table_name, result in metadata.get('table_results', {}).items():
                table_status = "SUCCESS" if result.get("status") == "success" else "FAILED"
                click.echo(f"  📝 Table: {table_name} → {table_status}")
            
            # Show pipeline marker that would be created
            pipeline_status = "SUCCESS" if overall_status == "success" else "FAILED"
            click.echo(f"  📝 Pipeline: {pipeline} → {pipeline_status}")
            
            return
        
        # Create S3 marker instance
        s3_marker = S3CompletionMarker(s3_completion_bucket)
        
        # Create table markers
        tables_processed = []
        for table_name, result in metadata.get('table_results', {}).items():
            table_status = "SUCCESS" if result.get("status") == "success" else "FAILED"
            
            success = s3_marker.create_table_marker(
                table_name, table_status, execution_id, completion_prefix
            )
            
            if success:
                click.echo(f"✅ Created table marker: {table_name} ({table_status})")
                tables_processed.append(table_name)
            else:
                click.echo(f"❌ Failed to create table marker: {table_name}")
        
        # Create pipeline marker
        pipeline_status = "SUCCESS" if overall_status == "success" else "FAILED"
        success = s3_marker.create_pipeline_marker(
            pipeline, tables_processed, pipeline_status, execution_id, completion_prefix
        )
        
        if success:
            click.echo(f"✅ Created pipeline marker: {pipeline} ({pipeline_status})")
        else:
            click.echo(f"❌ Failed to create pipeline marker: {pipeline}")
        
        click.echo(f"\n🎉 Completion markers created in s3://{s3_completion_bucket}/{completion_prefix}")
        
    except Exception as e:
        logger.error(f"Failed to create completion markers: {e}")
        click.echo(f"❌ Error: {e}")
        sys.exit(1)


@completion_markers.command()
@click.option('--pipeline', required=True, help='Pipeline name')
@click.option('--table', multiple=True, required=True, help='Table names')
@click.option('--status', type=click.Choice(['SUCCESS', 'FAILED', 'PARTIAL_SUCCESS']), 
              required=True, help='Overall completion status')
@click.option('--execution-id', help='Custom execution ID (auto-generated if not provided)')
@click.option('--s3-completion-bucket', required=True, help='S3 bucket for completion markers')
@click.option('--completion-prefix', default='completion_markers', help='S3 prefix for completion markers')
@click.option('--table-status', multiple=True, 
              help='Individual table status in format table_name:STATUS (defaults to overall status)')
@click.option('--dry-run', is_flag=True, help='Show what would be created without actually creating')
def manual(pipeline: str, table: List[str], status: str, s3_completion_bucket: str, 
           completion_prefix: str, execution_id: Optional[str], table_status: List[str], dry_run: bool):
    """Manually create completion markers with custom parameters"""
    
    try:
        # Generate execution ID if not provided
        if not execution_id:
            execution_id = f"manual_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # Parse table-specific statuses
        table_statuses = {}
        for ts in table_status:
            if ':' in ts:
                table_name, table_stat = ts.split(':', 1)
                table_statuses[table_name] = table_stat.upper()
        
        click.echo(f"🔧 Creating manual completion markers:")
        click.echo(f"  📋 Pipeline: {pipeline}")
        click.echo(f"  📊 Tables: {', '.join(table)}")
        click.echo(f"  ✅ Overall Status: {status}")
        click.echo(f"  🔢 Execution ID: {execution_id}")
        
        if dry_run:
            click.echo("🔍 DRY RUN - Markers that would be created:")
            
            for table_name in table:
                table_stat = table_statuses.get(table_name, status)
                click.echo(f"  📝 Table: {table_name} → {table_stat}")
            
            click.echo(f"  📝 Pipeline: {pipeline} → {status}")
            return
        
        # Create S3 marker instance
        s3_marker = S3CompletionMarker(s3_completion_bucket)
        
        # Create table markers
        for table_name in table:
            table_stat = table_statuses.get(table_name, status)
            
            success = s3_marker.create_table_marker(
                table_name, table_stat, execution_id, completion_prefix
            )
            
            if success:
                click.echo(f"✅ Created table marker: {table_name} ({table_stat})")
            else:
                click.echo(f"❌ Failed to create table marker: {table_name}")
        
        # Create pipeline marker
        success = s3_marker.create_pipeline_marker(
            pipeline, list(table), status, execution_id, completion_prefix
        )
        
        if success:
            click.echo(f"✅ Created pipeline marker: {pipeline} ({status})")
        else:
            click.echo(f"❌ Failed to create pipeline marker: {pipeline}")
        
        click.echo(f"\n🎉 Manual completion markers created in s3://{s3_completion_bucket}/{completion_prefix}")
        
    except Exception as e:
        logger.error(f"Failed to create manual completion markers: {e}")
        click.echo(f"❌ Error: {e}")
        sys.exit(1)


@completion_markers.command()
@click.option('--s3-completion-bucket', required=True, help='S3 bucket to list markers from')
@click.option('--completion-prefix', default='completion_markers', help='S3 prefix for completion markers')
@click.option('--date', help='Date filter (YYYYMMDD format)')
@click.option('--pipeline', help='Filter by pipeline name')
@click.option('--show-content', is_flag=True, help='Show marker file contents')
def list_markers(s3_completion_bucket: str, completion_prefix: str, 
                date: Optional[str], pipeline: Optional[str], show_content: bool):
    """List existing completion markers"""
    
    try:
        import boto3
        from botocore.exceptions import ClientError
        
        s3_client = boto3.client('s3')
        
        # Build prefix for listing
        prefix = completion_prefix + "/"
        if date:
            prefix += f"{date}/"
        
        click.echo(f"📋 Listing completion markers from s3://{s3_completion_bucket}/{prefix}")
        
        # List objects
        response = s3_client.list_objects_v2(Bucket=s3_completion_bucket, Prefix=prefix)
        
        if 'Contents' not in response:
            click.echo("📭 No completion markers found")
            return
        
        markers_found = 0
        for obj in response['Contents']:
            key = obj['Key']
            
            # Apply pipeline filter
            if pipeline and pipeline.replace('.', '_') not in key:
                continue
            
            markers_found += 1
            
            # Show basic info
            click.echo(f"\n📄 {key}")
            click.echo(f"   📅 Modified: {obj['LastModified']}")
            click.echo(f"   📦 Size: {obj['Size']} bytes")
            
            # Show content if requested
            if show_content:
                try:
                    content_obj = s3_client.get_object(Bucket=s3_completion_bucket, Key=key)
                    content = json.loads(content_obj['Body'].read())
                    click.echo(f"   📋 Content: {json.dumps(content, indent=6)}")
                except Exception as e:
                    click.echo(f"   ❌ Could not read content: {e}")
        
        if markers_found == 0:
            click.echo("📭 No completion markers found matching filters")
        else:
            click.echo(f"\n✅ Found {markers_found} completion markers")
        
    except Exception as e:
        logger.error(f"Failed to list completion markers: {e}")
        click.echo(f"❌ Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    completion_markers()