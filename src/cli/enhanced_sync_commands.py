"""
Enhanced Sync Commands with Airflow Integration
Adds critical enhancements while maintaining backward compatibility
"""

import click
import sys
from typing import List, Optional
from pathlib import Path

from src.cli.airflow_enhancements import (
    AirflowIntegrationMixin, 
    add_airflow_options,
    check_idempotency,
    print_exit_code_help,
    ExitCodes
)
from src.utils.logging import get_logger

logger = get_logger(__name__)

class EnhancedSyncProcessor(AirflowIntegrationMixin):
    """Enhanced sync processor with Airflow integration capabilities"""
    
    def __init__(self):
        super().__init__()
        
    def process_sync_request(self, pipeline: str, tables: List[str], **kwargs):
        """Process sync request with enhanced monitoring and error handling"""
        
        # Initialize metadata tracking
        strategy = kwargs.get('strategy', 'sequential')
        self.initialize_execution_metadata(pipeline, strategy, tables)
        
        # Extract Airflow options
        output_json = kwargs.get('output_json')
        create_completion_marker = kwargs.get('create_completion_marker', False)
        completion_marker_bucket = kwargs.get('completion_marker_bucket')
        completion_marker_prefix = kwargs.get('completion_marker_prefix', 'completion_markers')
        idempotent = kwargs.get('idempotent', False)
        
        click.echo(f"🚀 Enhanced Sync: {pipeline}")
        click.echo(f"📋 Tables: {', '.join(tables)}")
        click.echo(f"🔧 Execution ID: {self.execution_metadata['execution_id']}")
        
        if idempotent:
            click.echo("🔄 Idempotency check enabled")
        
        overall_success = True
        skipped_tables = []
        
        try:
            # Process each table
            for table in tables:
                click.echo(f"\n📊 Processing: {table}")
                
                # Idempotency check
                if idempotent and completion_marker_bucket:
                    if check_idempotency(completion_marker_bucket, pipeline, table, completion_marker_prefix):
                        click.echo(f"⏭️  Skipping {table} - already processed (idempotent mode)")
                        skipped_tables.append(table)
                        self.record_table_result(table, "skipped", 
                                               rows_extracted=0, rows_loaded=0, 
                                               watermark_updated=False)
                        continue
                
                # Process table (integrate with existing sync logic)
                table_result = self._process_single_table(pipeline, table, **kwargs)
                
                if table_result['success']:
                    click.echo(f"✅ {table}: {table_result['rows_processed']} rows processed")
                    self.record_table_result(table, "success", **table_result)
                else:
                    click.echo(f"❌ {table}: {table_result.get('error', 'Unknown error')}")
                    self.record_table_result(table, "failed", 
                                           error_message=table_result.get('error'))
                    overall_success = False
            
            # Determine final status
            if skipped_tables and len(skipped_tables) == len(tables):
                final_status = "skipped"
                click.echo("\n⏭️  All tables skipped (idempotent mode)")
            elif overall_success:
                final_status = "success"
                click.echo("\n✅ All tables processed successfully")
            else:
                final_status = "partial_success" if any(r["status"] == "success" for r in self.table_results.values()) else "failed"
                
        except Exception as e:
            logger.error(f"Unexpected error in sync processing: {e}")
            final_status = "failed"
            overall_success = False
            
        # Finalize metadata
        self.finalize_execution_metadata(final_status)
        
        # Output JSON metadata
        if output_json:
            self.output_json_metadata(output_json)
            click.echo(f"📄 Metadata written to: {output_json}")
        
        # Create S3 completion markers
        if create_completion_marker and completion_marker_bucket:
            marker_status = "SUCCESS" if overall_success else "FAILED"
            self.create_s3_completion_marker(
                completion_marker_bucket, pipeline, tables, 
                marker_status, completion_marker_prefix
            )
            click.echo(f"🏷️  Completion markers created in s3://{completion_marker_bucket}/{completion_marker_prefix}")
        
        # Display summary
        self._display_summary()
        
        # Display health check
        self.display_health_summary()
        
        # Return appropriate exit code
        exit_code = self.determine_exit_code()
        if exit_code != ExitCodes.SUCCESS:
            click.echo(f"\n⚠️  Exiting with code {exit_code} (see --help-exit-codes for details)")
        
        return exit_code
    
    def _process_single_table(self, pipeline: str, table: str, **kwargs) -> dict:
        """Process a single table - integrate with existing sync logic"""
        
        # This is where you'd integrate with your existing sync logic
        # For now, I'll create a mock implementation
        
        try:
            import time
            import random
            
            # Mock processing (replace with actual sync logic)
            start_time = time.time()
            
            # Simulate processing
            click.echo(f"  🔄 Extracting data from {table}...")
            time.sleep(0.1)  # Simulate work
            
            # Mock some results
            rows_processed = random.randint(1000, 50000)
            files_created = random.randint(1, 5)
            
            if random.random() > 0.1:  # 90% success rate for demo
                return {
                    'success': True,
                    'rows_processed': rows_processed,
                    'rows_extracted': rows_processed,
                    'rows_loaded': rows_processed,
                    's3_files_created': files_created,
                    'watermark_updated': True,
                    'duration_seconds': time.time() - start_time,
                    's3_files': [f"s3://bucket/path/file_{i}.parquet" for i in range(files_created)]
                }
            else:
                return {
                    'success': False,
                    'error': f"Mock error for testing - connection timeout to {table}"
                }
                
        except Exception as e:
            return {
                'success': False,
                'error': f"Unexpected error processing {table}: {str(e)}"
            }
    
    def _display_summary(self):
        """Display execution summary"""
        click.echo("\n" + "="*50)
        click.echo("📊 EXECUTION SUMMARY")
        click.echo("="*50)
        
        success_count = len([t for t in self.table_results.values() if t["status"] == "success"])
        failed_count = len([t for t in self.table_results.values() if t["status"] == "failed"])
        skipped_count = len([t for t in self.table_results.values() if t["status"] == "skipped"])
        
        click.echo(f"✅ Successful: {success_count}")
        click.echo(f"❌ Failed: {failed_count}")
        if skipped_count > 0:
            click.echo(f"⏭️  Skipped: {skipped_count}")
        
        click.echo(f"🕒 Duration: {self.execution_metadata.get('duration_seconds', 0):.1f}s")
        click.echo(f"📈 Rows processed: {self.execution_metadata.get('total_rows_processed', 0):,}")
        click.echo(f"📄 Files created: {self.execution_metadata.get('total_files_created', 0)}")
        
        if failed_count > 0:
            click.echo("\n❌ FAILURES:")
            for table, result in self.table_results.items():
                if result["status"] == "failed":
                    click.echo(f"  • {table}: {result.get('error_message', 'Unknown error')}")

# Enhanced command definitions
@click.group()
def enhanced_sync():
    """Enhanced sync commands with Airflow integration"""
    pass

@enhanced_sync.command()
@click.option('--pipeline', '-p', required=True, help='Pipeline configuration name')
@click.option('--table', '-t', multiple=True, required=True, help='Table names to sync')
@click.option('--backup-only', is_flag=True, help='Only backup to S3 (MySQL → S3)')
@click.option('--redshift-only', is_flag=True, help='Only load to Redshift (S3 → Redshift)')
@click.option('--limit', type=int, help='Limit rows per query (testing)')
@click.option('--max-workers', type=int, help='Maximum parallel workers (overrides config)')
@click.option('--max-chunks', type=int, help='Maximum number of chunks to process')
@click.option('--dry-run', is_flag=True, help='Preview operations without execution')
@add_airflow_options()
@click.option('--help-exit-codes', is_flag=True, help='Show exit codes for Airflow integration')
def pipeline(pipeline: str, table: List[str], backup_only: bool, redshift_only: bool,
            limit: Optional[int], max_workers: Optional[int], max_chunks: Optional[int],
            dry_run: bool, output_json: Optional[str], create_completion_marker: bool,
            completion_marker_bucket: Optional[str], completion_marker_prefix: str,
            idempotent: bool, help_exit_codes: bool):
    """Enhanced sync with Airflow integration - pipeline mode"""
    
    if help_exit_codes:
        print_exit_code_help()
        return
    
    if dry_run:
        click.echo("🔍 DRY RUN - Preview mode enabled")
        click.echo(f"Pipeline: {pipeline}")
        click.echo(f"Tables: {', '.join(table)}")
        click.echo(f"Backup only: {backup_only}")
        click.echo(f"Redshift only: {redshift_only}")
        if output_json:
            click.echo(f"JSON output: {output_json}")
        if create_completion_marker:
            click.echo(f"Completion markers: {completion_marker_bucket or 'default bucket'}/{completion_marker_prefix}")
        return
    
    # Process sync request
    processor = EnhancedSyncProcessor()
    
    kwargs = {
        'backup_only': backup_only,
        'redshift_only': redshift_only,
        'limit': limit,
        'max_workers': max_workers,
        'max_chunks': max_chunks,
        'output_json': output_json,
        'create_completion_marker': create_completion_marker,
        'completion_marker_bucket': completion_marker_bucket,
        'completion_marker_prefix': completion_marker_prefix,
        'idempotent': idempotent
    }
    
    exit_code = processor.process_sync_request(pipeline, list(table), **kwargs)
    sys.exit(exit_code)

def register_enhanced_sync_commands(cli):
    """Register enhanced sync commands with the main CLI"""
    cli.add_command(enhanced_sync, name='enhanced-sync')

if __name__ == "__main__":
    enhanced_sync()