"""
Command-line interface for the S3 to Redshift backup system.

This module provides a comprehensive CLI for executing backup operations,
monitoring system status, and managing backup configurations.
"""

import click
import sys
import json
from typing import List, Dict, Any
from pathlib import Path
import time

from src.config.settings import AppConfig
from src.backup.sequential import SequentialBackupStrategy
from src.backup.inter_table import InterTableBackupStrategy
from src.backup.intra_table import IntraTableBackupStrategy
from src.utils.logging import setup_logging, configure_logging_from_config
from src.utils.exceptions import BackupSystemError, ConfigurationError
from src.core.connections import ConnectionManager


# Strategy mapping
STRATEGIES = {
    'sequential': SequentialBackupStrategy,
    'inter-table': InterTableBackupStrategy,
    'intra-table': IntraTableBackupStrategy
}


@click.group()
@click.option('--debug', is_flag=True, help='Enable debug logging')
@click.option('--config-file', type=click.Path(exists=True), help='Configuration file path')
@click.option('--log-file', type=click.Path(), help='Log file path')
@click.option('--json-logs', is_flag=True, help='Output logs in JSON format')
@click.pass_context
def cli(ctx, debug, config_file, log_file, json_logs):
    """
    S3 to Redshift Incremental Backup System
    
    A production-ready system for backing up MySQL data to S3 in parquet format
    with support for multiple backup strategies and comprehensive monitoring.
    """
    ctx.ensure_object(dict)
    
    try:
        # Setup logging first
        log_level = "DEBUG" if debug else "INFO"
        setup_logging(
            level=log_level,
            log_file=log_file,
            json_logs=json_logs,
            include_caller=debug
        )
        
        # Load configuration
        if config_file:
            config = AppConfig.load(config_file)
        else:
            config = AppConfig.load()
        
        # Override debug setting if specified
        if debug:
            config.debug = True
            config.log_level = "DEBUG"
        
        # Configure application logging
        backup_logger = configure_logging_from_config(config)
        
        # Validate configuration (but don't fail CLI initialization)
        try:
            errors = config.validate_all()
            if errors:
                click.echo("⚠️  Configuration warnings:")
                for error in errors:
                    click.echo(f"   - {error}")
                click.echo()
        except Exception as validation_error:
            # Store validation error for later display but don't fail initialization
            click.echo(f"⚠️  Configuration validation error: {validation_error}")
            click.echo()
        
        # Store in context
        ctx.obj['config'] = config
        ctx.obj['backup_logger'] = backup_logger
        ctx.obj['debug'] = debug
        
        backup_logger.logger.info(
            "CLI initialized successfully",
            debug_mode=debug,
            config_file=config_file,
            log_file=log_file
        )
        
    except Exception as e:
        click.echo(f"❌ Failed to initialize: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.option('--tables', '-t', multiple=True, required=True, 
              help='Tables to backup (format: schema.table_name)')
@click.option('--strategy', '-s', 
              type=click.Choice(['sequential', 'inter-table', 'intra-table']),
              default='sequential', 
              help='Backup strategy to use')
@click.option('--max-workers', type=int, 
              help='Maximum parallel workers (overrides config)')
@click.option('--batch-size', type=int, 
              help='Batch size for processing (overrides config)')
@click.option('--dry-run', is_flag=True, 
              help='Show what would be done without executing')
@click.option('--estimate', is_flag=True, 
              help='Estimate completion time')
@click.pass_context
def backup(ctx, tables: List[str], strategy: str, max_workers: int, 
           batch_size: int, dry_run: bool, estimate: bool):
    """
    Run incremental backup for specified tables.
    
    Examples:
        s3-backup backup -t settlement.settlement_normal_delivery_detail -s sequential
        s3-backup backup -t table1 -t table2 -s inter-table --max-workers 8
        s3-backup backup -t large_table -s intra-table --estimate
    """
    config = ctx.obj['config']
    backup_logger = ctx.obj['backup_logger']
    
    # Override config with CLI options if provided
    if max_workers:
        config.backup.max_workers = max_workers
    if batch_size:
        config.backup.batch_size = batch_size
    
    # Validate strategy choice
    if strategy not in STRATEGIES:
        click.echo(f"❌ Unknown strategy: {strategy}", err=True)
        sys.exit(1)
    
    # Validate table names
    for table in tables:
        if '.' not in table:
            click.echo(f"⚠️  Table '{table}' should include schema (e.g., schema.table_name)")
    
    # Show strategy information
    strategy_class = STRATEGIES[strategy]
    temp_strategy = strategy_class(config)
    strategy_info = temp_strategy.get_strategy_info()
    
    click.echo(f"🚀 {strategy_info['name']}")
    click.echo(f"   {strategy_info['description']}")
    click.echo()
    
    # Show estimate if requested
    if estimate:
        click.echo("📊 Time Estimation:")
        estimates = temp_strategy.estimate_completion_time(list(tables))
        
        click.echo(f"   Strategy: {estimates['strategy']}")
        click.echo(f"   Tables: {estimates['total_tables']}")
        
        if 'parallel_duration_minutes' in estimates:
            click.echo(f"   Estimated time: {estimates['parallel_duration_minutes']} minutes")
            if 'estimated_speedup' in estimates:
                click.echo(f"   Estimated speedup: {estimates['estimated_speedup']}x")
        else:
            click.echo(f"   Estimated time: {estimates.get('estimated_duration_minutes', 'N/A')} minutes")
        
        click.echo()
        
        if not click.confirm("Continue with backup?"):
            return
    
    # Show dry run information
    if dry_run:
        click.echo("🔍 Dry Run - No actual backup will be performed")
        click.echo(f"   Strategy: {strategy}")
        click.echo(f"   Tables: {', '.join(tables)}")
        click.echo(f"   Batch size: {config.backup.batch_size}")
        click.echo(f"   Max workers: {config.backup.max_workers}")
        click.echo()
        return
    
    # Execute backup
    try:
        click.echo(f"▶️  Starting {strategy} backup...")
        click.echo(f"   Tables: {', '.join(tables)}")
        click.echo(f"   Workers: {config.backup.max_workers}")
        click.echo(f"   Batch size: {config.backup.batch_size}")
        click.echo()
        
        # Create and execute backup strategy
        backup_strategy = strategy_class(config)
        
        start_time = time.time()
        success = backup_strategy.execute(list(tables))
        duration = time.time() - start_time
        
        # Get detailed results
        summary = backup_strategy.get_backup_summary()
        
        # Display results
        if success:
            click.echo("✅ Backup completed successfully!")
        else:
            click.echo("❌ Backup failed!")
        
        click.echo()
        click.echo("📈 Backup Summary:")
        click.echo(f"   Duration: {duration:.1f} seconds ({duration/60:.1f} minutes)")
        click.echo(f"   Tables processed: {summary['tables_processed']}")
        click.echo(f"   Total rows: {summary['total_rows']:,}")
        click.echo(f"   Total batches: {summary['total_batches']}")
        click.echo(f"   Data uploaded: {summary['s3_stats']['total_size_mb']} MB")
        click.echo(f"   Processing rate: {summary['avg_rows_per_second']:,.0f} rows/second")
        
        if summary['errors'] > 0:
            click.echo(f"   Errors: {summary['errors']}")
        
        if summary['warnings'] > 0:
            click.echo(f"   Warnings: {summary['warnings']}")
        
        # Show per-table details if multiple tables
        if len(tables) > 1 and summary['per_table_metrics']:
            click.echo()
            click.echo("📋 Per-Table Results:")
            for table_name, metrics in summary['per_table_metrics'].items():
                click.echo(f"   {table_name}:")
                click.echo(f"     Rows: {metrics['rows']:,}")
                click.echo(f"     Batches: {metrics['batches']}")
                click.echo(f"     Duration: {metrics.get('duration', 0):.1f}s")
                click.echo(f"     Rate: {metrics.get('rows_per_second', 0):,.0f} rows/s")
        
        sys.exit(0 if success else 1)
        
    except KeyboardInterrupt:
        click.echo("\n⏹️  Backup interrupted by user")
        sys.exit(130)
    except Exception as e:
        backup_logger.error_occurred(e, "cli_backup_command")
        click.echo(f"❌ Backup error: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.pass_context
def status(ctx):
    """Check system status and connectivity."""
    config = ctx.obj['config']
    backup_logger = ctx.obj['backup_logger']
    
    click.echo("🔍 System Status Check")
    click.echo("=" * 50)
    
    try:
        # Check configuration
        click.echo("📋 Configuration:")
        click.echo(f"   Database: {config.database.host}:{config.database.port}")
        click.echo(f"   S3 Bucket: {config.s3.bucket_name}")
        click.echo(f"   SSH Host: {config.ssh.bastion_host}")
        click.echo(f"   Log Level: {config.log_level}")
        click.echo()
        
        # Test connections with error handling
        click.echo("🔌 Connectivity Tests:")
        
        try:
            connection_manager = ConnectionManager(config)
            health_status = connection_manager.health_check()
            
            for component, status in health_status.items():
                if status == 'OK':
                    click.echo(f"   ✅ {component.upper()}: {status}")
                else:
                    click.echo(f"   ❌ {component.upper()}: {status}")
        
        except Exception as conn_error:
            click.echo(f"   ⚠️  Connection tests skipped: {conn_error}")
            
            # Try individual components with better error handling
            click.echo("   🔍 Testing individual components:")
            
            # Test S3 configuration
            try:
                import boto3
                s3_client = boto3.client(
                    's3',
                    aws_access_key_id=config.s3.access_key,
                    aws_secret_access_key=config.s3.secret_key.get_secret_value(),
                    region_name=config.s3.region
                )
                s3_client.head_bucket(Bucket=config.s3.bucket_name)
                click.echo(f"   ✅ S3: OK (bucket accessible)")
            except Exception as s3_error:
                click.echo(f"   ❌ S3: {s3_error}")
            
            # Test SSH configuration
            try:
                from pathlib import Path
                ssh_key_path = Path(config.ssh.bastion_key_path)
                if ssh_key_path.exists():
                    click.echo(f"   ✅ SSH Key: OK (file exists)")
                else:
                    click.echo(f"   ❌ SSH Key: File not found at {config.ssh.bastion_key_path}")
            except Exception as ssh_error:
                click.echo(f"   ⚠️  SSH Key: {ssh_error}")
        
        # Check last backup info
        click.echo()
        click.echo("📅 Last Backup Information:")
        
        try:
            from src.core.watermark import WatermarkManager
            
            # Try to create watermark manager with S3-only access
            try:
                import boto3
                s3_client = boto3.client(
                    's3',
                    aws_access_key_id=config.s3.access_key,
                    aws_secret_access_key=config.s3.secret_key.get_secret_value(),
                    region_name=config.s3.region
                )
                watermark_manager = WatermarkManager(config, s3_client)
                
                last_watermark = watermark_manager.get_last_watermark()
                watermark_metadata = watermark_manager.get_watermark_metadata()
                
                click.echo(f"   Last watermark: {last_watermark}")
                
                if watermark_metadata:
                    click.echo(f"   Last update: {watermark_metadata.get('updated_at', 'Unknown')}")
                    if 'backup_strategy' in watermark_metadata:
                        click.echo(f"   Strategy used: {watermark_metadata['backup_strategy']}")
                else:
                    click.echo(f"   Default watermark (no backups yet)")
            
            except Exception as wm_error:
                click.echo(f"   ⚠️  Could not retrieve backup info: {wm_error}")
        
        except ImportError as e:
            click.echo(f"   ⚠️  Watermark module error: {e}")
        
        # System resources
        click.echo()
        click.echo("💾 System Configuration:")
        click.echo(f"   Max workers: {config.backup.max_workers}")
        click.echo(f"   Batch size: {config.backup.batch_size}")
        click.echo(f"   Timeout: {config.backup.timeout_seconds}s")
        click.echo(f"   Retry attempts: {config.backup.retry_attempts}")
        
        # Configuration validation summary
        click.echo()
        click.echo("⚙️  Configuration Validation:")
        errors = config.validate_all()
        if errors:
            click.echo(f"   ⚠️  Found {len(errors)} configuration issues:")
            for error in errors:
                click.echo(f"     - {error}")
        else:
            click.echo(f"   ✅ All configuration validated")
        
        click.echo()
        click.echo("✅ Status check completed")
        
    except Exception as e:
        backup_logger.error_occurred(e, "cli_status_command")
        click.echo(f"❌ Status check failed: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.option('--strategy', type=click.Choice(['sequential', 'inter-table', 'intra-table']), 
              help='Show info for specific strategy')
@click.pass_context
def info(ctx, strategy: str):
    """Show information about backup strategies."""
    config = ctx.obj['config']
    
    if strategy:
        # Show specific strategy info
        if strategy not in STRATEGIES:
            click.echo(f"❌ Unknown strategy: {strategy}", err=True)
            return
        
        strategy_class = STRATEGIES[strategy]
        temp_strategy = strategy_class(config)
        strategy_info = temp_strategy.get_strategy_info()
        
        click.echo(f"📋 {strategy_info['name']}")
        click.echo("=" * 50)
        click.echo(f"Description: {strategy_info['description']}")
        click.echo()
        
        click.echo("✅ Advantages:")
        for advantage in strategy_info['advantages']:
            click.echo(f"   • {advantage}")
        click.echo()
        
        click.echo("⚠️  Considerations:")
        for disadvantage in strategy_info['disadvantages']:
            click.echo(f"   • {disadvantage}")
        click.echo()
        
        click.echo("🎯 Best for:")
        for use_case in strategy_info['best_for']:
            click.echo(f"   • {use_case}")
        click.echo()
        
        click.echo("⚙️  Configuration:")
        for key, value in strategy_info['configuration'].items():
            click.echo(f"   {key}: {value}")
    
    else:
        # Show all strategies
        click.echo("📋 Available Backup Strategies")
        click.echo("=" * 50)
        
        for strategy_name, strategy_class in STRATEGIES.items():
            temp_strategy = strategy_class(config)
            strategy_info = temp_strategy.get_strategy_info()
            
            click.echo(f"\n🔸 {strategy_name.upper()}")
            click.echo(f"   {strategy_info['description']}")
            click.echo(f"   Best for: {', '.join(strategy_info['best_for'][:2])}")


@cli.command()
@click.option('--bucket', help='S3 bucket to clean (defaults to configured bucket)')
@click.option('--prefix', help='S3 prefix to clean (defaults to incremental path)')
@click.option('--older-than-days', type=int, default=30, 
              help='Delete files older than N days')
@click.option('--dry-run', is_flag=True, help='Show what would be deleted')
@click.option('--confirm', is_flag=True, help='Confirm deletion')
@click.pass_context
def clean(ctx, bucket: str, prefix: str, older_than_days: int, dry_run: bool, confirm: bool):
    """Clean old backup files from S3."""
    config = ctx.obj['config']
    backup_logger = ctx.obj['backup_logger']
    
    bucket = bucket or config.s3.bucket_name
    prefix = prefix or config.s3.incremental_path.strip('/')
    
    click.echo(f"🧹 S3 Cleanup Operation")
    click.echo(f"   Bucket: {bucket}")
    click.echo(f"   Prefix: {prefix}")
    click.echo(f"   Older than: {older_than_days} days")
    click.echo()
    
    if dry_run:
        click.echo("🔍 Dry run - No files will be deleted")
        click.echo()
    
    try:
        from src.core.s3_manager import S3Manager
        from src.core.connections import ConnectionManager
        
        connection_manager = ConnectionManager(config)
        s3_manager = S3Manager(config, connection_manager.get_s3_client())
        
        # List files
        files = s3_manager.list_backup_files(max_keys=1000)
        
        if not files:
            click.echo("No backup files found")
            return
        
        # Filter by age
        from datetime import datetime, timedelta
        cutoff_date = datetime.now() - timedelta(days=older_than_days)
        
        old_files = [
            f for f in files 
            if f['last_modified'].replace(tzinfo=None) < cutoff_date
        ]
        
        if not old_files:
            click.echo(f"No files older than {older_than_days} days found")
            return
        
        total_size = sum(f['size'] for f in old_files)
        total_size_mb = total_size / (1024 * 1024)
        
        click.echo(f"Found {len(old_files)} files to delete ({total_size_mb:.1f} MB)")
        
        if dry_run:
            click.echo("\nFiles that would be deleted:")
            for f in old_files[:10]:  # Show first 10
                click.echo(f"   {f['key']} ({f['size']} bytes)")
            if len(old_files) > 10:
                click.echo(f"   ... and {len(old_files) - 10} more files")
            return
        
        if not confirm:
            if not click.confirm(f"\nDelete {len(old_files)} files?"):
                return
        
        # Delete files
        keys_to_delete = [f['key'] for f in old_files]
        
        click.echo("Deleting files...")
        result = s3_manager.delete_backup_files(keys_to_delete, confirm=True)
        
        click.echo(f"✅ Cleanup completed:")
        click.echo(f"   Deleted: {result['deleted']} files")
        click.echo(f"   Errors: {result['errors']} files")
        click.echo(f"   Space freed: {total_size_mb:.1f} MB")
        
    except Exception as e:
        backup_logger.error_occurred(e, "cli_clean_command")
        click.echo(f"❌ Cleanup failed: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.option('--output', '-o', type=click.Path(), help='Output file path')
@click.pass_context
def config(ctx, output: str):
    """Show current configuration."""
    config = ctx.obj['config']
    
    # Prepare config data (hide sensitive info)
    config_data = {
        'database': {
            'host': config.database.host,
            'port': config.database.port,
            'user': config.database.user,
            'database': config.database.database
        },
        'ssh': {
            'bastion_host': config.ssh.bastion_host,
            'bastion_user': config.ssh.bastion_user,
            'bastion_key_path': config.ssh.bastion_key_path
        },
        's3': {
            'bucket_name': config.s3.bucket_name,
            'region': config.s3.region,
            'incremental_path': config.s3.incremental_path
        },
        'backup': {
            'batch_size': config.backup.batch_size,
            'max_workers': config.backup.max_workers,
            'num_chunks': config.backup.num_chunks,
            'retry_attempts': config.backup.retry_attempts,
            'timeout_seconds': config.backup.timeout_seconds
        },
        'logging': {
            'log_level': config.log_level,
            'debug': config.debug
        }
    }
    
    if output:
        with open(output, 'w') as f:
            json.dump(config_data, f, indent=2)
        click.echo(f"✅ Configuration written to {output}")
    else:
        click.echo("⚙️  Current Configuration:")
        click.echo(json.dumps(config_data, indent=2))


@cli.command()
@click.argument('operation', type=click.Choice(['get', 'set', 'list', 'backup', 'restore']))
@click.option('--table', '-t', help='Table name for table-specific watermark')
@click.option('--timestamp', help='Timestamp for set operation (YYYY-MM-DD HH:MM:SS)')
@click.option('--backup-key', help='Backup key for restore operation')
@click.option('--metadata', help='JSON metadata for set operation')
@click.pass_context
def watermark(ctx, operation: str, table: str, timestamp: str, backup_key: str, metadata: str):
    """
    Manage watermark timestamps for incremental backups.
    
    Operations:
        get - Get current watermark
        set - Set new watermark timestamp
        list - List watermark backups
        backup - Create watermark backup
        restore - Restore from backup
    
    Examples:
        s3-backup watermark get
        s3-backup watermark get -t settlement.table_name
        s3-backup watermark set --timestamp "2025-08-11 10:00:00"
        s3-backup watermark list
        s3-backup watermark backup
        s3-backup watermark restore --backup-key watermark/backup_20250811_100000
    """
    config = ctx.obj['config']
    backup_logger = ctx.obj['backup_logger']
    
    try:
        # Create S3 client directly (watermark operations only need S3, not SSH/DB)
        from src.core.watermark import WatermarkManager
        import boto3
        
        s3_client = boto3.client(
            's3',
            aws_access_key_id=config.s3.access_key,
            aws_secret_access_key=config.s3.secret_key.get_secret_value(),
            region_name=config.s3.region
        )
        watermark_manager = WatermarkManager(config, s3_client)
        
        click.echo(f"🔖 Watermark {operation.upper()}")
        if table:
            click.echo(f"   Table: {table}")
        click.echo()
        
        if operation == 'get':
            # Get watermark
            watermark = watermark_manager.get_last_watermark(table)
            watermark_metadata = watermark_manager.get_watermark_metadata()
            
            click.echo(f"📅 Current Watermark: {watermark}")
            
            if watermark_metadata:
                click.echo("📋 Metadata:")
                for key, value in watermark_metadata.items():
                    if key not in ['update_source', 'format_version']:  # Skip internal metadata
                        click.echo(f"   {key}: {value}")
        
        elif operation == 'set':
            if not timestamp:
                click.echo("❌ Timestamp required for set operation", err=True)
                click.echo("   Use --timestamp 'YYYY-MM-DD HH:MM:SS'")
                sys.exit(1)
            
            # Parse metadata if provided
            metadata_dict = {}
            if metadata:
                try:
                    metadata_dict = json.loads(metadata)
                except json.JSONDecodeError as e:
                    click.echo(f"❌ Invalid JSON metadata: {e}", err=True)
                    sys.exit(1)
            
            # Set watermark
            success = watermark_manager.update_watermark(
                timestamp=timestamp,
                table_name=table,
                metadata=metadata_dict
            )
            
            if success:
                click.echo(f"✅ Watermark updated to: {timestamp}")
                if table:
                    click.echo(f"   Table: {table}")
                if metadata_dict:
                    click.echo(f"   Metadata: {json.dumps(metadata_dict, indent=2)}")
            else:
                click.echo("❌ Failed to update watermark", err=True)
                sys.exit(1)
        
        elif operation == 'list':
            # List backups
            backups = watermark_manager.list_watermark_backups()
            
            if not backups:
                click.echo("No watermark backups found")
                return
            
            click.echo(f"📂 Found {len(backups)} watermark backups:")
            click.echo()
            
            for i, backup in enumerate(backups[:10]):  # Show first 10
                click.echo(f"   {i+1}. {backup['key']}")
                click.echo(f"      Size: {backup['size']} bytes")
                click.echo(f"      Modified: {backup['last_modified']}")
                click.echo()
            
            if len(backups) > 10:
                click.echo(f"   ... and {len(backups) - 10} more backups")
        
        elif operation == 'backup':
            # Create backup
            backup_key = watermark_manager.backup_watermark()
            click.echo(f"✅ Watermark backup created: {backup_key}")
        
        elif operation == 'restore':
            if not backup_key:
                click.echo("❌ Backup key required for restore operation", err=True)
                click.echo("   Use --backup-key 'backup_key_name'")
                sys.exit(1)
            
            if not click.confirm(f"⚠️  Restore watermark from '{backup_key}'? This will overwrite the current watermark."):
                return
            
            # Restore watermark
            success = watermark_manager.restore_watermark(backup_key)
            
            if success:
                click.echo(f"✅ Watermark restored from: {backup_key}")
                # Show new watermark
                watermark = watermark_manager.get_last_watermark(table)
                click.echo(f"   New watermark: {watermark}")
            else:
                click.echo("❌ Failed to restore watermark", err=True)
                sys.exit(1)
        
    except Exception as e:
        backup_logger.error_occurred(e, "cli_watermark_command")
        click.echo(f"❌ Watermark operation failed: {e}", err=True)
        sys.exit(1)


if __name__ == '__main__':
    cli()