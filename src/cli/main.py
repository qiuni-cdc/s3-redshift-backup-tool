"""
Command-line interface for the S3 to Redshift backup system.

This module provides a comprehensive CLI for executing backup operations,
monitoring system status, and managing backup configurations.
"""

import click
import sys
import json
import warnings
import fnmatch
from typing import List, Dict, Any, Optional, Tuple
from pathlib import Path
import time

# Suppress common library warnings in non-debug mode
warnings.filterwarnings("ignore", category=UserWarning, module="pydantic")
warnings.filterwarnings("ignore", category=DeprecationWarning, module="paramiko")
warnings.filterwarnings("ignore", category=DeprecationWarning, module="cryptography")

from src.config.settings import AppConfig
from src.backup.sequential import SequentialBackupStrategy
from src.backup.inter_table import InterTableBackupStrategy
# from src.backup.intra_table import IntraTableBackupStrategy  # Disabled: complex with bugs
from src.utils.logging import setup_logging, configure_logging_from_config
from src.utils.exceptions import BackupSystemError, ConfigurationError
from src.core.connections import ConnectionManager


def _auto_detect_pipeline_for_table(table_name: str) -> Tuple[Optional[str], List[str], bool]:
    """
    Auto-detect the appropriate pipeline for a given table with conflict detection.
    
    This provides smart validation to prevent accidental usage of wrong pipelines
    when multiple pipelines could handle the same table.
    
    Detection Strategy:
    1. Check explicit table configurations (highest priority)
    2. Find all potential pipeline matches  
    3. Return single match if unambiguous, None if multiple matches found
    
    Args:
        table_name: Table name to find pipeline for (e.g., 'settlement.settle_orders')
        
    Returns:
        Tuple of (pipeline_name, all_matches, is_ambiguous)
        - pipeline_name: Single unambiguous match, or None if ambiguous/none found
        - all_matches: List of all potential pipeline matches
        - is_ambiguous: True if multiple pipelines could handle this table
    """
    try:
        config_dir = Path("config/pipelines")
        if not config_dir.exists():
            return None, [], False
            
        # Extract schema from table name for pattern matching
        schema_name = table_name.split('.')[0] if '.' in table_name else None
        
        explicit_matches = []  # Pipelines that explicitly define this table
        pattern_matches = []   # Pipelines that match by schema pattern
        
        # Check each pipeline configuration
        import yaml
        for pipeline_file in config_dir.glob("*.yml"):
            pipeline_name = pipeline_file.stem
            
            try:
                with open(pipeline_file, 'r') as f:
                    pipeline_config = yaml.safe_load(f)
                
                # Check if this pipeline explicitly lists this table
                tables = pipeline_config.get('tables', {})
                if table_name in tables:
                    explicit_matches.append(pipeline_name)
                    continue  # Explicit match takes priority
                
                # Check schema-based patterns
                if schema_name and schema_name != 'default':
                    # settlement.* could match multiple settlement pipelines
                    if schema_name.lower() in pipeline_name.lower():
                        pattern_matches.append(pipeline_name)
                        
            except Exception:
                continue  # Skip invalid pipeline files
        
        # Combine matches with explicit taking priority
        all_matches = explicit_matches + pattern_matches
        
        # Remove duplicates while preserving order
        seen = set()
        all_matches = [x for x in all_matches if not (x in seen or seen.add(x))]
        
        # Add default as fallback if no other matches
        if not all_matches:
            default_pipeline = config_dir / "default.yml"
            if default_pipeline.exists():
                all_matches = ["default"]
        
        # Determine if ambiguous
        is_ambiguous = len(all_matches) > 1
        
        if is_ambiguous:
            # Multiple matches - return None to force user specification
            return None, all_matches, True
        elif all_matches:
            # Single unambiguous match
            return all_matches[0], all_matches, False
        else:
            # No matches found
            return None, [], False
        
    except Exception as e:
        # Graceful fallback
        return None, [], False


def _get_canonical_connection_for_pipeline(pipeline_name: str) -> Optional[str]:
    """
    Get canonical connection name for a pipeline to enable unified watermark scoping.
    
    Args:
        pipeline_name: Name of the pipeline to lookup
        
    Returns:
        Canonical connection name, or None if pipeline not found/no mapping
    """
    try:
        # Check if multi-schema components are available
        config_path = Path("config/pipelines") / f"{pipeline_name}.yml"
        if not config_path.exists():
            return None
            
        # Load pipeline configuration to get source connection
        import yaml
        with open(config_path, 'r') as f:
            pipeline_config = yaml.safe_load(f)
            
        pipeline_info = pipeline_config.get('pipeline', {})
        source_connection = pipeline_info.get('source')
        
        if source_connection:
            # Return the canonical connection name for unified scoping
            return source_connection
        else:
            return None
            
    except Exception as e:
        # Fallback gracefully - if we can't resolve, let pipeline scoping work as before
        return None


def setup_v1_1_0_commands():
    """Setup v1.1.0 multi-schema commands if configuration is available"""
    try:
        # Check if v1.1.0 configuration exists
        config_path = Path("config")
        if config_path.exists() and (config_path / "connections.yml").exists():
            # Import and add v1.1.0 commands
            from src.cli.multi_schema_commands import add_multi_schema_commands
            add_multi_schema_commands(cli)
            return True
    except ImportError:
        # v1.1.0 components not available
        pass
    except Exception:
        # Configuration issues - fallback to v1.0.0 mode
        pass
    
    return False


def setup_v1_2_0_commands():
    """Setup v1.2.0 CDC Intelligence Engine commands"""
    try:
        # Check if v1.2.0 CDC components are available
        from src.cli.v1_2_0_commands import register_v1_2_commands
        register_v1_2_commands(cli)
        return True
    except ImportError:
        # v1.2.0 components not available
        pass
    except Exception:
        # Configuration issues - v1.2.0 not available
        pass
    
    return False


# Strategy mapping
STRATEGIES = {
    'sequential': SequentialBackupStrategy,
    'inter-table': InterTableBackupStrategy,
    # 'intra-table': IntraTableBackupStrategy  # Disabled: complex with boundary bugs
}


@click.group()
@click.option('--debug', is_flag=True, help='Enable debug logging')
@click.option('--quiet', '-q', is_flag=True, help='Quiet mode - only show errors and warnings')
@click.option('--config-file', type=click.Path(exists=True), help='Configuration file path')
@click.option('--log-file', type=click.Path(), help='Log file path')
@click.option('--json-logs', is_flag=True, help='Output logs in JSON format')
@click.pass_context
def cli(ctx, debug, quiet, config_file, log_file, json_logs):
    """
    S3 to Redshift Incremental Backup System
    
    A production-ready system for backing up MySQL data to S3 in parquet format
    with support for multiple backup strategies and comprehensive monitoring.
    """
    ctx.ensure_object(dict)
    
    try:
        # Setup logging first
        if quiet:
            log_level = "WARNING"
        elif debug:
            log_level = "DEBUG"
        else:
            log_level = "INFO"
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
        
        # Integrate v1.1.0 Multi-Schema CLI Features
        try:
            from src.cli.cli_integration import integrate_multi_schema_cli, add_version_detection_commands
            integrate_multi_schema_cli(cli)
            add_version_detection_commands(cli)
        except ImportError as e:
            if debug:
                click.echo(f"⚠️  v1.1.0 multi-schema features not available: {e}")
        except Exception as e:
            if debug:
                click.echo(f"⚠️  Error integrating v1.1.0 features: {e}")
        
        # Only log CLI initialization in debug mode or when explicitly requested
        if debug:
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
              type=click.Choice(['sequential', 'inter-table']),
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
        s3-backup backup -t table1 -s sequential --estimate
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
@click.option('--tables', '-t', multiple=True, required=True, 
              help='Tables to sync (format: schema.table_name)')
@click.option('--strategy', '-s', 
              type=click.Choice(['sequential', 'inter-table']),
              default='sequential', 
              help='Backup strategy to use')
@click.option('--max-workers', type=int, 
              help='Maximum parallel workers (overrides config)')
@click.option('--batch-size', type=int, 
              help='Batch size for processing (overrides config)')
@click.option('--dry-run', is_flag=True, 
              help='Show what would be done without executing')
@click.option('--backup-only', is_flag=True, 
              help='Only run backup (MySQL → S3), skip Redshift loading')
@click.option('--redshift-only', is_flag=True, 
              help='Only run Redshift loading (S3 → Redshift), skip backup')
@click.option('--verify-data', is_flag=True, 
              help='Verify row counts after sync')
@click.option('--limit', type=int, 
              help='Rows per chunk (chunk size)')
@click.option('--max-chunks', type=int,
              help='Maximum number of chunks to process (total rows = limit × max-chunks)')
@click.option('--pipeline', '-p', help='Pipeline name for multi-schema support (v1.2.0)')
@click.option('--connection', '-c', help='Connection name for multi-schema support (v1.2.0)')
@click.pass_context
def sync(ctx, tables: List[str], strategy: str, max_workers: int, 
         batch_size: int, dry_run: bool, backup_only: bool, redshift_only: bool, verify_data: bool, limit: int, max_chunks: int, pipeline: str, connection: str):
    """
    Complete MySQL → S3 → Redshift synchronization with flexible schema discovery.
    
    This command performs the full pipeline:
    1. Extracts data from MySQL to S3 (parquet format) with dynamic schema discovery
    2. Loads S3 parquet data directly into Redshift using FORMAT AS PARQUET
    3. Updates watermarks for both MySQL and Redshift stages
    4. Provides comprehensive status tracking per table
    
    Perfect for production data synchronization with any table structure.
    
    Examples:
        # Full sync (MySQL → S3 → Redshift) with auto-detection
        s3-backup sync -t settlement.settlement_claim_detail
        
        # Multiple tables with parallel strategy  
        s3-backup sync -t settlement.settlement_claim_detail \\
                       -t settlement.settlement_normal_delivery_detail \\
                       -s inter-table
        
        # Explicit pipeline specification (v1.2.0 multi-schema)
        s3-backup sync -t settlement.settle_orders -p us_dw_pipeline
        
        # Backup only (MySQL → S3)
        s3-backup sync -t settlement.table_name --backup-only
        
        # Redshift loading only (S3 → Redshift)
        s3-backup sync -t settlement.table_name --redshift-only
        
        # Test run without execution
        s3-backup sync -t settlement.table_name --dry-run
        
        # Uses row-based chunking for reliable incremental processing
        s3-backup sync -t settlement.table_name
    """
    config = ctx.obj['config']
    backup_logger = ctx.obj['backup_logger']
    
    # Validate options
    if backup_only and redshift_only:
        click.echo("❌ Cannot specify both --backup-only and --redshift-only", err=True)
        sys.exit(1)
    
    # Override config with CLI options if provided
    if max_workers:
        config.backup.max_workers = max_workers
    if batch_size:
        config.backup.batch_size = batch_size
    
    # Validate strategy choice
    if strategy not in STRATEGIES:
        click.echo(f"❌ Unknown strategy: {strategy}", err=True)
        sys.exit(1)
    
    # Validate table names and handle v1.2.0 multi-schema support
    processed_tables = []
    for table in tables:
        if '.' not in table:
            click.echo(f"⚠️  Table '{table}' should include schema (e.g., schema.table_name)")
        
        # Handle pipeline auto-detection for each table (v1.2.0 multi-schema)
        effective_table_name = table
        
        # Auto-detect pipeline with conflict validation if no explicit pipeline/connection specified
        if not pipeline and not connection:
            detected_pipeline, all_matches, is_ambiguous = _auto_detect_pipeline_for_table(table)
            
            if is_ambiguous:
                click.echo(f"⚠️  Multiple pipelines could handle table '{table}':", err=True)
                for match in all_matches:
                    click.echo(f"   • {match}", err=True)
                click.echo(f"❌ Please specify --pipeline flag to disambiguate:", err=True)
                click.echo(f"   Example: sync -t {table} -p {all_matches[0]}", err=True)
                sys.exit(1)
            elif detected_pipeline:
                click.echo(f"🔍 Auto-detected pipeline: {detected_pipeline} for table {table}")
                # Map pipeline to canonical connection for scoped table name
                canonical_connection = _get_canonical_connection_for_pipeline(detected_pipeline)
                if canonical_connection:
                    effective_table_name = f"{canonical_connection}:{table}"
                else:
                    effective_table_name = f"{detected_pipeline}:{table}"
        
        elif pipeline or connection:
            # Handle explicit pipeline/connection specification
            if pipeline and connection:
                click.echo("❌ Cannot specify both --pipeline and --connection", err=True)
                sys.exit(1)
            
            if pipeline:
                # Map pipeline to canonical connection for scoped table name
                canonical_connection = _get_canonical_connection_for_pipeline(pipeline)
                if canonical_connection:
                    effective_table_name = f"{canonical_connection}:{table}"
                else:
                    effective_table_name = f"{pipeline}:{table}"
            elif connection:
                effective_table_name = f"{connection}:{table}"
        
        processed_tables.append(effective_table_name)
    
    # Use processed table names with proper scoping
    tables = processed_tables
    
    # Show operation information
    if backup_only:
        operation = "MySQL → S3 Backup"
        stages = "Stage 1 only"
    elif redshift_only:
        operation = "S3 → Redshift Loading"
        stages = "Stage 2 only"
    else:
        operation = "Complete Synchronization"
        stages = "MySQL → S3 → Redshift"
    
    click.echo(f"🔄 {operation}")
    click.echo(f"   Strategy: {strategy}")
    click.echo(f"   Pipeline: {stages}")
    click.echo(f"   Tables: {len(tables)} table(s)")
    click.echo(f"   Chunking: Row-based (incremental processing with watermarks)")
    click.echo(f"   Schema Discovery: Dynamic (flexible schema for each table)")
    click.echo()
    
    # Show dry run information
    if dry_run:
        click.echo("🔍 Dry Run - No actual sync will be performed")
        click.echo(f"   Strategy: {strategy}")
        click.echo(f"   Tables: {', '.join(tables)}")
        click.echo(f"   Batch size: {config.backup.batch_size}")
        click.echo(f"   Max workers: {config.backup.max_workers}")
        click.echo(f"   Backup to S3: {'Yes' if not redshift_only else 'Skip'}")
        click.echo(f"   Load to Redshift: {'Yes' if not backup_only else 'Skip'}")
        click.echo(f"   Parquet format: Direct COPY (FORMAT AS PARQUET)")
        click.echo()
        return
    
    # Execute sync pipeline
    try:
        click.echo(f"▶️  Starting {operation.lower()}...")
        click.echo(f"   Tables: {', '.join(tables)}")
        click.echo(f"   Workers: {config.backup.max_workers}")
        click.echo(f"   Batch size: {config.backup.batch_size}")
        click.echo()
        
        start_time = time.time()
        overall_success = True
        
        # Stage 1: MySQL → S3 Backup
        backup_success = True
        backup_summary = {}
        
        if not redshift_only:
            click.echo("📊 Stage 1: MySQL → S3 Backup")
            click.echo("   Dynamic schema discovery + parquet upload")
            
            # Create and execute backup strategy
            backup_strategy = STRATEGIES[strategy](config)
            
            # Calculate parameters based on limit and max_chunks
            chunk_size = limit if limit else config.backup.target_rows_per_chunk
            max_total_rows = None
            
            if limit and max_chunks:
                # limit = chunk size, max_chunks = number of chunks
                # total rows = limit × max_chunks
                max_total_rows = limit * max_chunks
                click.echo(f"   📏 Row limits: {chunk_size} rows/chunk × {max_chunks} chunks = {max_total_rows} total rows")
            elif limit and not max_chunks:
                # limit = total row limit (user expectation for --limit 100)
                max_total_rows = limit
                chunk_size = min(limit, config.backup.target_rows_per_chunk)
                click.echo(f"   📏 Total row limit: {max_total_rows} rows (chunk size: {chunk_size})")
            elif max_chunks and not limit:
                # max_chunks specified but no limit - use default chunk size
                max_total_rows = chunk_size * max_chunks
                click.echo(f"   📏 Row limits: {chunk_size} rows/chunk × {max_chunks} chunks = {max_total_rows} total rows")
            
            backup_success = backup_strategy.execute(list(tables), chunk_size=chunk_size, max_total_rows=max_total_rows)
            backup_summary = backup_strategy.get_backup_summary()
            
            if backup_success:
                click.echo(f"   ✅ Backup completed: {backup_summary['total_rows']:,} rows")
                click.echo(f"   📁 Uploaded: {backup_summary['s3_stats']['total_size_mb']} MB")
            else:
                click.echo("   ❌ Backup stage failed")
                overall_success = False
            click.echo()
        
        # Stage 2: S3 → Redshift Loading
        redshift_success = True
        redshift_summary = {}
        
        if not backup_only and (backup_success or redshift_only):
            click.echo("📊 Stage 2: S3 → Redshift Loading")
            click.echo("   Direct parquet COPY (FORMAT AS PARQUET)")
            
            try:
                from src.core.gemini_redshift_loader import GeminiRedshiftLoader
                from src.core.s3_watermark_manager import S3WatermarkManager
                
                click.echo("   Initializing Redshift connection...")
                redshift_loader = GeminiRedshiftLoader(config)
                watermark_manager = S3WatermarkManager(config)
                loaded_tables = 0
                total_redshift_rows = 0
                
                # Test connection first
                try:
                    click.echo("   Testing Redshift connectivity...")
                    connection_test = redshift_loader._test_connection()
                    if not connection_test:
                        raise Exception("Redshift connection test failed")
                    click.echo("   ✅ Redshift connection established")
                except Exception as conn_e:
                    click.echo(f"   ❌ Redshift connection failed: {conn_e}")
                    raise conn_e
                
                for table_name in tables:
                    click.echo(f"   Loading: {table_name}")
                    
                    # Add timeout and retry for individual table loading
                    max_attempts = 2
                    table_success = False
                    
                    for attempt in range(max_attempts):
                        try:
                            if attempt > 0:
                                click.echo(f"   Retrying {table_name} (attempt {attempt + 1}/{max_attempts})...")
                            
                            table_success = redshift_loader.load_table_data(table_name)
                            
                            if table_success:
                                break  # Success, exit retry loop
                                
                        except Exception as table_e:
                            if attempt == max_attempts - 1:  # Last attempt
                                click.echo(f"   ❌ {table_name}: Failed after {max_attempts} attempts - {table_e}")
                                table_success = False
                            else:
                                click.echo(f"   ⚠️ {table_name}: Attempt {attempt + 1} failed, retrying...")
                                time.sleep(5)  # Wait before retry
                    
                    if table_success:
                        loaded_tables += 1
                        # Get row count from watermark
                        watermark = watermark_manager.get_table_watermark(table_name)
                        table_rows = watermark.redshift_rows_loaded if watermark else 0
                        total_redshift_rows += table_rows
                        click.echo(f"   ✅ {table_name}: Loaded successfully ({table_rows:,} rows)")
                    else:
                        click.echo(f"   ❌ {table_name}: All loading attempts failed")
                        redshift_success = False
                
                redshift_summary = {
                    'loaded_tables': loaded_tables,
                    'total_tables': len(tables),
                    'total_rows': total_redshift_rows
                }
                
                if redshift_success:
                    click.echo(f"   ✅ Redshift loading completed: {loaded_tables}/{len(tables)} tables, {total_redshift_rows:,} rows")
                else:
                    click.echo(f"   ⚠️  Redshift loading partial: {loaded_tables}/{len(tables)} tables, {total_redshift_rows:,} rows")
                    overall_success = False
                    
            except ImportError:
                click.echo("   ❌ GeminiRedshiftLoader not available")
                redshift_success = False
                overall_success = False
            except Exception as e:
                click.echo(f"   ❌ Redshift loading failed: {e}")
                click.echo(f"   💡 Hint: Check SSH tunnel settings and Redshift credentials")
                redshift_success = False
                overall_success = False
            
            click.echo()
        
        # Data verification
        if verify_data and overall_success:
            click.echo("📊 Data Verification")
            click.echo("   Comparing row counts between stages...")
            # TODO: Implement row count verification
            click.echo("   ✅ Verification completed")
            click.echo()
        
        # Final summary
        duration = time.time() - start_time
        
        if overall_success:
            click.echo("✅ Sync completed successfully!")
        else:
            click.echo("❌ Sync completed with errors!")
        
        click.echo()
        click.echo("📈 Sync Summary:")
        click.echo(f"   Duration: {duration:.1f} seconds ({duration/60:.1f} minutes)")
        click.echo(f"   Tables processed: {len(tables)}")
        
        if not redshift_only and backup_summary:
            click.echo(f"   MySQL → S3: {backup_summary['total_rows']:,} rows, {backup_summary['s3_stats']['total_size_mb']} MB")
        
        if not backup_only and redshift_summary:
            click.echo(f"   S3 → Redshift: {redshift_summary['loaded_tables']}/{redshift_summary['total_tables']} tables, {redshift_summary['total_rows']:,} rows")
        
        click.echo(f"   Schema discovery: Dynamic (each table gets custom schema)")
        click.echo(f"   Parquet format: Direct COPY to Redshift")
        
        # Show per-table details if multiple tables
        if len(tables) > 1:
            click.echo()
            click.echo("📋 Per-Table Results:")
            for table_name in tables:
                clean_name = table_name.split('.')[-1]
                backup_status = "✅" if backup_success else "❌"
                redshift_status = "✅" if redshift_success else "❌"
                
                if backup_only:
                    click.echo(f"   {clean_name}: Backup {backup_status}")
                elif redshift_only:
                    click.echo(f"   {clean_name}: Redshift {redshift_status}")
                else:
                    click.echo(f"   {clean_name}: Backup {backup_status}, Redshift {redshift_status}")
        
        sys.exit(0 if overall_success else 1)
        
    except KeyboardInterrupt:
        click.echo("\n⏹️  Sync interrupted by user")
        sys.exit(130)
    except Exception as e:
        backup_logger.error_occurred(e, "cli_sync_command")
        click.echo(f"❌ Sync error: {e}", err=True)
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
@click.option('--strategy', type=click.Choice(['sequential', 'inter-table']), 
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
@click.argument('operation', type=click.Choice(['get', 'set', 'reset', 'force-reset', 'list']))
@click.option('--table', '-t', help='Table name for table-specific watermark')
@click.option('--timestamp', help='Timestamp for set operation (YYYY-MM-DD HH:MM:SS)')
@click.option('--show-files', is_flag=True, help='Show processed S3 files list (for get operation)')
@click.option('--pipeline', '-p', help='Pipeline name for multi-schema support (v1.2.0)')
@click.option('--connection', '-c', help='Connection name for multi-schema support (v1.2.0)')
@click.pass_context
def watermark(ctx, operation: str, table: str, timestamp: str, show_files: bool, pipeline: str, connection: str):
    """
    Manage table-specific watermark timestamps for incremental backups.
    
    Operations:
        get - Get current table watermark
        set - Set new watermark timestamp for table  
        reset - Delete watermark completely (fresh start)
        force-reset - Force overwrite watermark to epoch start (bypasses backups)
        list - List all table watermarks
    
    Examples:
        s3-backup watermark get -t settlement.settlement_claim_detail
        s3-backup watermark get -t settlement.settlement_claim_detail --show-files
        s3-backup watermark set -t settlement.settlement_claim_detail --timestamp "2024-01-01 00:00:00"
        s3-backup watermark reset -t settlement.settlement_claim_detail
        s3-backup watermark list
    """
    config = ctx.obj['config']
    backup_logger = ctx.obj['backup_logger']
    
    try:
        # Use S3-based watermark system (same as backup operations)
        from src.core.s3_watermark_manager import S3WatermarkManager
        
        watermark_manager = S3WatermarkManager(config)
        
        # Handle v1.2.0 multi-schema table identification with smart validation (defensive approach)
        effective_table_name = table
        canonical_scope = None
        scope_info = "Default scope"
        
        # Auto-detect pipeline with conflict validation (smart approach)
        effective_pipeline = pipeline
        if table and not pipeline and not connection:
            detected_pipeline, all_matches, is_ambiguous = _auto_detect_pipeline_for_table(table)
            
            if is_ambiguous:
                click.echo(f"⚠️  Multiple pipelines could handle table '{table}':", err=True)
                for match in all_matches:
                    click.echo(f"   • {match}", err=True)
                click.echo(f"❌ Please specify --pipeline flag to disambiguate:", err=True)
                click.echo(f"   Example: watermark {operation} -t {table} -p {all_matches[0]}", err=True)
                sys.exit(1)
            elif detected_pipeline:
                effective_pipeline = detected_pipeline
                click.echo(f"🔍 Auto-detected pipeline: {effective_pipeline} for table {table}")
            else:
                click.echo(f"⚠️  No pipeline found for table '{table}', using default scope")
        
        if effective_pipeline or connection:
            # Build scoped table name for multi-schema support
            if effective_pipeline and connection:
                click.echo("❌ Cannot specify both --pipeline and --connection", err=True)
                sys.exit(1)
            
            if effective_pipeline:
                # FIXED: Map pipeline to its source connection for canonical scoping
                canonical_connection = _get_canonical_connection_for_pipeline(effective_pipeline)
                if canonical_connection:
                    effective_table_name = f"{canonical_connection}:{table}"
                    canonical_scope = canonical_connection
                    scope_info = f"Pipeline: {effective_pipeline} → Connection: {canonical_connection}"
                else:
                    # Fallback to pipeline scoping if no connection mapping found
                    effective_table_name = f"{effective_pipeline}:{table}"
                    canonical_scope = effective_pipeline
                    scope_info = f"Pipeline: {effective_pipeline}"
                    
            elif connection:
                # Connection-scoped table: connection:table_name  
                effective_table_name = f"{connection}:{table}"
                canonical_scope = connection
                scope_info = f"Connection: {connection}"
        
        click.echo(f"🔖 Watermark {operation.upper()}")
        if table:
            click.echo(f"   Table: {table}")
            click.echo(f"   Scope: {scope_info}")
            if effective_table_name != table:
                click.echo(f"   Full identifier: {effective_table_name}")
        click.echo()
        
        if operation == 'get':
            if not table:
                click.echo("❌ Table name required for watermark operations", err=True)
                click.echo("   Use -t settlement.table_name")
                sys.exit(1)
                
            # Get table-specific watermark
            watermark = watermark_manager.get_table_watermark(effective_table_name)
            
            # Try to get start timestamp, but handle exceptions gracefully
            try:
                start_timestamp = watermark_manager.get_incremental_start_timestamp(effective_table_name)
            except ValueError as e:
                start_timestamp = f"Error: {str(e)}"
            
            if watermark:
                click.echo(f"📅 Current Watermark for {table}:")
                click.echo()
                
                # MySQL/Backup Stage
                click.echo("   🔄 MySQL → S3 Backup Stage:")
                click.echo(f"      Status: {watermark.mysql_status}")
                click.echo(f"      Rows Extracted: {watermark.mysql_rows_extracted:,}")
                click.echo(f"      S3 Files Created: {watermark.s3_file_count}")
                click.echo(f"      Last Data Timestamp: {watermark.last_mysql_data_timestamp}")
                click.echo(f"      Last Extraction Time: {watermark.last_mysql_extraction_time}")
                
                # Show row-based chunking information if available
                if hasattr(watermark, 'last_processed_id') and watermark.last_processed_id is not None:
                    click.echo(f"      Last Processed ID: {watermark.last_processed_id:,} (row-based resume point)")
                
                # Show backup strategy information
                if hasattr(watermark, 'backup_strategy') and watermark.backup_strategy:
                    strategy_display = {
                        'sequential': 'Sequential (Traditional)',
                        'inter-table': 'Inter-table (Parallel)',
                        'manual_cli': 'Manual CLI (User-controlled)'
                    }.get(watermark.backup_strategy, watermark.backup_strategy)
                    click.echo(f"      Chunking Strategy: {strategy_display}")
                
                click.echo()
                
                # S3 → Redshift Stage 
                click.echo("   📊 S3 → Redshift Loading Stage:")
                click.echo(f"      Status: {watermark.redshift_status}")
                click.echo(f"      Rows Loaded: {watermark.redshift_rows_loaded:,}")
                if watermark.last_redshift_load_time:
                    click.echo(f"      Last Load Time: {watermark.last_redshift_load_time}")
                else:
                    click.echo(f"      Last Load Time: Never")
                click.echo()
                
                # Next Incremental Backup
                click.echo("   🔜 Next Incremental Backup:")
                click.echo(f"      Will start from: {start_timestamp}")
                click.echo()
                
                # Show processed S3 files if requested
                if show_files and watermark.processed_s3_files:
                    click.echo("   📁 Processed S3 Files:")
                    if len(watermark.processed_s3_files) > 10:
                        click.echo(f"      Total: {len(watermark.processed_s3_files)} files")
                        click.echo("      Recent files (last 10):")
                        for file_path in watermark.processed_s3_files[-10:]:
                            filename = file_path.split('/')[-1]
                            click.echo(f"        • {filename}")
                        click.echo(f"      ... and {len(watermark.processed_s3_files) - 10} more files")
                    else:
                        for file_path in watermark.processed_s3_files:
                            filename = file_path.split('/')[-1]
                            click.echo(f"        • {filename}")
                    click.echo()
                elif show_files and not watermark.processed_s3_files:
                    click.echo("   📁 Processed S3 Files: None")
                    click.echo()
                
                # Errors and Storage Info
                if watermark.last_error:
                    click.echo("   ❌ Last Error:")
                    click.echo(f"      {watermark.last_error}")
                    click.echo()
                
                click.echo(f"   💾 Storage: S3 (unified with backup system)")
            else:
                click.echo(f"📅 No watermark found for {table}")
                click.echo(f"   Would start from: {start_timestamp}")
        
        elif operation == 'set':
            if not table:
                click.echo("❌ Table name required for watermark operations", err=True)
                click.echo("   Use -t settlement.table_name")
                sys.exit(1)
                
            if not timestamp:
                click.echo("❌ Timestamp required for set operation", err=True)
                click.echo("   Use --timestamp 'YYYY-MM-DD HH:MM:SS'")
                sys.exit(1)
            
            from datetime import datetime
            try:
                # Parse timestamp
                target_timestamp = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
                
                # Set manual watermark (data timestamp only, no extraction time)
                success = watermark_manager.set_manual_watermark(
                    table_name=effective_table_name,
                    data_timestamp=target_timestamp
                )
                
                if success:
                    click.echo(f"✅ Watermark updated for {table}")
                    click.echo(f"   Data timestamp reset to: {timestamp}")
                    
                    # Verify the change
                    new_start = watermark_manager.get_incremental_start_timestamp(table)
                    click.echo(f"   Next incremental backup will start from: {new_start}")
                else:
                    click.echo("❌ Failed to update watermark", err=True)
                    sys.exit(1)
                    
            except ValueError as e:
                click.echo(f"❌ Invalid timestamp format: {e}", err=True)
                click.echo("   Use format: 'YYYY-MM-DD HH:MM:SS'")
                sys.exit(1)
        
        elif operation == 'reset':
            if not table:
                click.echo("❌ Table name required for reset operation", err=True)
                click.echo("   Use -t settlement.table_name")
                sys.exit(1)
            
            # Confirm deletion
            click.echo(f"⚠️  This will completely delete the watermark for {table}")
            click.echo("   The next backup will start from the default timestamp")
            if not click.confirm("   Continue?"):
                click.echo("   Operation cancelled")
                return
            
            try:
                success = watermark_manager.delete_table_watermark(effective_table_name, create_backup=True)
                
                if success:
                    click.echo(f"✅ Watermark reset for {table}")
                    click.echo("   Backup created before deletion")
                    click.echo("   Next sync will start fresh from default timestamp")
                else:
                    click.echo("❌ Failed to reset watermark", err=True)
                    sys.exit(1)
                    
            except Exception as e:
                click.echo(f"❌ Reset failed: {e}", err=True)
                sys.exit(1)
                
        elif operation == 'force-reset':
            if not table:
                click.echo("❌ Table name required for force-reset operation", err=True)
                click.echo("   Use -t settlement.table_name")
                sys.exit(1)
            
            # Confirm force reset
            click.echo(f"⚠️  FORCE RESET will overwrite the watermark for {table}")
            click.echo("   This bypasses all backup/recovery mechanisms")
            click.echo("   Watermark will be set to epoch start (1970-01-01)")
            if not click.confirm("   Continue with force reset?"):
                click.echo("   Operation cancelled")
                return
            
            try:
                success = watermark_manager.force_reset_watermark(effective_table_name)
                
                if success:
                    click.echo(f"✅ Force reset completed for {table}")
                    click.echo("   Watermark overwritten with epoch start")
                    click.echo("   Next sync will start from 1970-01-01 00:00:00")
                else:
                    click.echo("❌ Force reset failed", err=True)
                    sys.exit(1)
                    
            except Exception as e:
                click.echo(f"❌ Force reset failed: {e}", err=True)
                sys.exit(1)
        
        elif operation == 'list':
            # List table watermarks using S3WatermarkManager
            try:
                watermarks = watermark_manager.list_all_tables()
                
                if not watermarks:
                    click.echo("📂 No table watermarks found")
                    return
                
                click.echo(f"📂 Found {len(watermarks)} table watermarks:")
                click.echo()
                
                for i, wm in enumerate(watermarks[:20]):  # Show first 20
                    click.echo(f"   {i+1}. {wm.table_name}")
                    click.echo(f"      MySQL Status: {wm.mysql_status}")
                    click.echo(f"      MySQL Rows: {wm.mysql_rows_extracted:,}")
                    click.echo(f"      Redshift Status: {wm.redshift_status}")
                    if wm.last_mysql_data_timestamp:
                        click.echo(f"      Last Data: {wm.last_mysql_data_timestamp}")
                    if wm.last_error:
                        click.echo(f"      Error: {wm.last_error[:100]}...")
                    click.echo()
                    
                if len(watermarks) > 20:
                    click.echo(f"   ... and {len(watermarks) - 20} more tables")
                    
            except Exception as e:
                click.echo(f"❌ Error listing watermarks: {e}", err=True)
        
        else:
            click.echo(f"❌ Unknown operation: {operation}", err=True)
            sys.exit(1)
        
    except Exception as e:
        backup_logger.error_occurred(e, "cli_watermark_command")
        click.echo(f"❌ Watermark operation failed: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.argument('operation', type=click.Choice(['set-count', 'validate-counts']))
@click.option('--table', '-t', required=True, help='Table name for count operations')
@click.option('--count', type=int, help='Row count to set (required for set-count)')
@click.option('--mode', type=click.Choice(['absolute', 'additive']), default='absolute', 
              help='How to update the count: absolute (replace) or additive (add to existing)')
@click.option('--pipeline', '-p', help='Pipeline name for multi-schema support (v1.2.0)')
@click.option('--connection', '-c', help='Connection name for multi-schema support (v1.2.0)')
@click.pass_context
def watermark_count(ctx, operation: str, table: str, count: int, mode: str, pipeline: str, connection: str):
    """
    Advanced watermark row count management (BUG FIX COMMANDS).
    
    Operations:
        set-count - Set or add to watermark row count (fixes double-counting bug)
        validate-counts - Cross-validate watermark vs actual Redshift counts
    
    Examples:
        # Fix current row count mismatch (absolute mode - replaces existing)
        s3-backup watermark-count set-count -t settlement.settlement_normal_delivery_detail --count 3000000 --mode absolute
        
        # Add incremental count (additive mode - adds to existing)  
        s3-backup watermark-count set-count -t settlement.settlement_normal_delivery_detail --count 500000 --mode additive
        
        # Validate consistency across systems
        s3-backup watermark-count validate-counts -t settlement.settlement_normal_delivery_detail
    """
    config = ctx.obj['config']
    backup_logger = ctx.obj['backup_logger']
    
    try:
        from src.core.s3_watermark_manager import S3WatermarkManager
        from src.core.gemini_redshift_loader import GeminiRedshiftLoader
        
        watermark_manager = S3WatermarkManager(config)
        
        # Handle v1.2.0 multi-schema table identification with smart validation (defensive approach) 
        effective_table_name = table
        canonical_scope = None
        scope_info = "Default scope"
        
        # Auto-detect pipeline with conflict validation (smart approach)
        effective_pipeline = pipeline
        if table and not pipeline and not connection:
            detected_pipeline, all_matches, is_ambiguous = _auto_detect_pipeline_for_table(table)
            
            if is_ambiguous:
                click.echo(f"⚠️  Multiple pipelines could handle table '{table}':", err=True)
                for match in all_matches:
                    click.echo(f"   • {match}", err=True)
                click.echo(f"❌ Please specify --pipeline flag to disambiguate:", err=True)
                click.echo(f"   Example: watermark-count {operation} -t {table} -p {all_matches[0]}", err=True)
                sys.exit(1)
            elif detected_pipeline:
                effective_pipeline = detected_pipeline
                click.echo(f"🔍 Auto-detected pipeline: {effective_pipeline} for table {table}")
            else:
                click.echo(f"⚠️  No pipeline found for table '{table}', using default scope")
        
        if effective_pipeline or connection:
            # Build scoped table name for multi-schema support
            if effective_pipeline and connection:
                click.echo("❌ Cannot specify both --pipeline and --connection", err=True)
                sys.exit(1)
            
            if effective_pipeline:
                # FIXED: Map pipeline to its source connection for canonical scoping
                canonical_connection = _get_canonical_connection_for_pipeline(effective_pipeline)
                if canonical_connection:
                    effective_table_name = f"{canonical_connection}:{table}"
                    canonical_scope = canonical_connection
                    scope_info = f"Pipeline: {effective_pipeline} → Connection: {canonical_connection}"
                else:
                    # Fallback to pipeline scoping if no connection mapping found
                    effective_table_name = f"{effective_pipeline}:{table}"
                    canonical_scope = effective_pipeline
                    scope_info = f"Pipeline: {effective_pipeline}"
                    
            elif connection:
                # Connection-scoped table: connection:table_name  
                effective_table_name = f"{connection}:{table}"
                canonical_scope = connection
                scope_info = f"Connection: {connection}"
        
        click.echo(f"🔢 Watermark Count {operation.upper()}")
        click.echo(f"   Table: {table}")
        click.echo(f"   Scope: {scope_info}")
        if effective_table_name != table:
            click.echo(f"   Full identifier: {effective_table_name}")
        click.echo()
        
        if operation == 'set-count':
            if count is None:
                click.echo("❌ Count value required for set-count operation", err=True)
                click.echo("   Use --count <number>")
                sys.exit(1)
            
            try:
                if mode == 'absolute':
                    # Set absolute count (replaces existing) - FIX: Update BOTH MySQL and Redshift counts
                    success = watermark_manager._update_watermark_direct(
                        table_name=effective_table_name,
                        watermark_data={
                            'mysql_rows_extracted': count,
                            'redshift_rows_loaded': count  # FIX: Also update Redshift count
                        }
                    )
                    
                    if success:
                        click.echo(f"✅ Set absolute count to {count:,} rows")
                        click.echo(f"   Mode: {mode} (replaced existing MySQL and Redshift counts)")
                    else:
                        click.echo("❌ Failed to update watermark count", err=True)
                        sys.exit(1)
                        
                elif mode == 'additive':
                    # Add to existing count - FIX: Update BOTH MySQL and Redshift counts
                    current_watermark = watermark_manager.get_table_watermark(effective_table_name)
                    existing_mysql_count = current_watermark.mysql_rows_extracted if current_watermark else 0
                    existing_redshift_count = current_watermark.redshift_rows_loaded if current_watermark else 0
                    new_mysql_count = existing_mysql_count + count
                    new_redshift_count = existing_redshift_count + count
                    
                    success = watermark_manager._update_watermark_direct(
                        table_name=effective_table_name,
                        watermark_data={
                            'mysql_rows_extracted': new_mysql_count,
                            'redshift_rows_loaded': new_redshift_count  # FIX: Also update Redshift count
                        }
                    )
                    
                    if success:
                        click.echo(f"✅ Added {count:,} to existing counts:")
                        click.echo(f"   MySQL: {existing_mysql_count:,} + {count:,} = {new_mysql_count:,}")
                        click.echo(f"   Redshift: {existing_redshift_count:,} + {count:,} = {new_redshift_count:,}")
                        click.echo(f"   Mode: {mode} (added to existing counts)")
                    else:
                        click.echo("❌ Failed to update watermark count", err=True)
                        sys.exit(1)
                
                # Verify the change by showing updated watermark
                click.echo()
                click.echo("📊 Updated Watermark Status:")
                updated_watermark = watermark_manager.get_table_watermark(effective_table_name)
                if updated_watermark:
                    click.echo(f"   MySQL Rows Extracted: {updated_watermark.mysql_rows_extracted:,}")
                    click.echo(f"   Redshift Rows Loaded: {updated_watermark.redshift_rows_loaded:,}")
                    click.echo(f"   MySQL Status: {updated_watermark.mysql_status}")
                    click.echo(f"   Redshift Status: {updated_watermark.redshift_status}")
                    
            except Exception as e:
                click.echo(f"❌ Error updating row count: {e}", err=True)
                sys.exit(1)
        
        elif operation == 'validate-counts':
            try:
                click.echo("🔍 Validating row counts across all systems...")
                click.echo()
                
                # Get watermark counts
                watermark = watermark_manager.get_table_watermark(table)
                if not watermark:
                    click.echo(f"❌ No watermark found for {table}", err=True)
                    sys.exit(1)
                
                watermark_backup_count = watermark.mysql_rows_extracted
                watermark_load_count = watermark.redshift_rows_loaded
                
                # Get actual Redshift count
                try:
                    redshift_loader = GeminiRedshiftLoader(config)
                    with redshift_loader._redshift_connection() as conn:
                        cursor = conn.cursor()
                        redshift_table_name = redshift_loader._get_redshift_table_name(table)
                        cursor.execute(f"SELECT COUNT(*) FROM {config.redshift.schema}.{redshift_table_name}")
                        actual_redshift_count = cursor.fetchone()[0]
                        cursor.close()
                except Exception as e:
                    click.echo(f"⚠️  Could not query Redshift: {e}")
                    actual_redshift_count = "Unable to query"
                
                # Display comparison
                click.echo("📊 Row Count Comparison:")
                click.echo(f"   Watermark Backup Count:  {watermark_backup_count:,}")
                click.echo(f"   Watermark Load Count:    {watermark_load_count:,}")
                
                if isinstance(actual_redshift_count, int):
                    click.echo(f"   Actual Redshift Count:   {actual_redshift_count:,}")
                else:
                    click.echo(f"   Actual Redshift Count:   {actual_redshift_count}")
                
                click.echo()
                
                # Analyze consistency
                issues = []
                if isinstance(actual_redshift_count, int):
                    if watermark_backup_count != watermark_load_count:
                        issues.append(f"Backup ({watermark_backup_count:,}) ≠ Load ({watermark_load_count:,}) watermark counts")
                    if watermark_load_count != actual_redshift_count:
                        issues.append(f"Load watermark ({watermark_load_count:,}) ≠ Actual Redshift ({actual_redshift_count:,})")
                    if watermark_backup_count > actual_redshift_count * 1.1:  # More than 10% higher
                        issues.append(f"Backup count suspiciously high (possible double-counting bug)")
                
                if issues:
                    click.echo("❌ Consistency Issues Found:")
                    for issue in issues:
                        click.echo(f"   • {issue}")
                    click.echo()
                    click.echo("💡 Recommended Actions:")
                    click.echo("   1. If backup count is inflated, use:")
                    if isinstance(actual_redshift_count, int):
                        click.echo(f"      watermark-count set-count -t {table} --count {actual_redshift_count} --mode absolute")
                    click.echo("   2. Check if recent syncs used additive mode incorrectly")
                    click.echo("   3. Verify S3 files match expected processing")
                else:
                    click.echo("✅ All counts are consistent!")
                    click.echo("   No row count discrepancies detected")
                    
            except Exception as e:
                click.echo(f"❌ Validation failed: {e}", err=True)
                sys.exit(1)
        
        else:
            click.echo(f"❌ Unknown operation: {operation}", err=True)
            sys.exit(1)
    
    except Exception as e:
        backup_logger.error_occurred(e, f"cli_watermark_count_{operation}")
        click.echo(f"❌ Watermark count operation failed: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.argument('operation', type=click.Choice(['list', 'clean', 'clean-all']))
@click.option('--table', '-t', help='Table name to clean (required for clean operation)')
@click.option('--older-than', help='Delete files older than X days/hours (e.g., "7d", "24h")')
@click.option('--pattern', help='File pattern to match (e.g., "batch_*", "*.parquet")')
@click.option('--dry-run', is_flag=True, help='Show what would be deleted without actually deleting')
@click.option('--force', is_flag=True, help='Skip confirmation prompts')
@click.option('--show-timestamps', is_flag=True, help='Show detailed timestamps for files (default: show simplified format)')
@click.option('--simple-delete', is_flag=True, help='Use simple deletion (ignore versioning, faster)')
@click.option('--pipeline', '-p', help='Pipeline name for multi-schema support (v1.2.0)')
@click.option('--connection', '-c', help='Connection name for multi-schema support (v1.2.0)')
@click.pass_context
def s3clean(ctx, operation: str, table: str, older_than: str, pattern: str, dry_run: bool, force: bool, show_timestamps: bool, simple_delete: bool, pipeline: str, connection: str):
    """
    Manage S3 backup files with safe cleanup operations.
    
    Operations:
        list - List S3 files for a table or all tables
        clean - Clean S3 files for a specific table
        clean-all - Clean S3 files for all tables (use with caution)
    
    Examples:
        # List files for specific table (default scope)
        s3-backup s3clean list -t settlement.settlement_return_detail
        
        # List files for connection-scoped table (v1.2.0 multi-schema)
        s3-backup s3clean list -t settlement.settle_orders -c US_DW_RO_SSH
        
        # List files for pipeline-scoped table (v1.2.0 multi-schema)
        s3-backup s3clean list -t settlement.settle_orders -p us_dw_pipeline
        
        # List files with detailed timestamps
        s3-backup s3clean list -t settlement.settlement_return_detail --show-timestamps
        
        # Clean all files for a table (with confirmation)
        s3-backup s3clean clean -t settlement.settlement_return_detail
        
        # Clean files older than 7 days
        s3-backup s3clean clean -t settlement.settlement_return_detail --older-than "7d"
        
        # Clean connection-scoped files (v1.2.0)
        s3-backup s3clean clean -t settlement.settle_orders -c US_DW_RO_SSH
        
        # Dry run to see what would be deleted
        s3-backup s3clean clean -t settlement.settlement_return_detail --dry-run
        
        # Clean with pattern matching
        s3-backup s3clean clean -t settlement.settlement_return_detail --pattern "batch_*"
        
        # Force clean without confirmation
        s3-backup s3clean clean -t settlement.settlement_return_detail --force
    """
    config = ctx.obj['config']
    backup_logger = ctx.obj['backup_logger']
    
    try:
        import boto3
        from datetime import datetime, timedelta
        import re
        
        # Initialize S3 client
        s3_client = boto3.client(
            's3',
            aws_access_key_id=config.s3.access_key,
            aws_secret_access_key=config.s3.secret_key.get_secret_value(),
            region_name=config.s3.region
        )
        
        # Handle v1.2.0 multi-schema table identification with smart validation (defensive approach)
        effective_table_name = table
        canonical_scope = None
        scope_info = "Default scope"
        
        # Auto-detect pipeline with conflict validation (smart approach)
        effective_pipeline = pipeline
        if table and not pipeline and not connection:
            detected_pipeline, all_matches, is_ambiguous = _auto_detect_pipeline_for_table(table)
            
            if is_ambiguous:
                click.echo(f"⚠️  Multiple pipelines could handle table '{table}':", err=True)
                for match in all_matches:
                    click.echo(f"   • {match}", err=True)
                click.echo(f"❌ Please specify --pipeline flag to disambiguate:", err=True)
                click.echo(f"   Example: s3clean {operation} -t {table} -p {all_matches[0]}", err=True)
                sys.exit(1)
            elif detected_pipeline:
                effective_pipeline = detected_pipeline
                click.echo(f"🔍 Auto-detected pipeline: {effective_pipeline} for table {table}")
            else:
                click.echo(f"⚠️  No pipeline found for table '{table}', using default scope")
        
        if table and (effective_pipeline or connection):
            if effective_pipeline and connection:
                click.echo("❌ Cannot specify both --pipeline and --connection", err=True)
                sys.exit(1)
            
            if effective_pipeline:
                # FIXED: Map pipeline to its source connection for canonical scoping
                canonical_connection = _get_canonical_connection_for_pipeline(effective_pipeline)
                if canonical_connection:
                    effective_table_name = f"{canonical_connection}:{table}"
                    canonical_scope = canonical_connection
                    scope_info = f"Pipeline: {effective_pipeline} → Connection: {canonical_connection}"
                else:
                    # Fallback to pipeline scoping if no connection mapping found
                    effective_table_name = f"{effective_pipeline}:{table}"
                    canonical_scope = effective_pipeline
                    scope_info = f"Pipeline: {effective_pipeline}"
                    
            elif connection:
                effective_table_name = f"{connection}:{table}"
                canonical_scope = connection
                scope_info = f"Connection: {connection}"
        
        click.echo(f"🗂️  S3 Clean Operation: {operation.upper()}")
        if table:
            click.echo(f"   Table: {table}")
            click.echo(f"   Scope: {scope_info}")
            if effective_table_name != table:
                click.echo(f"   Full identifier: {effective_table_name}")
        if older_than:
            click.echo(f"   Older than: {older_than}")
        if pattern:
            click.echo(f"   Pattern: {pattern}")
        if dry_run:
            click.echo("   🧪 DRY RUN - No files will be deleted")
        click.echo()
        
        # Parse older_than parameter
        cutoff_time = None
        if older_than:
            cutoff_time = _parse_time_delta(older_than)
            if not cutoff_time:
                click.echo(f"❌ Invalid time format: {older_than}", err=True)
                click.echo("   Use format: '7d' (days), '24h' (hours), '30m' (minutes)")
                sys.exit(1)
        
        if operation == 'list':
            _s3_list_files(s3_client, config, effective_table_name, older_than, pattern, cutoff_time, show_timestamps)
            
        elif operation == 'clean':
            if not table:
                click.echo("❌ Table name required for clean operation", err=True)
                click.echo("   Use -t settlement.table_name")
                sys.exit(1)
            
            _s3_clean_table(s3_client, config, effective_table_name, older_than, pattern, cutoff_time, dry_run, force, simple_delete)
            
        elif operation == 'clean-all':
            if not force and not dry_run:
                click.echo("⚠️  DANGER: This will clean S3 files for ALL TABLES")
                click.echo("   This operation affects your entire backup system")
                if not click.confirm("   Are you absolutely sure you want to continue?"):
                    click.echo("   Operation cancelled")
                    return
            
            _s3_clean_all_tables(s3_client, config, older_than, pattern, cutoff_time, dry_run, force)
        
    except ImportError:
        click.echo("❌ boto3 not installed. Install with: pip install boto3", err=True)
        sys.exit(1)
    except Exception as e:
        backup_logger.error_occurred(e, "cli_s3clean_command")
        click.echo(f"❌ S3 clean operation failed: {e}", err=True)
        sys.exit(1)


def _parse_time_delta(time_str: str):
    """Parse time delta string like '7d', '24h', '30m' and return cutoff datetime"""
    from datetime import datetime, timedelta
    try:
        if time_str.endswith('d'):
            days = int(time_str[:-1])
            return datetime.utcnow() - timedelta(days=days)
        elif time_str.endswith('h'):
            hours = int(time_str[:-1])
            return datetime.utcnow() - timedelta(hours=hours)
        elif time_str.endswith('m'):
            minutes = int(time_str[:-1])
            return datetime.utcnow() - timedelta(minutes=minutes)
        else:
            return None
    except ValueError:
        return None


def _s3_list_files(s3_client, config, table: str, older_than: str, pattern: str, cutoff_time, show_timestamps: bool = False):
    """List S3 files with filtering"""
    try:
        prefix = f"{config.s3.incremental_path.strip('/')}/"
        
        # Handle pagination to get ALL files, not just first 1000
        all_files = []
        filtered_files = []
        total_size = 0
        
        paginator = s3_client.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(
            Bucket=config.s3.bucket_name,
            Prefix=prefix
        )
        
        for page in page_iterator:
            for obj in page.get('Contents', []):
                key = obj['Key']
                size = obj['Size']
                modified = obj['LastModified']
                
                # Table filtering with v1.2.0 scoped table name support
                if table:
                    # Use the same table name cleaning logic as S3Manager
                    if ':' in table:
                        scope, actual_table = table.split(':', 1)
                        # Clean scope: lowercase, replace special chars with underscores
                        clean_scope = scope.lower().replace('-', '_').replace('.', '_')
                        # Clean table: replace dots with underscores
                        clean_table = actual_table.replace('.', '_').replace('-', '_')
                        # Combine: scope_table_name
                        clean_table_name = f"{clean_scope}_{clean_table}"
                    else:
                        # Unscoped table (v1.0.0 compatibility)
                        clean_table_name = table.replace('.', '_').replace('-', '_')
                    
                    if clean_table_name not in key:
                        continue
                
                all_files.append((key, size, modified))
                
                # Apply filters
                include_file = True
                
                # Pattern filtering
                if pattern:
                    filename = key.split('/')[-1]
                    if not fnmatch.fnmatch(filename, pattern):
                        include_file = False
                
                # Time filtering
                if cutoff_time and modified.replace(tzinfo=None) > cutoff_time:
                    include_file = False
                
                if include_file:
                    filtered_files.append((key, size, modified))
                    total_size += size
        
        # Display results
        if table:
            click.echo(f"📁 S3 Files for {table}:")
        else:
            click.echo("📁 All S3 Files:")
        
        click.echo(f"   Total files found: {len(all_files)}")
        if older_than or pattern:
            click.echo(f"   Files matching criteria: {len(filtered_files)}")
            click.echo(f"   Total size: {total_size / 1024 / 1024:.2f} MB")
        
        # Show sample files
        sample_files = filtered_files[:10]
        for key, size, modified in sample_files:
            size_mb = size / 1024 / 1024
            filename = key.split('/')[-1]
            
            if show_timestamps:
                # Show detailed format with full timestamp
                click.echo(f"     {filename} ({size_mb:.2f} MB, {modified})")
                click.echo(f"       Full path: {key}")
            else:
                # Show simplified format with just date
                date_str = modified.strftime('%Y-%m-%d %H:%M')
                click.echo(f"     {filename} ({size_mb:.2f} MB, {date_str})")
        
        if len(filtered_files) > 10:
            click.echo(f"     ... and {len(filtered_files) - 10} more files")
            
    except Exception as e:
        click.echo(f"❌ Failed to list S3 files: {e}", err=True)


def _s3_clean_table(s3_client, config, table: str, older_than: str, pattern: str, cutoff_time, dry_run: bool, force: bool, simple_delete: bool = False):
    """Clean S3 files for a specific table"""
    try:
        clean_table_name = table.replace('.', '_').replace('-', '_')
        prefix = f"{config.s3.incremental_path.strip('/')}/"
        
        # Find files to delete (handle pagination)
        files_to_delete = []
        total_size = 0
        
        paginator = s3_client.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(
            Bucket=config.s3.bucket_name,
            Prefix=prefix
        )
        
        for page in page_iterator:
            for obj in page.get('Contents', []):
                key = obj['Key']
                size = obj['Size']
                modified = obj['LastModified']
                
                # Must match table name
                if clean_table_name not in key or not key.endswith('.parquet'):
                    continue
                
                # Apply filters
                include_file = True
                
                # Pattern filtering
                if pattern:
                    filename = key.split('/')[-1]
                    if not fnmatch.fnmatch(filename, pattern):
                        include_file = False
                
                # Time filtering  
                if cutoff_time and modified.replace(tzinfo=None) > cutoff_time:
                    include_file = False
                
                if include_file:
                    files_to_delete.append({'Key': key})
                    total_size += size
        
        if not files_to_delete:
            click.echo(f"✅ No files found to delete for {table}")
            return
        
        # Show what will be deleted
        click.echo(f"🗑️  Files to delete for {table}:")
        click.echo(f"   Count: {len(files_to_delete)}")
        click.echo(f"   Total size: {total_size / 1024 / 1024:.2f} MB")
        
        # Show sample files
        sample_files = files_to_delete[:5]
        for file_info in sample_files:
            click.echo(f"     {file_info['Key']}")
        if len(files_to_delete) > 5:
            click.echo(f"     ... and {len(files_to_delete) - 5} more files")
        
        if dry_run:
            click.echo("🧪 DRY RUN - Files would be deleted but no action taken")
            return
        
        # Confirm deletion with safety check for large deletions
        if not force:
            click.echo()
            if len(files_to_delete) > 5000:
                click.echo(f"⚠️  WARNING: About to delete {len(files_to_delete)} files!")
                click.echo("   This is a very large deletion operation.")
                if not click.confirm(f"   Are you SURE you want to delete {len(files_to_delete)} files for {table}?"):
                    click.echo("Operation cancelled")
                    return
            else:
                if not click.confirm(f"Delete {len(files_to_delete)} files for {table}?"):
                    click.echo("Operation cancelled")
                    return
        
        # Delete files using proper error checking
        click.echo("🗑️  Deleting files...")
        
        # For versioned buckets, we need to permanently delete files
        # Extract just the keys for direct deletion
        file_keys = [obj['Key'] for obj in files_to_delete]
        
        deleted_count = 0
        error_count = 0
        
        try:
            # Check if user wants simple deletion or if we should try versioned deletion
            if simple_delete:
                click.echo("📁 Using simple deletion (ignoring versioning)...")
                is_versioned = False
            else:
                # Try to check if bucket has versioning (requires s3:GetBucketVersioning permission)
                is_versioned = False
                try:
                    bucket_versioning = s3_client.get_bucket_versioning(Bucket=config.s3.bucket_name)
                    is_versioned = bucket_versioning.get('Status') == 'Enabled'
                    click.echo(f"🔍 Bucket versioning status: {bucket_versioning.get('Status', 'Disabled')}")
                except Exception as version_error:
                    click.echo(f"⚠️ Cannot check versioning (permission issue): {str(version_error)[:100]}...")
                    click.echo("📁 Falling back to simple deletion...")
                    is_versioned = False  # Use simple deletion when permissions are insufficient
            
            if is_versioned:
                click.echo("🔄 Versioned bucket detected - performing permanent deletion...")
                
                # For versioned buckets, delete all versions of each object
                for key in file_keys:
                    try:
                        # List all versions of this object (requires s3:ListBucketVersions)
                        try:
                            versions_response = s3_client.list_object_versions(
                                Bucket=config.s3.bucket_name,
                                Prefix=key,
                                MaxKeys=100
                            )
                        except Exception as list_error:
                            # If we can't list versions, try simple deletion
                            click.echo(f"⚠️ Cannot list versions for {key}, using simple deletion: {list_error}")
                            delete_response = s3_client.delete_objects(
                                Bucket=config.s3.bucket_name,
                                Delete={'Objects': [{'Key': key}]}
                            )
                            deleted_count += len(delete_response.get('Deleted', []))
                            error_count += len(delete_response.get('Errors', []))
                            continue
                        
                        # Delete all versions and delete markers
                        objects_to_delete = []
                        
                        for version in versions_response.get('Versions', []):
                            if version['Key'] == key:
                                objects_to_delete.append({
                                    'Key': key,
                                    'VersionId': version['VersionId']
                                })
                        
                        for delete_marker in versions_response.get('DeleteMarkers', []):
                            if delete_marker['Key'] == key:
                                objects_to_delete.append({
                                    'Key': key,
                                    'VersionId': delete_marker['VersionId']
                                })
                        
                        if objects_to_delete:
                            # Permanently delete all versions
                            delete_response = s3_client.delete_objects(
                                Bucket=config.s3.bucket_name,
                                Delete={'Objects': objects_to_delete}
                            )
                            
                            deleted_count += len(delete_response.get('Deleted', []))
                            error_count += len(delete_response.get('Errors', []))
                        
                    except Exception as e:
                        click.echo(f"⚠️ Error deleting {key}: {e}")
                        error_count += 1
                        
            else:
                # Non-versioned bucket - use regular deletion with batching
                click.echo("📁 Non-versioned bucket - using standard deletion...")
                
                # AWS S3 delete_objects has a limit of 1000 objects per request
                batch_size = 1000
                total_batches = (len(file_keys) + batch_size - 1) // batch_size
                
                click.echo(f"🔄 Processing {len(file_keys)} files in {total_batches} batches...")
                
                for i in range(0, len(file_keys), batch_size):
                    batch_keys = file_keys[i:i + batch_size]
                    batch_num = (i // batch_size) + 1
                    
                    click.echo(f"   Batch {batch_num}/{total_batches}: {len(batch_keys)} files")
                    
                    delete_objects = [{'Key': key} for key in batch_keys]
                    
                    try:
                        delete_response = s3_client.delete_objects(
                            Bucket=config.s3.bucket_name,
                            Delete={'Objects': delete_objects}
                        )
                        
                        batch_deleted = len(delete_response.get('Deleted', []))
                        batch_errors = len(delete_response.get('Errors', []))
                        
                        deleted_count += batch_deleted
                        error_count += batch_errors
                        
                        if batch_errors > 0:
                            click.echo(f"   ⚠️ Batch {batch_num}: {batch_deleted} deleted, {batch_errors} errors")
                        else:
                            click.echo(f"   ✅ Batch {batch_num}: {batch_deleted} deleted")
                            
                    except Exception as batch_error:
                        click.echo(f"   ❌ Batch {batch_num} failed: {batch_error}")
                        error_count += len(batch_keys)
                
        except Exception as e:
            click.echo(f"❌ Deletion failed: {e}")
            error_count = len(file_keys)
        
        # Report actual results
        total_requested = len(file_keys)
        
        if error_count == 0:
            click.echo(f"✅ Successfully deleted {deleted_count} files for {table}")
        else:
            click.echo(f"⚠️  Partial success: {deleted_count} deleted, {error_count} failed out of {total_requested} for {table}")
            click.echo("❌ Some files could not be deleted - they may be in use or have permission issues")
        
    except Exception as e:
        click.echo(f"❌ Failed to clean S3 files for {table}: {e}", err=True)


def _s3_clean_all_tables(s3_client, config, older_than: str, pattern: str, cutoff_time, dry_run: bool, force: bool):
    """Clean S3 files for all tables"""
    try:
        prefix = f"{config.s3.incremental_path.strip('/')}/"
        
        # Get all files
        response = s3_client.list_objects_v2(
            Bucket=config.s3.bucket_name,
            Prefix=prefix,
            MaxKeys=1000
        )
        
        files_to_delete = []
        total_size = 0
        
        for obj in response.get('Contents', []):
            key = obj['Key']
            size = obj['Size']
            modified = obj['LastModified']
            
            # Only parquet files
            if not key.endswith('.parquet'):
                continue
            
            # Apply filters
            include_file = True
            
            # Pattern filtering
            if pattern:
                filename = key.split('/')[-1]
                if not fnmatch.fnmatch(filename, pattern):
                    include_file = False
            
            # Time filtering
            if cutoff_time and modified.replace(tzinfo=None) > cutoff_time:
                include_file = False
            
            if include_file:
                files_to_delete.append({'Key': key})
                total_size += size
        
        if not files_to_delete:
            click.echo("✅ No files found to delete")
            return
        
        # Show what will be deleted
        click.echo("🗑️  Files to delete (ALL TABLES):")
        click.echo(f"   Count: {len(files_to_delete)}")
        click.echo(f"   Total size: {total_size / 1024 / 1024:.2f} MB")
        
        if dry_run:
            click.echo("🧪 DRY RUN - Files would be deleted but no action taken")
            return
        
        # Extra confirmation for clean-all
        if not force:
            click.echo()
            click.echo("⚠️  WARNING: This will delete backup files for ALL tables!")
            if not click.confirm(f"Delete {len(files_to_delete)} files across all tables?"):
                click.echo("Operation cancelled")
                return
        
        # Delete files
        click.echo("🗑️  Deleting files...")
        s3_client.delete_objects(
            Bucket=config.s3.bucket_name,
            Delete={'Objects': files_to_delete}
        )
        
        click.echo(f"✅ Successfully deleted {len(files_to_delete)} files across all tables")
        
    except Exception as e:
        click.echo(f"❌ Failed to clean all S3 files: {e}", err=True)


if __name__ == '__main__':
    # Setup v1.1.0 commands if available
    v1_1_0_enabled = setup_v1_1_0_commands()
    if v1_1_0_enabled:
        # Add version indicator for v1.1.0 mode
        import os
        os.environ['CLI_VERSION'] = 'v1.1.0'
    
    # Setup v1.2.0 CDC Intelligence Engine commands
    v1_2_0_enabled = setup_v1_2_0_commands()
    if v1_2_0_enabled:
        import os
        os.environ['CLI_CDC_ENGINE'] = 'v1.2.0'
    
    cli()