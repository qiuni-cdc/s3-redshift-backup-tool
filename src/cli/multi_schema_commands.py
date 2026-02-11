"""
Multi-Schema CLI Commands for v1.1.0

Enhanced CLI commands supporting multi-connection data integration while maintaining
full backward compatibility with v1.0.0 syntax and workflows.
"""

import click
from typing import List, Optional, Dict, Any
from pathlib import Path
import sys
import os
from dataclasses import asdict

from src.core.connection_registry import ConnectionRegistry
from src.core.configuration_manager import ConfigurationManager
from src.utils.exceptions import ValidationError, ConnectionError
from src.utils.logging import get_logger

# Airflow Integration
from src.cli.airflow_integration import enhance_sync_with_airflow_integration

logger = get_logger(__name__)


class MultiSchemaContext:
    """Context object for multi-schema CLI commands"""

    def __init__(self):
        self.connection_registry: Optional[ConnectionRegistry] = None
        self.config_manager: Optional[ConfigurationManager] = None
        self._initialized = False

    def ensure_initialized(self, config_root: str = "config"):
        """Ensure multi-schema components are initialized"""
        if self._initialized:
            return

        try:
            self.connection_registry = ConnectionRegistry()
            self.config_manager = ConfigurationManager(config_root)
            self._initialized = True

            logger.info("Multi-schema CLI context initialized")

        except Exception as e:
            logger.error(f"Failed to initialize multi-schema context: {e}")
            click.echo(f"❌ Initialization failed: {e}")
            click.echo("💡 Tip: Run 'python -m src.cli.main config setup' to create default configuration")
            sys.exit(1)

    def cleanup(self):
        """Clean up all resources including SSH tunnels"""
        if self.connection_registry:
            try:
                logger.info("Cleaning up multi-schema context connections...")
                self.connection_registry.close_all_connections()
                logger.info("Multi-schema context cleanup completed")
            except Exception as e:
                logger.warning(f"Error during multi-schema context cleanup: {e}")
    
# Global context
multi_schema_ctx = MultiSchemaContext()


def add_multi_schema_commands(cli):
    """Add multi-schema commands to the main CLI"""
    
    # Enhanced sync command group
    @cli.group(name="sync", invoke_without_command=True)
    @click.option('--table', '-t', multiple=True, help='Table names (v1.0.0 compatibility)')
    @click.option('--backup-only', is_flag=True, help='Only backup to S3')
    @click.option('--redshift-only', is_flag=True, help='Only load to Redshift')
    @click.option('--limit', type=int, help='Limit rows per query (testing)')
    @click.option('--max-workers', type=int, help='Maximum parallel workers (overrides config)')
    @click.option('--max-chunks', type=int, help='Maximum number of chunks to process (total rows = limit × max-chunks)')
    @click.pass_context
    def sync_command(ctx, table: List[str], backup_only: bool, redshift_only: bool, limit: Optional[int], max_workers: Optional[int], max_chunks: Optional[int]):
        """Sync data - requires pipeline specification (v1.2.0 multi-pipeline system)"""

        if ctx.invoked_subcommand is None:
            # No subcommand provided - show help
            click.echo("❌ Pipeline or connection specification required")
            click.echo()
            click.echo("Usage:")
            click.echo("  python -m src.cli.main sync pipeline -p <pipeline_name> [-t table1 -t table2 ...]")
            click.echo("  python -m src.cli.main sync connection -c <connection_name> -t <table1> [-t table2 ...]")
            click.echo()
            click.echo("Examples:")
            click.echo("  python -m src.cli.main sync pipeline -p us_dw_unidw_2_public_pipeline")
            click.echo("  python -m src.cli.main sync connection -c US_DW_UNIDW -t unidw.table_name")
            click.echo()
            click.echo("💡 Use 'python -m src.cli.main config list-pipelines' to see available pipelines")
            sys.exit(1)
    
    @sync_command.command(name="pipeline")
    @click.option('--pipeline', '-p', required=True, help='Pipeline configuration name')
    @click.option('--table', '-t', multiple=True, required=True, help='Table names to sync')
    @click.option('--backup-only', is_flag=True, help='Only backup to S3 (MySQL → S3)')
    @click.option('--redshift-only', is_flag=True, help='Only load to Redshift (S3 → Redshift)')
    @click.option('--limit', type=int, help='Limit rows per query (testing)')
    @click.option('--max-workers', type=int, help='Maximum parallel workers (overrides config)')
    @click.option('--max-chunks', type=int, help='Maximum number of chunks to process (total rows = limit × max-chunks)')
    @click.option('--dry-run', is_flag=True, help='Preview operations without execution')
    @click.option('--parallel', is_flag=True, help='Override pipeline strategy to use parallel processing')
    @click.option('--json-output', type=click.Path(), help='Output execution metadata as JSON file for Airflow monitoring')
    @click.option('--s3-completion-bucket', type=str, help='S3 bucket for Airflow completion markers')
    @click.option('--initial-lookback-minutes', type=int, help='For first run only: Start backup from N minutes ago instead of full history')
    @click.option('--end-time', help='ISO 8601 timestamp to use as the end time (e.g., 2023-10-27T10:00:00). Defaults to current time.')
    def sync_pipeline(pipeline: str, table: List[str], backup_only: bool, redshift_only: bool,
                     limit: Optional[int], max_workers: Optional[int], max_chunks: Optional[int], dry_run: bool, parallel: bool,
                     json_output: Optional[str] = None, s3_completion_bucket: Optional[str] = None, initial_lookback_minutes: Optional[int] = None,
                     end_time: Optional[str] = None):
        """Sync tables using pipeline configuration (v1.1.0 enhanced syntax)"""

        multi_schema_ctx.ensure_initialized()

        try:
            # Load and validate pipeline configuration
            pipeline_config = multi_schema_ctx.config_manager.get_pipeline_config(pipeline)

            # Validate pipeline
            validation_report = multi_schema_ctx.config_manager.validate_pipeline(pipeline)
            if not validation_report['valid']:
                click.echo(f"❌ Pipeline validation failed:")
                for error in validation_report['errors'][:5]:  # Show first 5 errors
                    click.echo(f"  • {error}")
                if len(validation_report['errors']) > 5:
                    click.echo(f"  ... and {len(validation_report['errors']) - 5} more errors")
                return

            # Show pipeline information
            click.echo(f"🚀 Pipeline: {pipeline_config.name} (v{pipeline_config.version})")
            click.echo(f"📝 {pipeline_config.description}")
            click.echo(f"🔗 {pipeline_config.source} → {pipeline_config.target}")
            click.echo(f"📋 Tables: {', '.join(table)}")

            if dry_run:
                click.echo("🔍 DRY RUN - Preview mode enabled")

            if parallel and pipeline_config.processing.get('strategy') != 'parallel':
                click.echo("⚡ Overriding to parallel processing")

            # Check for explicit initial lookback request
            if initial_lookback_minutes:
                click.echo(f"🕒 First-run lookback enabled: {initial_lookback_minutes} minutes")

            # Check for explicit end time request
            if end_time:
                click.echo(f"🕒 Explicit end time provided: {end_time}")

            # Check if Airflow integration is requested
            if json_output or s3_completion_bucket:
                click.echo("🚀 Using Airflow integration features")

                # Create sync executor function for Airflow integration
                def execute_sync_tables(tracker):
                    """Execute sync for all tables and return results"""
                    table_results = {}
                    success_count = 0

                    for table_name in table:
                        if table_name not in pipeline_config.tables:
                            click.echo(f"⚠️  Warning: Table {table_name} not configured in pipeline")

                            # For default pipeline, allow dynamic registration
                            if pipeline == "default":
                                multi_schema_ctx.config_manager.register_table_dynamically(pipeline, table_name)
                                click.echo(f"📝 Registered {table_name} dynamically in default pipeline")
                            else:
                                table_results[table_name] = {
                                    "success": False,
                                    "error_message": f"Table {table_name} not configured in pipeline"
                                }
                                continue

                        table_config = pipeline_config.tables[table_name]

                        if dry_run:
                            _preview_table_sync(pipeline_config, table_config, backup_only, redshift_only)
                            table_results[table_name] = {
                                "success": True,
                                "rows_processed": 0,  # Dry run
                                "files_created": 0,
                                "duration": 0
                            }
                            success_count += 1
                        else:
                            result = _execute_table_sync(
                                pipeline_config, table_config,
                                backup_only, redshift_only,
                                limit, parallel, max_workers, max_chunks,
                                initial_lookback_minutes=initial_lookback_minutes,
                                end_time=end_time
                            )

                            # FIXED: Use actual metrics from _execute_table_sync instead of hardcoded values
                            table_results[table_name] = result

                            if result.get("success", False):
                                success_count += 1

                    return success_count, table_results

                # Use Airflow integration
                exit_code = enhance_sync_with_airflow_integration(
                    pipeline=pipeline,
                    tables=list(table),
                    sync_executor_func=execute_sync_tables,
                    json_output_path=json_output,
                    s3_completion_bucket=s3_completion_bucket,
                    completion_prefix="completion_markers"
                )
                sys.exit(exit_code)

            else:
                # Original logic for backward compatibility
                success_count = 0
                for table_name in table:
                    if table_name not in pipeline_config.tables:
                        click.echo(f"⚠️  Warning: Table {table_name} not configured in pipeline")

                        # For default pipeline, allow dynamic registration
                        if pipeline == "default":
                            multi_schema_ctx.config_manager.register_table_dynamically(pipeline, table_name)
                            click.echo(f"📝 Registered {table_name} dynamically in default pipeline")
                        else:
                            click.echo(f"❌ Skipping unconfigured table: {table_name}")
                            continue

                    table_config = pipeline_config.tables[table_name]

                    if dry_run:
                        _preview_table_sync(pipeline_config, table_config, backup_only, redshift_only)
                        success_count += 1
                    else:
                            pipeline_config, table_config,
                            backup_only, redshift_only, limit, parallel, max_workers, max_chunks,
                            initial_lookback_minutes=initial_lookback_minutes,
                            end_time=end_time
                        )
                        if success:
                            success_count += 1

                # Summary
                total_tables = len(table)
                if success_count == total_tables:
                    click.echo(f"✅ Pipeline completed successfully: {success_count}/{total_tables} tables")
                else:
                    click.echo(f"⚠️  Pipeline completed with issues: {success_count}/{total_tables} tables successful")
                    if success_count < total_tables:
                        sys.exit(1)

        except Exception as e:
            logger.error(f"Pipeline sync failed: {e}")
            click.echo(f"❌ Pipeline sync failed: {e}")
            sys.exit(1)

        finally:
            # Clean up SSH tunnels and connections from global context
            multi_schema_ctx.cleanup()
    
    @sync_command.command(name="connections")
    @click.option('--source', '-s', required=True, help='Source connection name')
    @click.option('--target', '-r', required=True, help='Target connection name')
    @click.option('--table', '-t', multiple=True, required=True, help='Table names to sync')
    @click.option('--backup-only', is_flag=True, help='Only backup to S3')
    @click.option('--redshift-only', is_flag=True, help='Only load to Redshift')
    @click.option('--limit', type=int, help='Limit rows per query')
    @click.option('--max-workers', type=int, help='Maximum parallel workers (overrides config)')
    @click.option('--max-chunks', type=int, help='Maximum number of chunks to process (total rows = limit × max-chunks)')
    @click.option('--batch-size', type=int, default=10000, help='Batch size for processing')
    def sync_connections(source: str, target: str, table: List[str], backup_only: bool,
                        redshift_only: bool, limit: Optional[int], max_workers: Optional[int], max_chunks: Optional[int], batch_size: int):
        """Sync tables using explicit connections (v1.1.0 ad-hoc syntax)"""

        multi_schema_ctx.ensure_initialized()

        try:
            # Validate connections exist
            source_info = multi_schema_ctx.connection_registry.get_connection_info(source)
            target_info = multi_schema_ctx.connection_registry.get_connection_info(target)

            click.echo(f"🚀 Ad-hoc Connection Sync")
            click.echo(f"📊 Source: {source} ({source_info['database']}@{source_info['host']})")
            click.echo(f"🎯 Target: {target} ({target_info['database']}@{target_info['host']})")
            click.echo(f"📋 Tables: {', '.join(table)}")

            # Test connections
            click.echo("🔍 Testing connections...")

            source_test = multi_schema_ctx.connection_registry.test_connection(source)
            if not source_test['success']:
                click.echo(f"❌ Source connection failed: {source_test['error']}")
                return
            click.echo(f"✅ Source connection: {source} ({source_test['duration_seconds']}s)")

            target_test = multi_schema_ctx.connection_registry.test_connection(target)
            if not target_test['success']:
                click.echo(f"❌ Target connection failed: {target_test['error']}")
                return
            click.echo(f"✅ Target connection: {target} ({target_test['duration_seconds']}s)")

            # Create ad-hoc pipeline configuration
            ad_hoc_pipeline = _create_adhoc_pipeline_config(source, target, list(table), batch_size)

            # Process tables
            success_count = 0
            for table_name in table:
                table_config = ad_hoc_pipeline.tables[table_name]
                success = _execute_table_sync(
                    ad_hoc_pipeline, table_config,
                    backup_only, redshift_only, limit, False, max_workers, max_chunks
                )
                if success:
                    success_count += 1

            # Summary
            total_tables = len(table)
            if success_count == total_tables:
                click.echo(f"✅ Connection sync completed: {success_count}/{total_tables} tables")
            else:
                click.echo(f"⚠️  Connection sync completed with issues: {success_count}/{total_tables} tables")
                if success_count < total_tables:
                    sys.exit(1)

        except Exception as e:
            logger.error(f"Connection-based sync failed: {e}")
            click.echo(f"❌ Connection sync failed: {e}")
            sys.exit(1)

        finally:
            # Clean up SSH tunnels and connections from global context
            multi_schema_ctx.cleanup()
    
    # Configuration management commands
    @cli.group(name="config")
    def config_command():
        """Configuration management for multi-schema support"""
        pass
    
    @config_command.command(name="setup")
    @click.option('--force', is_flag=True, help='Overwrite existing configuration')
    def config_setup(force: bool):
        """Set up default configuration for v1.1.0 multi-schema support"""
        
        config_dir = Path("config")
        
        # Check if configuration already exists
        if config_dir.exists() and not force:
            existing_files = list(config_dir.glob("*.yml"))
            if existing_files:
                click.echo("⚠️  Configuration directory already exists with files:")
                for file in existing_files:
                    click.echo(f"  • {file}")
                click.echo("Use --force to overwrite existing configuration")
                return
        
        try:
            # Initialize configuration manager (will create default files)
            multi_schema_ctx.ensure_initialized()
            
            click.echo("✅ Configuration setup completed!")
            click.echo("")
            click.echo("📁 Created configuration structure:")
            click.echo("  config/")
            click.echo("    ├── connections.yml       # Database connections")
            click.echo("    ├── pipelines/")
            click.echo("    │   └── default.yml      # v1.0.0 compatibility pipeline") 
            click.echo("    └── environments/")
            click.echo("        ├── development.yml   # Development settings")
            click.echo("        └── production.yml    # Production settings")
            click.echo("")
            click.echo("🔧 Next steps:")
            click.echo("  1. Review and update config/connections.yml with your database details")
            click.echo("  2. Create additional pipelines in config/pipelines/")
            click.echo("  3. Test connections: python -m src.cli.main connections test default")
            
        except Exception as e:
            click.echo(f"❌ Configuration setup failed: {e}")
            sys.exit(1)
    
    @config_command.command(name="list-pipelines")
    def config_list_pipelines():
        """List available pipeline configurations"""
        
        multi_schema_ctx.ensure_initialized()
        
        pipelines = multi_schema_ctx.config_manager.list_pipelines()
        
        if not pipelines:
            click.echo("📋 No pipeline configurations found")
            click.echo("💡 Run 'python -m src.cli.main config setup' to create default configuration")
            return
        
        click.echo("📋 Available Pipeline Configurations:")
        click.echo("")
        
        for pipeline_name in pipelines:
            try:
                pipeline_config = multi_schema_ctx.config_manager.get_pipeline_config(pipeline_name)
                
                # Get validation status
                validation = multi_schema_ctx.config_manager.validate_pipeline(pipeline_name)
                status_icon = "✅" if validation['valid'] else "❌"
                
                click.echo(f"  {status_icon} {pipeline_name}")
                click.echo(f"      Name: {pipeline_config.name}")
                click.echo(f"      Version: {pipeline_config.version}")
                click.echo(f"      Description: {pipeline_config.description}")
                click.echo(f"      Route: {pipeline_config.source} → {pipeline_config.target}")
                click.echo(f"      Tables: {len(pipeline_config.tables)}")
                
                if not validation['valid']:
                    click.echo(f"      Issues: {len(validation['errors'])} error(s)")
                
                click.echo("")
                
            except Exception as e:
                click.echo(f"  ❌ {pipeline_name} (Error: {e})")
                click.echo("")
    
    @config_command.command(name="show-pipeline")
    @click.argument('pipeline')
    @click.option('--verbose', '-v', is_flag=True, help='Show detailed configuration')
    def config_show_pipeline(pipeline: str, verbose: bool):
        """Show detailed pipeline configuration"""
        
        multi_schema_ctx.ensure_initialized()
        
        try:
            pipeline_config = multi_schema_ctx.config_manager.get_pipeline_config(pipeline)
            
            click.echo(f"📊 Pipeline Configuration: {pipeline}")
            click.echo("=" * 50)
            click.echo(f"Name: {pipeline_config.name}")
            click.echo(f"Version: {pipeline_config.version}")
            click.echo(f"Description: {pipeline_config.description}")
            click.echo(f"Source: {pipeline_config.source}")
            click.echo(f"Target: {pipeline_config.target}")
            click.echo("")
            
            # Processing configuration
            click.echo("🔧 Processing Configuration:")
            for key, value in pipeline_config.processing.items():
                click.echo(f"  {key}: {value}")
            click.echo("")
            
            # S3 configuration
            click.echo("🗂️  S3 Configuration:")
            for key, value in pipeline_config.s3.items():
                click.echo(f"  {key}: {value}")
            click.echo("")
            
            # Tables
            click.echo(f"📋 Tables ({len(pipeline_config.tables)}):")
            
            for table_name, table_config in pipeline_config.tables.items():
                click.echo(f"  • {table_name}")
                click.echo(f"    Full name: {table_config.full_name}")
                click.echo(f"    Target: {table_config.target_name}")
                click.echo(f"    Type: {table_config.table_type}")
                click.echo(f"    CDC Strategy: {table_config.cdc_strategy}")
                
                if table_config.depends_on:
                    click.echo(f"    Dependencies: {', '.join(table_config.depends_on)}")
                
                if verbose:
                    click.echo(f"    Timestamp Column: {table_config.cdc_timestamp_column}")
                    click.echo(f"    ID Column: {table_config.cdc_id_column}")
                    
                    if table_config.processing:
                        click.echo("    Processing:")
                        for key, value in table_config.processing.items():
                            click.echo(f"      {key}: {value}")
                    
                    if table_config.validation:
                        click.echo("    Validation:")
                        for key, value in table_config.validation.items():
                            click.echo(f"      {key}: {value}")
                
                click.echo("")
        
        except Exception as e:
            click.echo(f"❌ Error showing pipeline: {e}")
            sys.exit(1)
    
    @config_command.command(name="validate-pipeline")
    @click.argument('pipeline')
    @click.option('--verbose', '-v', is_flag=True, help='Show detailed validation results')
    def config_validate_pipeline(pipeline: str, verbose: bool):
        """Validate pipeline configuration"""
        
        multi_schema_ctx.ensure_initialized()
        
        try:
            validation_report = multi_schema_ctx.config_manager.validate_pipeline(pipeline)
            
            if validation_report['valid']:
                click.echo(f"✅ Pipeline '{pipeline}' is valid")
                click.echo(f"📊 Tables validated: {validation_report['tables_validated']}")
                
                if verbose and validation_report['warnings']:
                    click.echo("")
                    click.echo("⚠️  Warnings:")
                    for warning in validation_report['warnings']:
                        click.echo(f"  • {warning}")
            else:
                click.echo(f"❌ Pipeline '{pipeline}' has validation errors:")
                click.echo("")
                
                # Show errors by category
                details = validation_report['validation_details']
                
                if not details['pipeline_structure']['valid']:
                    click.echo("🏗️  Pipeline Structure Issues:")
                    for issue in details['pipeline_structure']['issues']:
                        click.echo(f"  • {issue}")
                    click.echo("")
                
                if not details['table_configurations']['valid']:
                    click.echo("📋 Table Configuration Issues:")
                    for issue in details['table_configurations']['issues']:
                        click.echo(f"  • {issue}")
                    click.echo("")
                
                if not details['dependencies']['valid']:
                    click.echo("🔗 Dependency Issues:")
                    for issue in details['dependencies']['issues']:
                        click.echo(f"  • {issue}")
                    click.echo("")
                
                if not details['resource_requirements']['valid']:
                    click.echo("⚡ Resource Requirement Issues:")
                    for issue in details['resource_requirements']['issues']:
                        click.echo(f"  • {issue}")
                    click.echo("")
                
                # Show warnings if verbose
                if verbose and validation_report['warnings']:
                    click.echo("⚠️  Warnings:")
                    for warning in validation_report['warnings']:
                        click.echo(f"  • {warning}")
                
                sys.exit(1)
        
        except Exception as e:
            click.echo(f"❌ Validation failed: {e}")
            sys.exit(1)
    
    @config_command.command(name="status")
    def config_status():
        """Show overall configuration status"""
        
        multi_schema_ctx.ensure_initialized()
        
        status = multi_schema_ctx.config_manager.get_configuration_status()
        
        click.echo("📊 Configuration Status")
        click.echo("=" * 30)
        
        # Pipelines
        pipelines = status['pipelines']
        click.echo(f"📋 Pipelines: {pipelines['count']}")
        if pipelines['names']:
            for name in pipelines['names']:
                click.echo(f"  • {name}")
        click.echo("")
        
        # Environments
        environments = status['environments']
        click.echo(f"🌍 Environments: {environments['count']}")
        click.echo(f"  Current: {environments['current']}")
        if environments['available']:
            click.echo(f"  Available: {', '.join(environments['available'])}")
        click.echo("")
        
        # Templates
        templates = status['templates']
        if templates['count'] > 0:
            click.echo(f"📄 Templates: {templates['count']}")
            if templates['available']:
                click.echo(f"  Available: {', '.join(templates['available'])}")
            click.echo("")
        
        # Configuration details
        config = status['configuration']
        click.echo(f"⚙️  Configuration:")
        click.echo(f"  Root: {config['root_directory']}")
        click.echo(f"  Auto-reload: {'enabled' if config['auto_reload_enabled'] else 'disabled'}")
        if config['last_reload']:
            click.echo(f"  Last reload: {config['last_reload']}")
        click.echo(f"  Tracked files: {config['tracked_files']}")
        click.echo("")
        
        # Validation summary
        validation = status['validation_summary']
        click.echo(f"🔍 Validation Summary:")
        click.echo(f"  Total pipelines: {validation['total_pipelines']}")
        click.echo(f"  Valid: {validation['valid_pipelines']}")
        click.echo(f"  Invalid: {validation['invalid_pipelines']}")
        click.echo(f"  Total tables: {validation['total_tables']}")
        
        if validation['validation_errors']:
            click.echo("")
            click.echo("❌ Validation Errors:")
            for error in validation['validation_errors'][:5]:
                click.echo(f"  • {error}")
            if len(validation['validation_errors']) > 5:
                click.echo(f"  ... and {len(validation['validation_errors']) - 5} more")
    
    # Connection management commands
    @cli.group(name="connections")
    def connections_command():
        """Connection management for multi-schema support"""
        pass
    
    @connections_command.command(name="list")
    @click.option('--type', 'connection_type', type=click.Choice(['mysql', 'redshift', 'all']), 
                  default='all', help='Filter by connection type')
    def connections_list(connection_type: str):
        """List available database connections"""
        
        multi_schema_ctx.ensure_initialized()
        
        all_connections = multi_schema_ctx.connection_registry.list_connections()
        
        if not all_connections:
            click.echo("🔌 No connections configured")
            click.echo("💡 Run 'python -m src.cli.main config setup' to create default configuration")
            return
        
        # Filter by type
        if connection_type != 'all':
            all_connections = {
                k: v for k, v in all_connections.items() 
                if v['type'] == connection_type
            }
        
        if not all_connections:
            click.echo(f"🔌 No {connection_type} connections found")
            return
        
        click.echo("🔌 Available Database Connections:")
        click.echo("")
        
        # Group by type
        sources = {k: v for k, v in all_connections.items() if v['type'] == 'mysql'}
        targets = {k: v for k, v in all_connections.items() if v['type'] == 'redshift'}
        
        if sources and connection_type in ['mysql', 'all']:
            click.echo("🗄️  MySQL Sources:")
            for name, info in sources.items():
                status_icon = "🟢" if info['status'] == 'active' else "⚪"
                click.echo(f"  {status_icon} {name}")
                click.echo(f"      Host: {info['host']}:{info['port']}")
                click.echo(f"      Database: {info['database']}")
                click.echo(f"      User: {info['username']}")
                if info['description']:
                    click.echo(f"      Description: {info['description']}")
                click.echo("")
        
        if targets and connection_type in ['redshift', 'all']:
            click.echo("🎯 Redshift Targets:")
            for name, info in targets.items():
                status_icon = "🟢" if info['status'] == 'active' else "⚪"
                tunnel_icon = "🔒" if info['has_ssh_tunnel'] else "🌐"
                click.echo(f"  {status_icon} {name} {tunnel_icon}")
                click.echo(f"      Host: {info['host']}:{info['port']}")
                click.echo(f"      Database: {info['database']} (schema: {info['schema']})")
                click.echo(f"      User: {info['username']}")
                if info['description']:
                    click.echo(f"      Description: {info['description']}")
                click.echo("")
    
    @connections_command.command(name="test")
    @click.argument('connection_name', required=False)
    @click.option('--all', 'test_all', is_flag=True, help='Test all connections')
    def connections_test(connection_name: Optional[str], test_all: bool):
        """Test database connections"""

        multi_schema_ctx.ensure_initialized()

        try:
            if test_all:
                click.echo("🔍 Testing all connections...")
                click.echo("")

                results = multi_schema_ctx.connection_registry.test_all_connections()

                for conn_name, result in results['results'].items():
                    if result['success']:
                        click.echo(f"✅ {conn_name} ({result['duration_seconds']}s)")
                        if result['details']:
                            for key, value in result['details'].items():
                                if 'version' in key.lower():
                                    click.echo(f"   {value}")
                    else:
                        click.echo(f"❌ {conn_name}: {result['error']}")
                    click.echo("")

                # Summary
                summary = results['summary']
                click.echo(f"📊 Summary: {summary['successful_connections']}/{summary['total_connections']} successful ({summary['success_rate']}%)")

                if summary['successful_connections'] < summary['total_connections']:
                    sys.exit(1)

            elif connection_name:
                click.echo(f"🔍 Testing connection: {connection_name}")

                result = multi_schema_ctx.connection_registry.test_connection(connection_name)

                if result['success']:
                    click.echo(f"✅ Connection successful ({result['duration_seconds']}s)")

                    if result['details']:
                        click.echo("")
                        click.echo("Connection Details:")
                        for key, value in result['details'].items():
                            click.echo(f"  {key}: {value}")
                else:
                    click.echo(f"❌ Connection failed: {result['error']}")
                    sys.exit(1)

            else:
                click.echo("❌ Please specify a connection name or use --all")
                click.echo("💡 List connections: python -m src.cli.main connections list")
                sys.exit(1)

        finally:
            # Clean up SSH tunnels and connections from global context
            multi_schema_ctx.cleanup()
    
    @connections_command.command(name="info")
    @click.argument('connection_name')
    def connections_info(connection_name: str):
        """Show detailed connection information"""
        
        multi_schema_ctx.ensure_initialized()
        
        try:
            info = multi_schema_ctx.connection_registry.get_connection_info(connection_name)
            
            click.echo(f"🔌 Connection: {info['name']}")
            click.echo("=" * 40)
            click.echo(f"Type: {info['type'].upper()}")
            click.echo(f"Host: {info['host']}:{info['port']}")
            click.echo(f"Database: {info['database']}")
            if info['schema']:
                click.echo(f"Schema: {info['schema']}")
            click.echo(f"Username: {info['username']}")
            click.echo(f"Status: {info['status']}")
            
            if info['description']:
                click.echo(f"Description: {info['description']}")
            
            if info['has_ssh_tunnel']:
                click.echo("🔒 SSH Tunnel: Enabled")
            
            if info['pool_settings']:
                click.echo("")
                click.echo("🏊 Connection Pool Settings:")
                for key, value in info['pool_settings'].items():
                    click.echo(f"  {key}: {value}")
        
        except Exception as e:
            click.echo(f"❌ Error getting connection info: {e}")
            sys.exit(1)
    
    @connections_command.command(name="health")
    def connections_health():
        """Show connection registry health status"""
        
        multi_schema_ctx.ensure_initialized()
        
        health = multi_schema_ctx.connection_registry.get_health_status()
        
        click.echo("🏥 Connection Registry Health")
        click.echo("=" * 35)
        click.echo(f"Total connections: {health['total_connections']}")
        click.echo(f"Active MySQL pools: {health['mysql_pools_active']}")
        click.echo(f"Active SSH tunnels: {health['ssh_tunnels_active']}")
        click.echo("")
        
        click.echo("Connection Types:")
        for conn_type, count in health['connection_types'].items():
            click.echo(f"  {conn_type}: {count}")
        click.echo("")
        
        click.echo(f"Configuration file: {health['configuration_file']}")
        click.echo(f"Configuration exists: {'✅' if health['configuration_exists'] else '❌'}")
        click.echo("")
        
        click.echo("Settings:")
        for key, value in health['settings'].items():
            click.echo(f"  {key}: {value}")


def _show_sync_help():
    """Show help for sync command options (v1.2.0 multi-pipeline system)"""
    click.echo("📚 Multi-Pipeline Sync Options (v1.2.0):")
    click.echo("")
    click.echo("Pipeline-based sync (required):")
    click.echo("  python -m src.cli.main sync pipeline -p PIPELINE_NAME -t TABLE1 -t TABLE2")
    click.echo("")
    click.echo("Connection-based sync:")
    click.echo("  python -m src.cli.main sync connection -c SOURCE_CONN -t TABLE1 -t TABLE2")
    click.echo("")
    click.echo("💡 Tips:")
    click.echo("  • List pipelines: python -m src.cli.main config list-pipelines")
    click.echo("  • List connections: python -m src.cli.main connections list")
    click.echo("  • Setup configuration: python -m src.cli.main config setup")


def _preview_table_sync(pipeline_config, table_config, backup_only: bool, redshift_only: bool):
    """Preview table sync operations without execution"""
    click.echo(f"🔍 Preview: {table_config.full_name}")
    click.echo(f"  Strategy: {table_config.cdc_strategy}")
    click.echo(f"  Type: {table_config.table_type}")
    from src.utils.validation import resolve_batch_size
    batch_size = resolve_batch_size(
        table_config={'processing': table_config.processing}
    )
    click.echo(f"  Batch size: {batch_size}")
    
    if not redshift_only:
        click.echo(f"  📤 MySQL extraction: {pipeline_config.source}")
        click.echo(f"    Source table: {table_config.full_name}")
        click.echo(f"    Timestamp column: {table_config.cdc_timestamp_column}")
        if table_config.cdc_strategy in ['hybrid', 'id_only']:
            click.echo(f"    ID column: {table_config.cdc_id_column}")
    
    if not backup_only:
        click.echo(f"  📥 Redshift loading: {pipeline_config.target}")
        # Show if it's a custom mapping or default
        if table_config.target_name != table_config.full_name:
            # Extract the default table name for comparison
            default_name = table_config.full_name
            if '.' in default_name:
                _, default_name = default_name.rsplit('.', 1)
            if table_config.target_name != default_name:
                click.echo(f"    Target table: {table_config.target_name} (custom mapping)")
            else:
                click.echo(f"    Target table: {table_config.target_name}")
        else:
            click.echo(f"    Target table: {table_config.target_name}")
    
    if table_config.depends_on:
        click.echo(f"  📋 Dependencies: {', '.join(table_config.depends_on)}")


def _execute_table_sync(pipeline_config, table_config, backup_only: bool, redshift_only: bool,
                       limit: Optional[int], parallel: bool, max_workers: Optional[int] = None, max_chunks: Optional[int] = None,
                       initial_lookback_minutes: Optional[int] = None, end_time: Optional[str] = None) -> Dict[str, Any]:
    """Execute actual table sync with multi-schema configuration"""

    click.echo(f"🚀 Syncing: {table_config.full_name}")

    # Track connection registry for cleanup
    connection_registry = None

    try:
        # Import the existing backup strategies
        from src.backup.sequential import SequentialBackupStrategy
        from src.backup.inter_table import InterTableBackupStrategy
        
        # Use v1.0.0 backup strategy with proper configuration
        # The existing backup strategies are designed to work with AppConfig, not direct connections
        try:
            # Extract S3 isolation prefix for target-based scoping (v2.1)
            isolation_prefix = pipeline_config.s3.get('isolation_prefix', '')

            # Use YAML-based configuration (migrated from AppConfig)
            config = multi_schema_ctx.config_manager.create_app_config(
                source_connection=pipeline_config.source,
                target_connection=pipeline_config.target,
                s3_config_name=pipeline_config.s3_config,  # Pass S3 config from pipeline
                isolation_prefix=isolation_prefix  # v2.1: Target-based S3 isolation
            )
            
            # Determine backup strategy based on pipeline configuration  
            strategy_name = pipeline_config.processing.get('strategy', 'sequential')
            if parallel:
                strategy_name = 'parallel'
            
            # Prepare pipeline configuration for backup strategy
            pipeline_config_dict = {
                'pipeline': {
                    's3': pipeline_config.s3,
                    'processing': pipeline_config.processing
                },
                'tables': {name: asdict(cfg) for name, cfg in pipeline_config.tables.items()}
            }

            # Create backup strategy with pipeline configuration
            if strategy_name == 'parallel':
                backup_strategy = InterTableBackupStrategy(config, pipeline_config=pipeline_config_dict)
            else:
                backup_strategy = SequentialBackupStrategy(config, pipeline_config=pipeline_config_dict)
            
            # Configure strategy with pipeline settings
            if hasattr(backup_strategy, 'set_batch_size'):
                from src.utils.validation import resolve_batch_size

                batch_size = resolve_batch_size(
                    table_config={'processing': table_config.processing},
                    pipeline_config=pipeline_config_dict
                )
                backup_strategy.set_batch_size(batch_size)
            
            # Execute the sync using the table name
            # The backup strategy will handle its own connection management
            
            # FIXED: Generate scoped table name for v1.2.0 multi-schema S3 support
            # For connection-based sync, we need to scope the table name with connection identifier
            base_table_name = table_config.full_name
            
            # Check if we're in ad-hoc connection sync mode (pipeline name contains "adhoc_")
            if pipeline_config.name.startswith("adhoc_") and "_to_" in pipeline_config.name:
                # Extract source connection from ad-hoc pipeline name: "adhoc_US_DW_RO_SSH_to_redshift_default"
                parts = pipeline_config.name.split("_to_")
                if len(parts) >= 2:
                    source_connection = parts[0].replace("adhoc_", "")
                    scoped_table_name = f"{source_connection}:{base_table_name}"
                    logger.info(f"Using scoped table name for v1.2.0 S3 isolation: {scoped_table_name}")
                else:
                    scoped_table_name = base_table_name
            else:
                # Regular pipeline sync - use scoped table name for v1.2.0 multi-schema consistency
                source_connection = pipeline_config.source
                scoped_table_name = f"{source_connection}:{base_table_name}"
                logger.info(f"Using scoped table name for v1.2.0 pipeline: {scoped_table_name}")
            
            table_list = [scoped_table_name]
            
            # Calculate parameters for the backup
            from src.utils.validation import resolve_batch_size
            default_batch_size = resolve_batch_size(
                table_config={'processing': table_config.processing}
            )
            chunk_size = limit if limit else default_batch_size
            max_total_rows = None
            
            if limit and max_chunks:
                # limit = chunk size, max_chunks = number of chunks
                # total rows = limit × max_chunks
                max_total_rows = limit * max_chunks
                logger.info(f"Row limits for {base_table_name}: {chunk_size} rows/chunk × {max_chunks} chunks = {max_total_rows} total rows")
            elif limit and not max_chunks:
                # limit = total row limit (user expectation for --limit 100)
                max_total_rows = limit
                from src.utils.validation import resolve_batch_size
                default_batch_size = resolve_batch_size(
                    table_config={'processing': table_config.processing}
                )
                chunk_size = min(limit, default_batch_size)
                logger.info(f"Total row limit for {base_table_name}: {max_total_rows} rows (chunk size: {chunk_size})")
            elif max_chunks and not limit:
                # max_chunks specified but no limit - use default chunk size
                max_total_rows = chunk_size * max_chunks
                logger.info(f"Row limits for {base_table_name}: {chunk_size} rows/chunk × {max_chunks} chunks = {max_total_rows} total rows")
            
            # FIXED: Properly handle backup_only and redshift_only flags (v1.2.0 regression fix)
            backup_success = True
            redshift_success = True

            if not redshift_only:
                # Stage 1: MySQL → S3 Backup with multi-schema support
                source_connection = pipeline_config.source
                backup_success = backup_strategy.execute(
                    tables=table_list,
                    chunk_size=chunk_size,
                    max_total_rows=max_total_rows,
                    source_connection=source_connection,
                    initial_lookback_minutes=initial_lookback_minutes,
                    end_time=end_time
                )
                # Get connection registry for later cleanup
                connection_registry = getattr(backup_strategy, 'connection_registry', None)

            if not backup_only and backup_success:
                # Stage 2: S3 → Redshift Loading (using v1.0.0 working pattern)
                try:
                    from src.core.gemini_redshift_loader import GeminiRedshiftLoader
                    from src.core.watermark_adapter import create_watermark_manager
                    from src.core.cdc_configuration_manager import CDCConfigurationManager

                    # Use the watermark v2.0 system for Redshift loading with shared connection registry
                    # Pass the connection registry from backup to ensure SSH tunnels are reused
                    if connection_registry is None:
                        connection_registry = getattr(backup_strategy, 'connection_registry', None)
                    redshift_loader = GeminiRedshiftLoader(config, connection_registry=connection_registry)
                    watermark_manager = create_watermark_manager(config.to_dict())
                    
                    # Create CDC strategy for full_sync replace mode support
                    cdc_strategy = None
                    try:
                        cdc_config_manager = CDCConfigurationManager()
                        # Convert table_config to dict for CDC parsing
                        # Get batch_size with proper fallback hierarchy
                        # 1. Table-specific processing.batch_size (highest priority)
                        batch_size = table_config.processing.get('batch_size')
                        if batch_size is None:
                            # 2. Pipeline processing.batch_size (fallback)
                            pipeline_processing = config.pipeline.processing if hasattr(config.pipeline, 'processing') else {}
                            batch_size = pipeline_processing.get('batch_size')
                        if batch_size is None:
                            # 3. System default from AppConfig (final fallback)
                            batch_size = config.backup.target_rows_per_chunk
                        
                        table_config_dict = {
                            'cdc_strategy': table_config.cdc_strategy,
                            'cdc_timestamp_column': table_config.cdc_timestamp_column,
                            'cdc_id_column': table_config.cdc_id_column,
                            'full_sync_mode': table_config.full_sync_mode,
                            'batch_size': batch_size
                        }
                        cdc_strategy = cdc_config_manager.create_cdc_strategy(table_config_dict, base_table_name)
                        if cdc_strategy:
                            strategy_mode = cdc_strategy.get_sync_mode() if hasattr(cdc_strategy, 'get_sync_mode') else 'unknown'
                            logger.info(f"Created CDC strategy for {base_table_name}: {cdc_strategy.strategy_name} ({strategy_mode} mode)")
                    except Exception as cdc_e:
                        logger.warning(f"Failed to create CDC strategy for {base_table_name}: {cdc_e}")
                        # Continue without CDC strategy - will use default behavior
                    
                    # Test Redshift connection first
                    connection_test = redshift_loader._test_connection()
                    if not connection_test:
                        raise Exception("Redshift connection test failed")
                    
                    # Load table data to Redshift using the scoped name for S3 file discovery
                    # Pass CDC strategy for full_sync replace mode TRUNCATE support and table_config for target name mapping
                    redshift_success = redshift_loader.load_table_data(scoped_table_name, cdc_strategy, table_config)
                    
                except Exception as redshift_error:
                    logger.error(f"Redshift loading failed for {scoped_table_name}: {redshift_error}")
                    redshift_success = False
            
            # Overall result
            result = backup_success and redshift_success
            
            # FIXED: Get actual metrics from watermark system instead of hardcoded values
            actual_rows = 0
            actual_files = 0
            
            if result:
                try:
                    # Get actual metrics from watermark
                    from src.core.watermark_adapter import create_watermark_manager
                    watermark_manager = create_watermark_manager(config.to_dict())
                    watermark = watermark_manager.get_table_watermark(scoped_table_name)

                    if watermark:
                        # FIXED: Get session-specific rows processed from watermark (not cumulative)
                        # For Airflow integration, we want to report what was processed in THIS sync session
                        backup_session_rows = getattr(watermark, 'mysql_last_session_rows', 0) or 0

                        # For Redshift, we don't have session tracking yet, use cumulative as fallback
                        # TODO: Add session tracking for Redshift loading as well
                        redshift_rows = getattr(watermark, 'redshift_rows_loaded', 0) or 0

                        # Use the most relevant metric based on operation type
                        if redshift_only:
                            actual_rows = redshift_rows  # Cumulative (no session tracking for Redshift yet)
                        elif backup_only:
                            actual_rows = backup_session_rows  # Session-specific for backup
                        else:
                            # Full sync - use backup session rows (more accurate for this sync)
                            actual_rows = backup_session_rows

                        # Get file count from processed S3 files list
                        processed_files = getattr(watermark, 'processed_s3_files', []) or []
                        actual_files = len(processed_files)

                        logger.info(f"METRICS (session): {scoped_table_name} - {actual_rows} rows, {actual_files} files")
                    
                except Exception as metrics_error:
                    logger.warning(f"Failed to get actual metrics for {scoped_table_name}: {metrics_error}")
                    # Fall back to estimated values based on chunk processing
                    actual_rows = max_total_rows if max_total_rows else 0
                    actual_files = 1
            
            result_dict = {
                "success": result,
                "rows_processed": actual_rows,
                "files_created": actual_files,
                "duration": 30.0  # TODO: Add actual duration tracking
            }

            if result:
                click.echo(f"  ✅ {table_config.full_name} synced successfully")
            else:
                click.echo(f"  ❌ {table_config.full_name} sync failed")

            return result_dict

        except Exception as config_error:
            logger.error(f"Failed to execute table sync with v1.0.0 compatibility: {config_error}")
            click.echo(f"  ❌ Configuration error: {config_error}")
            return {
                "success": False,
                "error_message": str(config_error),
                "rows_processed": 0,
                "files_created": 0,
                "duration": 0
            }

    except ImportError as e:
        # v1.0.0 compatibility removed - fail with clear error message
        error_msg = f"Required backup strategies not available: {e}. Ensure all dependencies are installed."
        click.echo(f"  ❌ {table_config.full_name} failed: {error_msg}")
        logger.error(f"ImportError during table sync for {table_config.full_name}: {e}")
        return {
            "success": False,
            "error_message": error_msg,
            "rows_processed": 0,
            "files_created": 0,
            "duration": 0
        }

    except Exception as e:
        click.echo(f"  ❌ {table_config.full_name} failed: {e}")
        logger.error(f"Table sync failed for {table_config.full_name}: {e}")
        return {
            "success": False,
            "error_message": str(e),
            "rows_processed": 0,
            "files_created": 0,
            "duration": 0
        }

    finally:
        # Always clean up connection registry to close SSH tunnels
        logger.info(f"Cleanup: connection_registry is {'set' if connection_registry else 'None'}")
        if connection_registry:
            try:
                logger.info("Closing all connections...")
                connection_registry.close_all_connections()
                logger.info("Connection registry cleaned up")
            except Exception as cleanup_error:
                logger.warning(f"Error during connection cleanup: {cleanup_error}")


def _create_adhoc_pipeline_config(source: str, target: str, tables: List[str], batch_size: int):
    """Create ad-hoc pipeline configuration for connection-based sync"""
    from src.core.configuration_manager import PipelineConfig, TableConfig
    
    # Create table configurations with proper timestamp column detection
    table_configs = {}
    for table_name in tables:
        # FIXED: Look up table-specific configuration from existing pipelines
        timestamp_column = "updated_at"  # Default
        id_column = "id"  # Default
        
        # Try to find existing configuration for this table in any pipeline
        if multi_schema_ctx.config_manager:
            try:
                # Check us_dw_pipeline first (maps to US_DW_RO_SSH connection)
                if source == "US_DW_RO_SSH":
                    pipeline_config = multi_schema_ctx.config_manager.get_pipeline_config("us_dw_pipeline")
                    if pipeline_config and table_name in pipeline_config.tables:
                        table_def = pipeline_config.tables[table_name]
                        timestamp_column = table_def.cdc_timestamp_column or timestamp_column
                        id_column = table_def.cdc_id_column or id_column
                        logger.info(f"Found pipeline config for {table_name}: timestamp={timestamp_column}, id={id_column}")
                    else:
                        logger.info(f"No specific config found for {table_name}, using defaults")
                else:
                    # For other connections, try to find matching pipeline
                    for pipeline_name in multi_schema_ctx.config_manager.list_pipelines():
                        try:
                            pipeline_config = multi_schema_ctx.config_manager.get_pipeline_config(pipeline_name)
                            if pipeline_config and pipeline_config.source == source and table_name in pipeline_config.tables:
                                table_def = pipeline_config.tables[table_name]
                                timestamp_column = table_def.cdc_timestamp_column or timestamp_column
                                id_column = table_def.cdc_id_column or id_column
                                logger.info(f"Found pipeline config in {pipeline_name} for {table_name}: timestamp={timestamp_column}, id={id_column}")
                                break
                        except:
                            continue
            except Exception as e:
                logger.warning(f"Failed to lookup pipeline config for {table_name}: {e}")
        
        table_configs[table_name] = TableConfig(
            full_name=table_name,
            cdc_strategy="hybrid",
            cdc_timestamp_column=timestamp_column,
            cdc_id_column=id_column,
            processing={'batch_size': batch_size}
        )
    
    return PipelineConfig(
        name=f"adhoc_{source}_to_{target}",
        version="1.1.0",
        description=f"Ad-hoc pipeline: {source} → {target}",
        source=source,
        target=target,
        processing={'strategy': 'sequential', 'batch_size': batch_size},
        s3={'isolation_prefix': f"adhoc_{source}_{target}", 'partition_strategy': 'hybrid'},
        default_table_config={},
        tables=table_configs
    )