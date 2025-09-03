"""
v1.2.0 Enhanced CLI Commands

New CLI commands for v1.2.0 CDC Intelligence Engine:
- CDC strategy validation and testing
- Configuration migration tools  
- Advanced debugging and diagnostics
"""

import click
from typing import Dict, Any, Optional
import json
from pathlib import Path
import yaml

from src.core.cdc_strategy_engine import CDCStrategyFactory, CDCConfig, CDCStrategyType
from src.core.cdc_configuration_manager import CDCConfigurationManager
from src.backup.cdc_backup_integration import CDCBackupIntegration
from src.utils.logging import get_logger

logger = get_logger(__name__)


@click.group()
def cdc():
    """CDC Strategy Engine commands (v1.2.0)"""
    pass


@cdc.command()
@click.option('--table', '-t', required=True, help='Table name to validate CDC configuration')
@click.option('--pipeline', '-p', help='Pipeline configuration file')
@click.option('--strategy', '-s', help='CDC strategy to test (overrides config)')
@click.option('--timestamp-column', help='Timestamp column (for strategy testing)')
@click.option('--id-column', help='ID column (for strategy testing)')
@click.option('--dry-run', is_flag=True, help='Only validate, do not test actual queries')
def validate(table: str, pipeline: Optional[str], strategy: Optional[str], 
            timestamp_column: Optional[str], id_column: Optional[str], dry_run: bool):
    """Validate CDC configuration for a table"""
    
    click.echo(f"🔍 Validating CDC configuration for table: {table}")
    
    try:
        cdc_manager = CDCConfigurationManager()
        
        # Load table configuration
        table_config = {}
        if pipeline:
            with open(pipeline, 'r') as f:
                pipeline_config = yaml.safe_load(f)
                tables = pipeline_config.get('tables', {})
                table_config = tables.get(table, {})
        
        # Override with command-line options
        if strategy:
            table_config['cdc_strategy'] = strategy
        if timestamp_column:
            table_config['cdc_timestamp_column'] = timestamp_column
        if id_column:
            table_config['cdc_id_column'] = id_column
        
        # Create and validate CDC strategy
        cdc_strategy = cdc_manager.create_cdc_strategy(table_config, table)
        
        click.echo(f"✅ CDC Strategy: {cdc_strategy.strategy_name}")
        click.echo(f"✅ Configuration:")
        click.echo(f"   - Timestamp Column: {cdc_strategy.config.timestamp_column}")
        click.echo(f"   - ID Column: {cdc_strategy.config.id_column}")
        click.echo(f"   - Batch Size: {cdc_strategy.config.batch_size}")
        
        if not dry_run:
            # Test query building
            test_watermark = {
                'last_mysql_data_timestamp': '2025-01-01 00:00:00',
                'last_processed_id': 1000
            }
            
            query = cdc_strategy.build_query(table, test_watermark, 100)
            click.echo(f"\n📋 Sample Query:")
            click.echo(query)
        
        click.echo(f"\n✅ CDC validation successful for {table}")
        
    except Exception as e:
        click.echo(f"❌ CDC validation failed: {e}", err=True)
        raise click.ClickException(str(e))


@cdc.command()
def strategies():
    """List supported CDC strategies"""
    
    click.echo("📋 Supported CDC Strategies:")
    
    factory = CDCStrategyFactory()
    strategies = factory.get_supported_strategies()
    
    strategy_descriptions = {
        'timestamp_only': 'Uses single timestamp column (v1.0.0/v1.1.0 compatible)',
        'hybrid': 'Uses timestamp + ID columns (most robust)',
        'id_only': 'Uses auto-increment ID column (append-only tables)', 
        'full_sync': 'Complete table refresh (small tables)',
        'custom_sql': 'User-defined SQL query (advanced)'
    }
    
    for strategy in strategies:
        desc = strategy_descriptions.get(strategy, 'No description available')
        click.echo(f"  • {strategy:<15} - {desc}")
    
    click.echo(f"\n📊 Total strategies: {len(strategies)}")


@cdc.command()
@click.option('--pipeline', '-p', required=True, help='Pipeline configuration file to validate')
@click.option('--output', '-o', help='Save validation results to file')
def validate_pipeline(pipeline: str, output: Optional[str]):
    """Validate CDC configuration for entire pipeline"""
    
    click.echo(f"🔍 Validating pipeline CDC configuration: {pipeline}")
    
    try:
        # Load pipeline configuration
        with open(pipeline, 'r') as f:
            pipeline_config = yaml.safe_load(f)
        
        # Validate CDC configuration
        cdc_manager = CDCConfigurationManager()
        results = cdc_manager.validate_pipeline_cdc_config(pipeline_config)
        
        # Display results
        if results['valid']:
            click.echo(f"✅ Pipeline validation successful")
        else:
            click.echo(f"❌ Pipeline validation failed")
        
        click.echo(f"📊 Tables validated: {results['tables_validated']}")
        click.echo(f"📊 Strategies used: {', '.join(results['strategies_used'])}")
        
        if results['warnings']:
            click.echo(f"\n⚠️  Warnings ({len(results['warnings'])}):")
            for warning in results['warnings']:
                click.echo(f"   - {warning}")
        
        if results['errors']:
            click.echo(f"\n❌ Errors ({len(results['errors'])}):")
            for error in results['errors']:
                click.echo(f"   - {error}")
        
        # Save results if requested
        if output:
            with open(output, 'w') as f:
                json.dump(results, f, indent=2, default=str)
            click.echo(f"\n💾 Results saved to: {output}")
        
        if not results['valid']:
            raise click.ClickException("Pipeline validation failed")
        
    except FileNotFoundError:
        click.echo(f"❌ Pipeline file not found: {pipeline}", err=True)
        raise click.ClickException(f"File not found: {pipeline}")
    except Exception as e:
        click.echo(f"❌ Pipeline validation error: {e}", err=True)
        raise click.ClickException(str(e))


@cdc.command()
@click.option('--input', '-i', required=True, help='v1.1.0 pipeline configuration file')
@click.option('--output', '-o', required=True, help='Output file for v1.2.0 configuration')
@click.option('--backup', is_flag=True, help='Create backup of original file')
def migrate(input: str, output: str, backup: bool):
    """Migrate v1.1.0 pipeline configuration to v1.2.0"""
    
    click.echo(f"🔄 Migrating configuration: {input} → {output}")
    
    try:
        # Load v1.1.0 configuration
        with open(input, 'r') as f:
            v1_1_config = yaml.safe_load(f)
        
        # Create backup if requested
        if backup:
            backup_path = f"{input}.v1.1.backup"
            with open(backup_path, 'w') as f:
                yaml.dump(v1_1_config, f, default_flow_style=False, indent=2)
            click.echo(f"📁 Backup created: {backup_path}")
        
        # Migrate configuration
        cdc_manager = CDCConfigurationManager()
        v1_2_config = cdc_manager.migrate_v1_1_to_v1_2_config(v1_1_config)
        
        # Save v1.2.0 configuration
        with open(output, 'w') as f:
            yaml.dump(v1_2_config, f, default_flow_style=False, indent=2)
        
        click.echo(f"✅ Migration completed successfully")
        click.echo(f"💾 v1.2.0 configuration saved: {output}")
        
        # Show migration summary
        tables = v1_2_config.get('tables', {})
        if tables:
            click.echo(f"\n📊 Tables migrated: {len(tables)}")
            for table_name, table_config in tables.items():
                strategy = table_config.get('cdc_strategy', 'timestamp_only')
                click.echo(f"   - {table_name}: {strategy}")
        
    except FileNotFoundError:
        click.echo(f"❌ Input file not found: {input}", err=True)
        raise click.ClickException(f"File not found: {input}")
    except Exception as e:
        click.echo(f"❌ Migration failed: {e}", err=True)
        raise click.ClickException(str(e))


@cdc.command()
@click.option('--output', '-o', help='Output file for examples')
def examples(output: Optional[str]):
    """Generate CDC configuration examples"""
    
    click.echo("📋 Generating CDC configuration examples...")
    
    try:
        cdc_manager = CDCConfigurationManager()
        examples_data = cdc_manager.generate_cdc_config_examples()
        
        if output:
            # Save to file
            with open(output, 'w') as f:
                yaml.dump(examples_data, f, default_flow_style=False, indent=2)
            click.echo(f"💾 Examples saved to: {output}")
        else:
            # Display to console
            for example_name, example_data in examples_data.items():
                click.echo(f"\n🔧 {example_name}:")
                click.echo(f"   Description: {example_data['description']}")
                click.echo(f"   Configuration:")
                config_yaml = yaml.dump(example_data['config'], default_flow_style=False, indent=4)
                for line in config_yaml.splitlines():
                    click.echo(f"     {line}")
        
        click.echo(f"\n✅ Generated {len(examples_data)} CDC configuration examples")
        
    except Exception as e:
        click.echo(f"❌ Failed to generate examples: {e}", err=True)
        raise click.ClickException(str(e))


@cdc.command()
@click.option('--table', '-t', required=True, help='Table name to test')
@click.option('--strategy', '-s', required=True, 
              type=click.Choice(['timestamp_only', 'hybrid', 'id_only', 'full_sync', 'custom_sql']),
              help='CDC strategy to test')
@click.option('--timestamp-column', help='Timestamp column name')
@click.option('--id-column', help='ID column name') 
@click.option('--custom-query', help='Custom SQL query (for custom_sql strategy)')
@click.option('--batch-size', type=int, default=1000, help='Batch size for testing')
@click.option('--dry-run', is_flag=True, help='Only generate query, do not execute')
def test_strategy(table: str, strategy: str, timestamp_column: Optional[str],
                 id_column: Optional[str], custom_query: Optional[str], 
                 batch_size: int, dry_run: bool):
    """Test CDC strategy with sample data"""
    
    click.echo(f"🧪 Testing CDC strategy: {strategy} for table: {table}")
    
    try:
        # Build CDC configuration
        cdc_config = CDCConfig(
            strategy=CDCStrategyType(strategy),
            timestamp_column=timestamp_column,
            id_column=id_column,
            custom_query=custom_query,
            batch_size=batch_size
        )
        
        # Create CDC strategy
        factory = CDCStrategyFactory()
        cdc_strategy = factory.create_strategy(cdc_config)
        
        # Test query building
        test_watermarks = [
            {},  # First run
            {
                'last_mysql_data_timestamp': '2025-01-01 12:00:00',
                'last_processed_id': 5000
            },  # Incremental run
        ]
        
        for i, watermark in enumerate(test_watermarks, 1):
            click.echo(f"\n📋 Test Case {i}: {'First run' if not watermark else 'Incremental run'}")
            
            query = cdc_strategy.build_query(table, watermark, batch_size)
            click.echo(f"Generated Query:")
            click.echo(query)
            
            if not dry_run:
                # TODO: Execute query if connection available
                click.echo("⚠️  Query execution not implemented (use --dry-run for now)")
        
        click.echo(f"\n✅ CDC strategy test completed for {table}")
        
    except Exception as e:
        click.echo(f"❌ CDC strategy test failed: {e}", err=True)
        raise click.ClickException(str(e))


# Add v1.2.0 commands to main CLI
def register_v1_2_commands(cli_group):
    """Register v1.2.0 commands with main CLI"""
    cli_group.add_command(cdc)
    logger.info("Registered v1.2.0 CDC commands with CLI")