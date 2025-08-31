"""
Column Mapping CLI Commands

Commands for viewing and managing column name mappings
between MySQL source tables and Redshift target tables.
"""

import click
from pathlib import Path

from src.core.column_mapper import ColumnMapper
from src.utils.logging import get_logger

logger = get_logger(__name__)


@click.group(name='column-mappings')
def column_mappings_group():
    """Manage column name mappings for Redshift compatibility"""
    pass


@column_mappings_group.command('list')
def list_mappings():
    """List all column mappings"""
    try:
        mapper = ColumnMapper()
        mappings = mapper.list_all_mappings()
        
        if not mappings:
            click.echo("📝 No column mappings found")
            return
        
        click.echo("📊 Column Mappings:")
        click.echo(f"   Total tables with mappings: {len(mappings)}")
        click.echo()
        
        for mapping in mappings:
            click.echo(f"   📋 {mapping['table_name']}")
            click.echo(f"      Mapped columns: {mapping['mapped_columns']}/{mapping['total_columns']}")
            click.echo(f"      Created: {mapping['created_at']}")
            click.echo()
    
    except Exception as e:
        logger.error(f"Failed to list column mappings: {e}")
        click.echo(f"❌ Error: {e}", err=True)


@column_mappings_group.command('show')
@click.option('-t', '--table', required=True, help='Table name')
def show_mapping(table):
    """Show column mapping for a specific table"""
    try:
        mapper = ColumnMapper()
        
        # Load mapping file
        safe_filename = table.replace(':', '_').replace('.', '_') + '.json'
        mapping_file = mapper.mappings_dir / safe_filename
        
        if not mapping_file.exists():
            click.echo(f"📝 No column mapping found for table: {table}")
            return
        
        # Read and display mapping
        import json
        with open(mapping_file, 'r') as f:
            data = json.load(f)
        
        click.echo(f"📊 Column Mapping for {data['table_name']}:")
        click.echo(f"   Created: {data['created_at']}")
        click.echo(f"   Total columns: {data['total_columns']}")
        click.echo(f"   Mapped columns: {data['mapped_columns']}")
        click.echo()
        click.echo("   Mappings:")
        
        for orig, mapped in data['mappings'].items():
            click.echo(f"      {orig} → {mapped}")
    
    except Exception as e:
        logger.error(f"Failed to show column mapping: {e}")
        click.echo(f"❌ Error: {e}", err=True)


@column_mappings_group.command('clear')
@click.option('-t', '--table', required=True, help='Table name')
@click.option('--force', is_flag=True, help='Skip confirmation')
def clear_mapping(table, force):
    """Clear column mapping for a table"""
    try:
        mapper = ColumnMapper()
        
        if not force:
            click.confirm(f"⚠️  Clear column mapping for {table}?", abort=True)
        
        if mapper.clear_mapping(table):
            click.echo(f"✅ Cleared column mapping for {table}")
        else:
            click.echo(f"❌ No mapping found for {table}")
    
    except Exception as e:
        logger.error(f"Failed to clear column mapping: {e}")
        click.echo(f"❌ Error: {e}", err=True)


@column_mappings_group.command('clear-all')
@click.option('--force', is_flag=True, help='Skip confirmation')
def clear_all_mappings(force):
    """Clear all column mappings"""
    try:
        mapper = ColumnMapper()
        mappings = mapper.list_all_mappings()
        
        if not mappings:
            click.echo("📝 No column mappings to clear")
            return
        
        if not force:
            click.confirm(f"⚠️  Clear ALL {len(mappings)} column mappings?", abort=True)
        
        cleared = 0
        for mapping in mappings:
            if mapper.clear_mapping(mapping['table_name']):
                cleared += 1
        
        click.echo(f"✅ Cleared {cleared} column mappings")
    
    except Exception as e:
        logger.error(f"Failed to clear all column mappings: {e}")
        click.echo(f"❌ Error: {e}", err=True)