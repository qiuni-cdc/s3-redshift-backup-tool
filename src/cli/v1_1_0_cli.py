#!/usr/bin/env python3
"""
V1.1.0 Multi-Schema CLI Entry Point
Lightweight entry point that can load without heavy dependencies
"""

import click
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

@click.group()
@click.pass_context
def cli(ctx):
    """
    S3 to Redshift Backup System - V1.1.0 Multi-Schema Mode
    
    This CLI provides multi-schema database integration capabilities.
    For v1.0.0 compatibility, use: python -m src.cli.main
    """
    ctx.ensure_object(dict)

def setup_v1_1_0_commands():
    """Setup v1.1.0 multi-schema commands"""
    try:
        # Check if v1.1.0 configuration exists
        config_path = Path("config")
        if config_path.exists() and (config_path / "connections.yaml").exists():
            # Import and add v1.1.0 commands
            from src.cli.multi_schema_commands import add_multi_schema_commands
            add_multi_schema_commands(cli)
            return True
    except ImportError as e:
        click.echo(f"❌ V1.1.0 dependencies missing: {e}")
        click.echo("💡 Install dependencies: pip install -r requirements-minimal.txt")
        return False
    except Exception as e:
        click.echo(f"❌ V1.1.0 setup failed: {e}")
        return False
    
    return False

@click.command()
def install_guide():
    """Show installation guide for v1.1.0 dependencies"""
    click.echo("📦 V1.1.0 Installation Guide")
    click.echo("=" * 40)
    click.echo()
    click.echo("🚀 Option 1: Virtual Environment (Recommended)")
    click.echo("python3 -m venv s3_backup_venv")
    click.echo("source s3_backup_venv/bin/activate")
    click.echo("pip install -r requirements-minimal.txt")
    click.echo()
    click.echo("🚀 Option 2: User Installation")
    click.echo("pip3 install --user pydantic PyYAML click mysql-connector-python")
    click.echo()
    click.echo("🚀 Option 3: System Installation")
    click.echo("sudo pip3 install pydantic PyYAML click mysql-connector-python")
    click.echo()
    click.echo("🧪 After installation, test with:")
    click.echo("python -m src.cli.v1_1_0_cli connections list")

# Always add the installation guide
cli.add_command(install_guide)

if __name__ == '__main__':
    # Try to setup v1.1.0 commands
    v1_1_0_enabled = setup_v1_1_0_commands()
    
    if v1_1_0_enabled:
        click.echo("✅ V1.1.0 Multi-Schema Mode Enabled")
    else:
        click.echo("⚠️  V1.1.0 commands not available - use 'install-guide' for help")
    
    cli()