"""
CLI Integration for v1.1.0 Multi-Schema Support

Integrates multi-schema commands with the existing CLI while maintaining
full backward compatibility with v1.0.0 workflows.
"""

import os
from pathlib import Path
import click
from typing import Optional

from src.utils.logging import get_logger

logger = get_logger(__name__)


def integrate_multi_schema_cli(cli):
    """
    Integrate multi-schema commands with the existing CLI
    
    This function adds v1.1.0 multi-schema capabilities to the existing CLI
    while maintaining 100% backward compatibility with v1.0.0 commands.
    """
    
    # Check if multi-schema configuration exists
    def has_multi_schema_config() -> bool:
        """Check if multi-schema configuration is available"""
        config_path = Path("config/connections.yml")
        return config_path.exists()
    
    def is_multi_schema_enabled() -> bool:
        """Check if multi-schema features should be enabled"""
        # Enable if configuration exists or explicitly requested
        return (
            has_multi_schema_config() or
            os.getenv('ENABLE_MULTI_SCHEMA', '').lower() in ['true', '1', 'yes'] or
            os.getenv('BACKUP_VERSION', '').startswith('1.1')
        )
    
    # Only add multi-schema commands if enabled
    if is_multi_schema_enabled():
        try:
            from src.cli.multi_schema_commands import add_multi_schema_commands
            add_multi_schema_commands(cli)
            logger.info("Multi-schema commands integrated successfully")
        except ImportError as e:
            logger.warning(f"Multi-schema commands not available: {e}")
            # Add a placeholder command to inform users about the feature
            _add_multi_schema_placeholder(cli)
        except Exception as e:
            logger.error(f"Failed to integrate multi-schema commands: {e}")
            _add_multi_schema_placeholder(cli)
    else:
        # Add informational commands about multi-schema features
        _add_multi_schema_info_commands(cli)


def _add_multi_schema_placeholder(cli):
    """Add placeholder commands when multi-schema features are not available"""
    
    @cli.group(name="v1-1", hidden=True)
    def v1_1_placeholder():
        """v1.1.0 Multi-Schema Features (Not Available)"""
        pass
    
    @v1_1_placeholder.command(name="setup")
    def v1_1_setup():
        """Set up v1.1.0 multi-schema configuration"""
        click.echo("🚀 v1.1.0 Multi-Schema Features")
        click.echo("=" * 35)
        click.echo("")
        click.echo("❌ Multi-schema features are not currently available")
        click.echo("")
        click.echo("📋 To enable v1.1.0 multi-schema support:")
        click.echo("  1. Ensure all required dependencies are installed")
        click.echo("  2. Set environment variable: ENABLE_MULTI_SCHEMA=true")
        click.echo("  3. Run: python -m src.cli.main v1-1 setup")
        click.echo("")
        click.echo("💡 Current version supports v1.0.0 functionality only")


def _add_multi_schema_info_commands(cli):
    """Add informational commands about multi-schema features"""
    
    @cli.group(name="multi-schema", hidden=True)
    def multi_schema_info():
        """Multi-Schema Information and Setup"""
        pass
    
    @multi_schema_info.command(name="info")
    def multi_schema_info_cmd():
        """Show information about v1.1.0 multi-schema features"""
        click.echo("🚀 v1.1.0 Multi-Schema Features")
        click.echo("=" * 35)
        click.echo("")
        click.echo("📊 Enhanced Capabilities:")
        click.echo("  • Multiple MySQL source databases")
        click.echo("  • Multiple Redshift target clusters")  
        click.echo("  • Pipeline-based configuration")
        click.echo("  • Advanced connection management")
        click.echo("  • Environment-specific settings")
        click.echo("")
        click.echo("🔧 Current Status:")
        
        config_path = Path("config/connections.yml")
        if config_path.exists():
            click.echo("  ✅ Multi-schema configuration detected")
            click.echo("  💡 Enable with: ENABLE_MULTI_SCHEMA=true")
        else:
            click.echo("  ⚪ Multi-schema configuration not found")
            click.echo("  💡 Run 'python -m src.cli.main multi-schema setup' to configure")
        
        click.echo("")
        click.echo("📚 Learn more: V1_1_MULTI_SCHEMA_DESIGN.md")
    
    @multi_schema_info.command(name="setup")
    def multi_schema_setup():
        """Set up multi-schema configuration structure"""
        click.echo("🔧 Setting up v1.1.0 Multi-Schema Configuration")
        click.echo("")
        
        config_dir = Path("config")
        
        try:
            # Create directory structure
            directories = [
                config_dir,
                config_dir / "pipelines",
                config_dir / "environments", 
                config_dir / "templates",
                config_dir / "tables"
            ]
            
            for directory in directories:
                directory.mkdir(parents=True, exist_ok=True)
            
            # Create connections.yml if it doesn't exist
            connections_file = config_dir / "connections.yml"
            if not connections_file.exists():
                connections_template = """# Multi-Schema Database Connections Configuration
# This file enables v1.1.0 multi-schema features

connections:
  sources:
    # Default connection for v1.0.0 compatibility
    default:
      type: mysql
      host: "${MYSQL_HOST}"
      port: 3306
      database: "${MYSQL_DATABASE}"
      username: "${MYSQL_USERNAME}"
      password: "${MYSQL_PASSWORD}"
      description: "Default v1.0.0 compatibility connection"
    
    # Example: Additional MySQL source
    # sales_mysql:
    #   type: mysql
    #   host: "sales-db.company.com"
    #   port: 3306
    #   database: "sales_production"
    #   username: "${SALES_DB_USER}"
    #   password: "${SALES_DB_PASS}"
    #   description: "Sales department database"

  targets:
    # Default Redshift connection for v1.0.0 compatibility
    default:
      type: redshift
      host: "${REDSHIFT_HOST}"
      port: 5439
      database: "${REDSHIFT_DATABASE}"
      username: "${REDSHIFT_USERNAME}"
      password: "${REDSHIFT_PASSWORD}"
      schema: public
      ssh_tunnel:
        enabled: true
        host: "${SSH_HOST}"
        port: 22
        username: "${SSH_USER}"
        private_key_path: "${SSH_KEY_PATH}"
      description: "Default v1.0.0 compatibility connection"
    
    # Example: Additional Redshift target
    # reporting_redshift:
    #   type: redshift
    #   host: "reporting-cluster.redshift.amazonaws.com"
    #   port: 5439
    #   database: "analytics"
    #   username: "${REPORTING_DB_USER}"
    #   password: "${REPORTING_DB_PASS}"
    #   schema: reporting
    #   description: "Reporting and analytics cluster"

# Global connection settings
connection_settings:
  default_timeout: 1800  # 30 minutes
  connection_retry_delay: 5
  pool_recycle_time: 3600
  health_check_interval: 300
"""
                with open(connections_file, 'w') as f:
                    f.write(connections_template)
                
                click.echo(f"✅ Created: {connections_file}")
            
            # Create default pipeline
            default_pipeline = config_dir / "pipelines" / "default.yml"
            if not default_pipeline.exists():
                pipeline_template = """# Default Pipeline for v1.0.0 Compatibility
pipeline:
  name: "default_legacy"
  version: "1.0.0"
  description: "Default pipeline maintaining v1.0.0 compatibility"
  source: "default"
  target: "default"
  
  processing:
    strategy: "sequential"
    batch_size: 10000
    timeout_minutes: 120
  
  s3:
    isolation_prefix: ""  # No isolation for v1.0.0 compatibility
    partition_strategy: "table"
    compression: "snappy"
  
  default_table_config:
    backup_strategy: "sequential"
    watermark_strategy: "automatic"

# Tables will be registered dynamically for v1.0.0 compatibility
tables: {}
"""
                with open(default_pipeline, 'w') as f:
                    f.write(pipeline_template)
                
                click.echo(f"✅ Created: {default_pipeline}")
            
            # Create environment configurations
            prod_env = config_dir / "environments" / "production.yml"
            if not prod_env.exists():
                prod_template = """# Production Environment Configuration
pipeline:
  processing:
    max_parallel_tables: 5
    timeout_minutes: 240
    
  default_table_config:
    validation:
      enable_data_quality_checks: true
      max_null_percentage: 5.0
"""
                with open(prod_env, 'w') as f:
                    f.write(prod_template)
                
                click.echo(f"✅ Created: {prod_env}")
            
            dev_env = config_dir / "environments" / "development.yml"
            if not dev_env.exists():
                dev_template = """# Development Environment Configuration
pipeline:
  processing:
    max_parallel_tables: 2
    timeout_minutes: 30
    
  default_table_config:
    validation:
      enable_data_quality_checks: false
      max_null_percentage: 20.0
    processing:
      batch_size: 1000  # Smaller batches for development
"""
                with open(dev_env, 'w') as f:
                    f.write(dev_template)
                
                click.echo(f"✅ Created: {dev_env}")
            
            click.echo("")
            click.echo("🎉 Multi-schema configuration setup completed!")
            click.echo("")
            click.echo("📁 Configuration structure created:")
            click.echo("  config/")
            click.echo("    ├── connections.yml       # Database connections")
            click.echo("    ├── pipelines/")
            click.echo("    │   └── default.yml      # Default pipeline")
            click.echo("    ├── environments/")
            click.echo("    │   ├── development.yml   # Dev settings")
            click.echo("    │   └── production.yml    # Prod settings")
            click.echo("    ├── templates/            # Reusable templates")
            click.echo("    └── tables/               # Table-specific configs")
            click.echo("")
            click.echo("🔧 Next steps:")
            click.echo("  1. Update config/connections.yml with your database details")
            click.echo("  2. Set ENABLE_MULTI_SCHEMA=true to enable features")
            click.echo("  3. Test: python -m src.cli.main connections test default")
            click.echo("")
            click.echo("📚 Documentation: V1_1_MULTI_SCHEMA_DESIGN.md")
            
        except Exception as e:
            click.echo(f"❌ Setup failed: {e}")
            return
    
    @multi_schema_info.command(name="migrate")
    def multi_schema_migrate():
        """Show migration guide from v1.0.0 to v1.1.0"""
        click.echo("🔄 Migration Guide: v1.0.0 → v1.1.0")
        click.echo("=" * 40)
        click.echo("")
        click.echo("✅ Good news: v1.1.0 is fully backward compatible!")
        click.echo("")
        click.echo("📋 Current v1.0.0 commands continue to work unchanged:")
        click.echo("  python -m src.cli.main sync -t settlement.table_name")
        click.echo("  python -m src.cli.main watermark get -t settlement.table_name")  
        click.echo("  python -m src.cli.main s3clean list -t settlement.table_name")
        click.echo("")
        click.echo("🚀 New v1.1.0 capabilities (when enabled):")
        click.echo("  # Pipeline-based sync")
        click.echo("  python -m src.cli.main sync pipeline -p sales_pipeline -t customers")
        click.echo("")
        click.echo("  # Connection-based sync")
        click.echo("  python -m src.cli.main sync connections -s mysql_src -r redshift_tgt -t orders")
        click.echo("")
        click.echo("  # Configuration management")
        click.echo("  python -m src.cli.main config list-pipelines")
        click.echo("  python -m src.cli.main connections list")
        click.echo("")
        click.echo("📝 Migration steps (optional):")
        click.echo("  1. Keep using v1.0.0 commands (no changes needed)")
        click.echo("  2. Optionally: Run 'python -m src.cli.main multi-schema setup'")
        click.echo("  3. Optionally: Set ENABLE_MULTI_SCHEMA=true")
        click.echo("  4. Optionally: Create additional pipelines for new projects")
        click.echo("")
        click.echo("⚠️  Zero migration required - v1.0.0 workflows preserved!")


def add_version_detection_commands(cli):
    """Add commands to detect and show version information"""
    
    @cli.command(name="version", hidden=False)
    def version_cmd():
        """Show version information and available features"""
        click.echo("📦 S3-Redshift Backup System")
        click.echo("=" * 30)
        
        # Base version
        click.echo("🏷️  Base Version: v1.0.0 (Production Ready)")
        click.echo("   ✅ MySQL → S3 → Redshift pipeline")
        click.echo("   ✅ Watermark management")
        click.echo("   ✅ S3 storage cleanup")
        click.echo("   ✅ 65M+ row processing capability")
        click.echo("")
        
        # Check v1.1.0 availability
        config_path = Path("config/connections.yml")
        multi_schema_enabled = (
            config_path.exists() or
            os.getenv('ENABLE_MULTI_SCHEMA', '').lower() in ['true', '1', 'yes']
        )
        
        if multi_schema_enabled:
            try:
                from src.cli.multi_schema_commands import add_multi_schema_commands
                click.echo("🚀 Enhanced Version: v1.1.0 (Multi-Schema)")
                click.echo("   ✅ Multiple database connections")
                click.echo("   ✅ Pipeline-based configuration") 
                click.echo("   ✅ Environment-specific settings")
                click.echo("   ✅ Advanced connection management")
                click.echo("")
                click.echo("💡 Multi-schema features are active")
            except ImportError:
                click.echo("⚙️  Enhanced Version: v1.1.0 (Available)")
                click.echo("   ⚪ Multi-schema features available but not loaded")
                click.echo("   💡 Set ENABLE_MULTI_SCHEMA=true to activate")
        else:
            click.echo("⚙️  Enhanced Version: v1.1.0 (Available)")
            click.echo("   ⚪ Multi-schema configuration not detected")
            click.echo("   💡 Run 'python -m src.cli.main multi-schema setup' to configure")
        
        click.echo("")
        
        # Environment info
        env = os.getenv('BACKUP_ENVIRONMENT', 'production')
        click.echo(f"🌍 Environment: {env}")
        
        debug_mode = os.getenv('DEBUG', '0') == '1'
        click.echo(f"🔍 Debug Mode: {'enabled' if debug_mode else 'disabled'}")
        click.echo("")
        
        # Available commands hint
        click.echo("💡 Available commands:")
        click.echo("   python -m src.cli.main --help")
        if multi_schema_enabled:
            click.echo("   python -m src.cli.main config --help")
            click.echo("   python -m src.cli.main connections --help")
    
    @cli.command(name="features", hidden=True)
    def features_cmd():
        """Show detailed feature availability"""
        click.echo("🎛️  Feature Availability Matrix")
        click.echo("=" * 35)
        click.echo("")
        
        features = [
            ("MySQL → S3 → Redshift Pipeline", True, "v1.0.0"),
            ("Watermark Management", True, "v1.0.0"),
            ("S3 Storage Cleanup", True, "v1.0.0"),
            ("SSH Tunnel Support", True, "v1.0.0"),
            ("Multiple Database Connections", _check_multi_schema(), "v1.1.0"),
            ("Pipeline Configuration", _check_multi_schema(), "v1.1.0"),
            ("Environment Management", _check_multi_schema(), "v1.1.0"),
            ("Connection Health Monitoring", _check_multi_schema(), "v1.1.0"),
        ]
        
        for feature_name, available, version in features:
            status = "✅" if available else "⚪"
            click.echo(f"  {status} {feature_name} ({version})")
        
        click.echo("")
        
        if not _check_multi_schema():
            click.echo("💡 Enable v1.1.0 features:")
            click.echo("   python -m src.cli.main multi-schema setup")


def _check_multi_schema() -> bool:
    """Check if multi-schema features are available"""
    try:
        from src.cli.multi_schema_commands import add_multi_schema_commands
        return True
    except ImportError:
        return False


# Export the integration function
__all__ = ['integrate_multi_schema_cli', 'add_version_detection_commands']