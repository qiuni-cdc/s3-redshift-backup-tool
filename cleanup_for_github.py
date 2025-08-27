#!/usr/bin/env python3
"""
Clean up project files for GitHub submission
Remove outdated/testing files and organize production-ready code
"""

import os
import shutil
from pathlib import Path

def cleanup_for_github():
    """Clean up project files for GitHub submission"""
    
    project_root = Path("/home/qi_chen/s3-redshift-backup")
    
    print("🧹 Cleaning up S3-Redshift-Backup project for GitHub")
    print("=" * 60)
    
    # Files to keep (production-ready)
    keep_files = {
        # Core source code
        "src/",
        "tests/",
        
        # Main documentation
        "README.md",
        "USER_MANUAL.md", 
        "CLAUDE.md",
        "PRODUCTION_SUMMARY.md",
        "LATEST_STATUS_USAGE.sql",
        
        # Configuration files
        "requirements.txt",
        "setup.py",
        ".env.template",
        
        # Production scripts (final versions)
        "create_latest_status_view.py",
        "create_production_table.py", 
        "complete_full_loading.py",
        "quick_status_check.py",
        
        # Cleanup script itself
        "cleanup_for_github.py"
    }
    
    # Directories to completely remove
    remove_dirs = {
        "test_env",           # Virtual environment
        "s3-redshift-backup", # Duplicate directory
        "__pycache__",        # Python cache
        ".pytest_cache",     # Pytest cache
        "docs"               # Old docs
    }
    
    # Files to remove (testing/debugging/outdated)
    remove_files = {
        # Testing and debugging files
        "test_*.py",
        "debug_*.py",
        "check_*.py",
        "verify_*.py",
        "inspect_*.py",
        "analyze_*.py",
        "diagnose_*.py",
        "fix_*.py",
        "execute_*.py",
        "manual_*.py",
        "simple_*.py",
        
        # Temporary/experimental files
        "backup_dashboard.py",
        "clean_and_restart_backup.py",
        "cleanup_s3.py",
        "complete_data_loading.py",
        "configure.py",
        "csv_conversion_test.py",
        "display_*.py",
        "environment_summary.py",
        "final_*.py",
        "force_backup.py",
        "generate_*.py",
        "list_schemas.py",
        "migrate_to_final_table.py",
        "production_s3_to_redshift.py",
        "redshift_*.py",
        "run_*.py",
        "setup_config.sh",
        "show_info.py",
        "successful_redshift_copy.py",
        
        # Old documentation files
        "BACKUP_SUMMARY.md",
        "CLAIM_DATA_REPORT.md", 
        "REDSHIFT_SCHEMA_SOLUTION.md",
        
        # SQL test files
        "*.sql"
    }
    
    print("🗂️  Keeping production files:")
    for item in sorted(keep_files):
        if (project_root / item).exists():
            print(f"   ✅ {item}")
    
    print(f"\n🗑️  Removing outdated directories:")
    removed_dirs = 0
    for dir_name in remove_dirs:
        dir_path = project_root / dir_name
        if dir_path.exists() and dir_path.is_dir():
            print(f"   🗂️  Removing: {dir_name}")
            try:
                shutil.rmtree(dir_path)
                removed_dirs += 1
            except Exception as e:
                print(f"   ❌ Failed to remove {dir_name}: {e}")
    
    print(f"\n🗑️  Removing outdated files:")
    removed_files = 0
    
    # Get all files in project root
    all_files = [f for f in project_root.iterdir() if f.is_file()]
    
    for file_path in all_files:
        file_name = file_path.name
        
        # Skip files we want to keep
        if file_name in [f for f in keep_files if "/" not in f]:
            continue
            
        # Check if file matches removal patterns
        should_remove = False
        for pattern in remove_files:
            if "*" in pattern:
                import fnmatch
                if fnmatch.fnmatch(file_name, pattern):
                    should_remove = True
                    break
            elif file_name == pattern:
                should_remove = True
                break
        
        if should_remove:
            print(f"   📄 Removing: {file_name}")
            try:
                file_path.unlink()
                removed_files += 1
            except Exception as e:
                print(f"   ❌ Failed to remove {file_name}: {e}")
    
    print(f"\n📊 Cleanup Summary:")
    print(f"   🗂️  Directories removed: {removed_dirs}")
    print(f"   📄 Files removed: {removed_files}")
    
    print(f"\n📋 Final project structure:")
    
    def show_tree(path, prefix="", max_depth=3, current_depth=0):
        if current_depth >= max_depth:
            return
            
        items = []
        try:
            items = sorted([p for p in path.iterdir() if not p.name.startswith('.')])
        except PermissionError:
            return
            
        for i, item in enumerate(items):
            is_last = i == len(items) - 1
            current_prefix = "└── " if is_last else "├── "
            print(f"{prefix}{current_prefix}{item.name}")
            
            if item.is_dir() and current_depth < max_depth - 1:
                next_prefix = prefix + ("    " if is_last else "│   ")
                show_tree(item, next_prefix, max_depth, current_depth + 1)
    
    print(f"\n📁 {project_root.name}/")
    show_tree(project_root)
    
    return removed_dirs + removed_files

def create_env_template():
    """Create .env.template file for GitHub"""
    
    template_content = """# S3 to Redshift Backup System Configuration
# Copy this file to .env and fill in your actual values

# Database Configuration
DB_HOST=your-mysql-host
DB_PORT=3306
DB_USER=your-username
DB_PASSWORD=your-password
DB_DATABASE=settlement

# SSH Configuration (for bastion host access)
SSH_BASTION_HOST=your-bastion-host
SSH_BASTION_USER=your-ssh-username
SSH_BASTION_KEY_PATH=/path/to/your/ssh/key.pem

# S3 Configuration  
AWS_ACCESS_KEY_ID=your-aws-access-key
AWS_SECRET_ACCESS_KEY=your-aws-secret-key
S3_BUCKET_NAME=your-s3-bucket
S3_REGION=us-east-1
S3_INCREMENTAL_PATH=incremental/
S3_HIGH_WATERMARK_KEY=watermark/last_run_timestamp.txt

# Redshift Configuration (optional)
REDSHIFT_HOST=your-redshift-cluster
REDSHIFT_PORT=5439
REDSHIFT_DATABASE=dw
REDSHIFT_USER=your-redshift-user
REDSHIFT_PASSWORD=your-redshift-password

# Backup Performance Settings
BACKUP_BATCH_SIZE=10000
BACKUP_MAX_WORKERS=4
BACKUP_NUM_CHUNKS=4
BACKUP_RETRY_ATTEMPTS=3
BACKUP_TIMEOUT_SECONDS=300
"""
    
    template_path = Path("/home/qi_chen/s3-redshift-backup/.env.template")
    with open(template_path, 'w') as f:
        f.write(template_content)
    
    print(f"✅ Created .env.template file")

def create_gitignore():
    """Create .gitignore file"""
    
    gitignore_content = """# Environment and credentials
.env
*.pem
*.key

# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
build/
develop-eggs/
dist/
downloads/
eggs/
.eggs/
lib/
lib64/
parts/
sdist/
var/
wheels/
*.egg-info/
.installed.cfg
*.egg

# Virtual environments
venv/
env/
ENV/
test_env/

# IDE
.vscode/
.idea/
*.swp
*.swo
*~

# Logs
logs/
*.log

# Testing
.pytest_cache/
.coverage
htmlcov/

# MacOS
.DS_Store

# Windows
Thumbs.db
ehthumbs.db
Desktop.ini
"""
    
    gitignore_path = Path("/home/qi_chen/s3-redshift-backup/.gitignore")
    with open(gitignore_path, 'w') as f:
        f.write(gitignore_content)
    
    print(f"✅ Created .gitignore file")

if __name__ == "__main__":
    removed_count = cleanup_for_github()
    create_env_template()
    create_gitignore()
    
    print(f"\n🎉 Project cleanup complete!")
    print(f"📊 Removed {removed_count} outdated items")
    print(f"✅ Project is now ready for GitHub submission")
    print(f"\n📝 Next steps:")
    print(f"   1. Review the cleaned project structure")
    print(f"   2. Initialize git repository: git init")
    print(f"   3. Add files: git add .")
    print(f"   4. Commit: git commit -m 'Initial commit - S3 to Redshift Backup System'")
    print(f"   5. Push to GitHub")