# 🚀 S3 to Redshift Incremental Backup System

[![Security Scan](https://github.com/qiuni-cdc/s3-redshift-backup-tool/actions/workflows/security-scan.yml/badge.svg)](https://github.com/qiuni-cdc/s3-redshift-backup-tool/actions/workflows/security-scan.yml)
[![Python 3.12+](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

🎉 **PRODUCTION READY** - A fully operational Python application for incremental data backup from MySQL to S3 and Redshift with enterprise architecture, comprehensive testing, and verified deployment capabilities.

## 🎯 **Quick Start**

### **1. One-Time Setup**
```bash
# Clone repository
git clone https://github.com/qiuni-cdc/s3-redshift-backup-tool.git
cd s3-redshift-backup-tool

# Setup virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Setup security protection (REQUIRED)
./scripts/setup-git-hooks.sh
```

### **2. Configure Credentials**
```bash
# Copy configuration template
cp .env .env.local

# Edit .env.local with your actual credentials
# (All files starting with .env are gitignored for security)
```

### **3. Run Your First Sync**
```bash
# Check system status
python -m src.cli.main status

# Sync with pipeline (v1.1.0+ recommended)
python -m src.cli.main sync pipeline -p default -t schema.table

# Or use direct sync (v1.0.0 compatibility)
python -m src.cli.main sync -t schema.table

# See CLI_QUICK_REFERENCE.md for complete command syntax
```

## 🛡️ **Security Features**

This repository implements **comprehensive credential protection**:

### **🔒 6-Layer Protection System**
1. **GitIgnore**: Prevents credential files from being committed
2. **Pre-commit Hooks**: Scans every commit for secrets
3. **Pre-push Hooks**: Final validation before GitHub push
4. **GitLeaks Integration**: Industry-standard secret detection
5. **Custom Scanner**: Domain-specific credential patterns
6. **GitHub Actions**: Automated security scanning on every push

### **🚨 Zero Credential Exposure**
- ✅ **All documentation uses placeholder credentials**
- ✅ **Real credentials never committed to repository**
- ✅ **Automatic scanning prevents accidental exposure**
- ✅ **Safe for public repositories and collaboration**

**Setup Security**: Run `./scripts/setup-git-hooks.sh` once after cloning.

## ⚡ Features

- **Multiple Backup Strategies**: Sequential and inter-table parallel processing
- **Incremental Processing**: High-watermark based incremental data processing  
- **Production Ready**: Proper error handling, logging, monitoring, and testing
- **Configurable**: Environment-based configuration with validation
- **CLI Interface**: Command-line tool for easy operation and automation

## Quick Start

1. **Clone and Setup**
```bash
git clone <repository-url>
cd s3-redshift-backup
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -e .
```

2. **Configure Environment**
```bash
cp .env.template .env
# Edit .env with your actual configuration values
```

3. **Run Backup**
```bash
s3-backup backup -t your_table_name -s sequential
```

## Configuration

Copy `.env.template` to `.env` and configure:

- **Database**: MySQL connection details
- **SSH**: Bastion host configuration for secure tunneling
- **S3**: AWS credentials and bucket information  
- **Backup**: Batch sizes, parallelism, and retry settings

## Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   MySQL DB      │    │   S3 Storage    │    │   Redshift DW   │
│   (Source)      │────│   (Staging)     │────│   (Target)      │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 │
                    ┌─────────────────┐
                    │  Backup System  │
                    │  - Sequential   │
                    │  - Inter-table  │
                    └─────────────────┘
```

## Development Status

🚧 **Currently in development** - See `CLAUDE.md` for detailed development tasks and progress.

### Completed
- [x] Project structure and setup files
- [x] Development plan and task breakdown
- [x] Package structure with proper imports
- [x] Requirements and setup configuration
- [x] Environment template and Git ignore rules

### In Progress
- [ ] Core infrastructure (configuration, logging, exceptions)
- [ ] Connection management (SSH, DB, S3)
- [ ] Data management (S3 manager, watermark system)
- [ ] Backup strategies implementation

## Contributing

1. Check `CLAUDE.md` for current development tasks
2. Follow the development phases in priority order  
3. Ensure all code includes proper tests and documentation
4. Run linting and type checking before commits

## License

MIT License - see LICENSE file for details