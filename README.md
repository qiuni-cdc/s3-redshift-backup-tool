# S3 to Redshift Incremental Backup System

A production-ready Python application for incremental data backup from MySQL to S3 and Redshift, designed to replace Google Colab prototypes with proper architecture, testing, and deployment capabilities.

## Features

- **Multiple Backup Strategies**: Sequential, inter-table parallel, and intra-table parallel processing
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
                    │  - Intra-table  │
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