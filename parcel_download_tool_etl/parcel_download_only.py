#!/usr/bin/env python3
"""
Download-Only Automation Script

This script executes ONLY the download phase from the parcel download tool.
No syncing, no cleanup, just downloads data from the source.

DOWNLOAD-ONLY MODE:
1. Executes download.sh from parcel_download_tool_etl directory with specified parameters
2. Reports success/failure

Usage:
  python parcel_download_only.py                                    # Download with default: -1 hour
  python parcel_download_only.py -d "2024-08-14 10:00:00" --hours "-2"  # Download with specific datetime
  python parcel_download_only.py --date "" --hours "-1"             # Use current time, 1 hour ago
  python parcel_download_only.py --help                             # Show help
"""

import subprocess
import sys
import argparse
from pathlib import Path
import logging

# Project root directory (parent of parcel_download_tool_etl)
SCRIPT_PATH = Path(__file__).resolve()
SCRIPT_DIR = SCRIPT_PATH.parent
PROJECT_ROOT = SCRIPT_DIR.parent

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(PROJECT_ROOT / 'parcel_download_tool_etl' / 'parcel_download_only.log')
    ]
)
logger = logging.getLogger(__name__)


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description="Download-Only Automation Script",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python parcel_download_only.py                                    # Download with default: -1 hour
  python parcel_download_only.py -d "2024-08-14 10:00:00" --hours "-2"  # Download with specific datetime
  python parcel_download_only.py --date "" --hours "-1"             # Use current time, 1 hour ago
  python parcel_download_only.py --help                             # Show this help message
        """
    )

    parser.add_argument(
        '-d', '--date',
        type=str,
        default='',
        help='Start datetime string for download script (e.g., "2024-08-14 10:00:00"). Empty string means use current time.'
    )

    parser.add_argument(
        '--hours',
        type=str,
        default='-1',
        help='Hours offset string for download script (e.g., "-2" for 2 hours ago, "-1" for 1 hour ago). Default: -1'
    )

    parser.add_argument(
        '-p', '--pipeline',
        type=str,
        default='us_dw_unidw_2_public_pipeline',
        help='Pipeline name (used for logging only, not functionally used in download-only mode). Default: us_dw_unidw_2_public_pipeline'
    )

    return parser.parse_args()


def execute_download_script(download_dir="parcel_download_tool_etl", start_datetime=None, hours_offset=None):
    """
    Execute download_tool.sh script from the specified directory with parameters

    Args:
        download_dir: Directory containing download_tool.sh script
        start_datetime: Start datetime string (e.g., "2024-08-14 10:00:00")
        hours_offset: Hours offset string (e.g., "-2")

    Returns:
        tuple: (success: bool, stdout: str, stderr: str)
    """

    logger.info(f"🔄 Starting download script execution from {download_dir}")
    logger.info(f"   Parameters: start_datetime='{start_datetime}', hours_offset='{hours_offset}'")

    # Check if directory exists
    if not Path(download_dir).exists():
        logger.error(f"❌ Directory {download_dir} not found")
        return False, "", f"Directory {download_dir} not found"
    # Check if download_tool.sh exists
    download_script = Path(download_dir) / "download_tool.sh"
    if not download_script.exists():
        logger.error(f"❌ download_tool.sh not found in {download_dir}")
        return False, "", f"download_tool.sh not found in {download_dir}"

    try:
        # Execute download script with parameters
        result = subprocess.run(
            ["./download_tool.sh", start_datetime, hours_offset],
            cwd=download_dir,
            capture_output=True,
            text=True,
            timeout=2400  # 40 minute timeout
        )

        if result.returncode == 0:
            logger.info("✅ Download script completed successfully")
            return True, result.stdout, result.stderr
        else:
            logger.error(f"❌ Download script failed with exit code {result.returncode}")
            logger.error(f"Error output: {result.stderr}")
            return False, result.stdout, result.stderr

    except subprocess.TimeoutExpired:
        logger.error("❌ Download script timed out after 40 minutes")
        return False, "", "Script execution timed out"
    except Exception as e:
        logger.error(f"❌ Error executing download script: {e}")
        return False, "", str(e)


def main():
    """Main execution function"""
    # Parse command line arguments
    args = parse_arguments()

    # Configuration
    DOWNLOAD_DIR = str(PROJECT_ROOT / "parcel_download_tool_etl")
    PIPELINE = args.pipeline

    logger.info("🚀 Starting Download-Only Pipeline")
    logger.info("=" * 50)
    logger.info(f"Configuration:")
    logger.info(f"  Download Dir: {DOWNLOAD_DIR}")
    logger.info(f"  Pipeline: {PIPELINE} (for logging only)")
    logger.info(f"  Start DateTime: {args.date if args.date else 'current time'}")
    logger.info(f"  Hours Offset: {args.hours}")
    logger.info("")

    # Execute download script
    logger.info("📥 Phase 1: Executing download script")
    download_success, download_stdout, download_stderr = execute_download_script(
        DOWNLOAD_DIR, args.date, args.hours
    )

    # Final status
    logger.info("=" * 50)
    if download_success:
        logger.info("🎉 Download phase completed successfully!")
        sys.exit(0)
    else:
        logger.error("❌ Download phase failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
