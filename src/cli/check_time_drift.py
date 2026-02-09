#!/usr/bin/env python3
import sys
import json
import os
import time
from datetime import datetime

# Ensure src is in path
# Assuming this script is in src/cli/
current_dir = os.path.dirname(os.path.abspath(__file__))
# src/cli/../../ -> project root
project_root = os.path.dirname(os.path.dirname(current_dir))
if project_root not in sys.path:
    sys.path.append(project_root)

from src.config.settings import AppConfig
from src.core.connections import ConnectionManager

def main():
    try:
        # Load config from environment
        config = AppConfig.load()
        cm = ConnectionManager(config)
        
        # Use simple unbuffered cursor
        with cm.database_session() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT UNIX_TIMESTAMP()")
                result = cursor.fetchone()
                if result:
                    mysql_now = int(result[0])
                else:
                    raise Exception("No result from UNIX_TIMESTAMP()")
                
        airflow_now = int(datetime.now().timestamp())
        drift = airflow_now - mysql_now
        
        output = {
            "mysql_now": mysql_now,
            "airflow_now": airflow_now,
            "drift_seconds": drift,
            "status": "success"
        }
        # Print JSON to stdout for Airflow XCom
        print(json.dumps(output))
        
    except Exception as e:
        # Print valid JSON error for upstream handling
        error_output = {
            "status": "error", 
            "error": str(e)
        }
        print(json.dumps(error_output))
        sys.exit(1)
    finally:
        # cleanup
        try:
            if 'cm' in locals():
                cm.close_all_connections()
        except:
            pass

if __name__ == "__main__":
    main()
