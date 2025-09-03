"""
Redshift Data Verifier for Test Framework

This module provides comprehensive verification of actual Redshift data
to ensure tests validate real data changes, not just log messages.

Key principle: NEVER trust logs alone - always verify actual database state.
"""

from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime
import json
from dataclasses import dataclass

from src.config.settings import AppConfig
from src.core.gemini_redshift_loader import GeminiRedshiftLoader
from src.utils.logging import get_logger

logger = get_logger(__name__)


@dataclass
class TableDataSnapshot:
    """Snapshot of table data for verification"""
    table_name: str
    total_rows: int
    id_range: Tuple[Optional[int], Optional[int]]  # (min_id, max_id)
    sample_data: List[Dict[str, Any]]
    timestamp_range: Tuple[Optional[datetime], Optional[datetime]]  # (earliest, latest)
    unique_timestamps: int
    snapshot_time: datetime
    verification_passed: bool = False
    issues: List[str] = None
    
    def __post_init__(self):
        if self.issues is None:
            self.issues = []


class RedshiftDataVerifier:
    """
    Comprehensive Redshift data verification for test framework.
    
    This class provides methods to:
    1. Take snapshots of table data before/after operations
    2. Verify CDC strategy behavior with actual data
    3. Validate TRUNCATE operations worked correctly
    4. Compare expected vs actual data changes
    """
    
    def __init__(self, config: AppConfig = None):
        self.config = config or AppConfig()
        self.loader = GeminiRedshiftLoader(self.config)
        
    def verify_connection(self) -> bool:
        """Verify we can connect to Redshift"""
        try:
            return self.loader._test_connection()
        except Exception as e:
            logger.error(f"Redshift connection verification failed: {e}")
            return False
    
    def take_table_snapshot(self, table_name: str, schema: str = "public") -> TableDataSnapshot:
        """
        Take a comprehensive snapshot of table data state.
        
        Args:
            table_name: Name of the table (e.g., 'dw_dim_driver')
            schema: Schema name (default: 'public')
            
        Returns:
            TableDataSnapshot with current state
        """
        full_table_name = f"{schema}.{table_name}"
        snapshot = TableDataSnapshot(
            table_name=full_table_name,
            total_rows=0,
            id_range=(None, None),
            sample_data=[],
            timestamp_range=(None, None),
            unique_timestamps=0,
            snapshot_time=datetime.now()
        )
        
        try:
            with self.loader._redshift_connection() as conn:
                with conn.cursor() as cursor:
                    # Check if table exists
                    cursor.execute(f"""
                        SELECT COUNT(*) 
                        FROM information_schema.tables 
                        WHERE table_schema = '{schema}' 
                        AND table_name = '{table_name}'
                    """)
                    table_exists = cursor.fetchone()[0]
                    
                    if not table_exists:
                        snapshot.issues.append(f"Table {full_table_name} does not exist")
                        return snapshot
                    
                    # Get total row count
                    cursor.execute(f"SELECT COUNT(*) FROM {full_table_name}")
                    snapshot.total_rows = cursor.fetchone()[0]
                    
                    if snapshot.total_rows > 0:
                        # Get ID range (if id column exists)
                        try:
                            cursor.execute(f"SELECT MIN(id), MAX(id) FROM {full_table_name}")
                            snapshot.id_range = cursor.fetchone()
                        except Exception:
                            snapshot.id_range = (None, None)  # No ID column or other issue
                        
                        # Get sample data (first and last 3 rows by ID if possible)
                        try:
                            cursor.execute(f"""
                                (SELECT id, updated_at FROM {full_table_name} ORDER BY id ASC LIMIT 3)
                                UNION ALL
                                (SELECT id, updated_at FROM {full_table_name} ORDER BY id DESC LIMIT 3)
                            """)
                            rows = cursor.fetchall()
                            snapshot.sample_data = [{"id": r[0], "updated_at": r[1]} for r in rows]
                        except Exception:
                            # Fallback: get any 5 rows
                            cursor.execute(f"SELECT * FROM {full_table_name} LIMIT 5")
                            columns = [desc[0] for desc in cursor.description]
                            rows = cursor.fetchall()
                            snapshot.sample_data = [dict(zip(columns, row)) for row in rows]
                        
                        # Get timestamp information (if updated_at column exists)
                        try:
                            cursor.execute(f"""
                                SELECT MIN(updated_at), MAX(updated_at), COUNT(DISTINCT updated_at)
                                FROM {full_table_name}
                            """)
                            timestamp_info = cursor.fetchone()
                            snapshot.timestamp_range = (timestamp_info[0], timestamp_info[1])
                            snapshot.unique_timestamps = timestamp_info[2]
                        except Exception:
                            pass  # No updated_at column or other issue
                    
                    snapshot.verification_passed = True
                    logger.info(f"Table snapshot captured: {full_table_name} ({snapshot.total_rows:,} rows)")
                    
        except Exception as e:
            snapshot.issues.append(f"Failed to capture snapshot: {e}")
            logger.error(f"Snapshot failed for {full_table_name}: {e}")
        
        return snapshot
    
    def verify_truncate_operation(self, 
                                  before_snapshot: TableDataSnapshot,
                                  after_snapshot: TableDataSnapshot,
                                  expected_new_rows: int) -> Dict[str, Any]:
        """
        Verify that a TRUNCATE + REPLACE operation worked correctly.
        
        Args:
            before_snapshot: Snapshot before the operation
            after_snapshot: Snapshot after the operation  
            expected_new_rows: Expected number of rows after operation
            
        Returns:
            Verification result dictionary
        """
        result = {
            "truncate_verified": False,
            "replace_verified": False,
            "row_count_correct": False,
            "data_is_fresh": False,
            "issues": [],
            "summary": ""
        }
        
        try:
            # Verify TRUNCATE worked (no accumulation of old data)
            if before_snapshot.total_rows > 0 and after_snapshot.total_rows == expected_new_rows:
                result["truncate_verified"] = True
                result["replace_verified"] = True
            elif after_snapshot.total_rows > expected_new_rows:
                result["issues"].append(f"TRUNCATE failed: found {after_snapshot.total_rows} rows, expected {expected_new_rows}")
            
            # Verify row count is correct
            if after_snapshot.total_rows == expected_new_rows:
                result["row_count_correct"] = True
            else:
                result["issues"].append(f"Row count mismatch: got {after_snapshot.total_rows}, expected {expected_new_rows}")
            
            # Verify data is fresh (ID range starts from 1 if using ID-based data)
            if after_snapshot.id_range[0] == 1 and after_snapshot.id_range[1] == expected_new_rows:
                result["data_is_fresh"] = True
            elif after_snapshot.id_range[0] is not None:
                result["issues"].append(f"Data not fresh: ID range {after_snapshot.id_range}, expected (1, {expected_new_rows})")
            
            # Overall success
            all_checks = [
                result["truncate_verified"],
                result["replace_verified"], 
                result["row_count_correct"]
            ]
            
            if all(all_checks):
                result["summary"] = f"✅ TRUNCATE + REPLACE verified: {expected_new_rows:,} fresh rows loaded"
            else:
                failed_checks = [k for k, v in result.items() if k.endswith("_verified") or k.endswith("_correct") and not v]
                result["summary"] = f"❌ TRUNCATE + REPLACE failed: {', '.join(failed_checks)}"
                
        except Exception as e:
            result["issues"].append(f"Verification error: {e}")
            result["summary"] = f"❌ Verification failed: {e}"
        
        return result
    
    def verify_full_sync_replace_mode(self,
                                      table_name: str,
                                      expected_rows: int,
                                      test_description: str = "") -> Dict[str, Any]:
        """
        Comprehensive verification for full_sync replace mode operation.
        
        This is the main method for validating full_sync CDC strategy.
        
        Args:
            table_name: Table to verify (e.g., 'dw_dim_driver')
            expected_rows: Expected number of rows after sync
            test_description: Description of the test for reporting
            
        Returns:
            Complete verification result
        """
        verification_start = datetime.now()
        
        result = {
            "test_description": test_description,
            "table_name": table_name,
            "expected_rows": expected_rows,
            "verification_time": verification_start,
            "connection_ok": False,
            "table_exists": False,
            "actual_rows": 0,
            "truncate_replace_verified": False,
            "data_consistency_verified": False,
            "overall_success": False,
            "issues": [],
            "detailed_results": {},
            "summary": ""
        }
        
        try:
            # 1. Verify connection
            if not self.verify_connection():
                result["issues"].append("Cannot connect to Redshift")
                result["summary"] = "❌ Connection failed"
                return result
            
            result["connection_ok"] = True
            
            # 2. Take current snapshot
            current_snapshot = self.take_table_snapshot(table_name)
            
            if not current_snapshot.verification_passed:
                result["issues"].extend(current_snapshot.issues)
                if f"does not exist" in str(current_snapshot.issues):
                    result["summary"] = f"❌ Table {table_name} does not exist"
                else:
                    result["summary"] = "❌ Failed to snapshot table"
                return result
            
            result["table_exists"] = True
            result["actual_rows"] = current_snapshot.total_rows
            
            # 3. Verify row count matches expectation
            if current_snapshot.total_rows == expected_rows:
                result["truncate_replace_verified"] = True
            else:
                result["issues"].append(f"Row count mismatch: got {current_snapshot.total_rows:,}, expected {expected_rows:,}")
            
            # 4. Verify data consistency (fresh data load)
            if current_snapshot.id_range[0] == 1 and current_snapshot.id_range[1] == expected_rows:
                result["data_consistency_verified"] = True
            elif current_snapshot.id_range[0] is not None:
                result["issues"].append(f"Data not consistent: ID range {current_snapshot.id_range}, expected (1, {expected_rows})")
            else:
                # No ID column, check timestamp consistency
                if current_snapshot.unique_timestamps == 1:
                    result["data_consistency_verified"] = True
                else:
                    result["issues"].append(f"Multiple timestamps detected: {current_snapshot.unique_timestamps} (expected 1 for fresh sync)")
            
            # 5. Store detailed results
            result["detailed_results"] = {
                "snapshot": {
                    "total_rows": current_snapshot.total_rows,
                    "id_range": current_snapshot.id_range,
                    "timestamp_range": current_snapshot.timestamp_range,
                    "unique_timestamps": current_snapshot.unique_timestamps,
                    "sample_data": current_snapshot.sample_data[:3]  # First 3 rows
                }
            }
            
            # 6. Overall success determination
            result["overall_success"] = (
                result["connection_ok"] and
                result["table_exists"] and
                result["truncate_replace_verified"] and
                result["data_consistency_verified"]
            )
            
            # 7. Generate summary
            if result["overall_success"]:
                result["summary"] = f"✅ Full sync replace mode verified: {expected_rows:,} rows loaded correctly"
            else:
                failed_aspects = []
                if not result["truncate_replace_verified"]:
                    failed_aspects.append("row count")
                if not result["data_consistency_verified"]:
                    failed_aspects.append("data consistency") 
                result["summary"] = f"❌ Full sync verification failed: {', '.join(failed_aspects)}"
            
        except Exception as e:
            result["issues"].append(f"Verification exception: {e}")
            result["summary"] = f"❌ Verification error: {e}"
        
        return result
    
    def print_verification_report(self, result: Dict[str, Any]) -> None:
        """Print a comprehensive verification report"""
        print("\n" + "="*60)
        print(f"🔍 REDSHIFT DATA VERIFICATION REPORT")
        print("="*60)
        
        if result.get("test_description"):
            print(f"📋 Test: {result['test_description']}")
        
        print(f"📊 Table: {result['table_name']}")
        print(f"⏰ Time: {result['verification_time']}")
        print(f"🎯 Expected Rows: {result['expected_rows']:,}")
        print(f"📈 Actual Rows: {result['actual_rows']:,}")
        
        print("\n📋 Verification Checks:")
        print(f"  🔗 Connection: {'✅' if result['connection_ok'] else '❌'}")
        print(f"  📁 Table Exists: {'✅' if result['table_exists'] else '❌'}")  
        print(f"  🔢 Row Count: {'✅' if result['truncate_replace_verified'] else '❌'}")
        print(f"  🔄 Data Consistency: {'✅' if result['data_consistency_verified'] else '❌'}")
        
        if result.get("detailed_results", {}).get("snapshot"):
            snapshot = result["detailed_results"]["snapshot"]
            print(f"\n📊 Data Details:")
            print(f"  ID Range: {snapshot['id_range']}")
            print(f"  Timestamp Range: {snapshot['timestamp_range']}")
            print(f"  Unique Timestamps: {snapshot['unique_timestamps']}")
        
        if result.get("issues"):
            print(f"\n❌ Issues Found:")
            for issue in result["issues"]:
                print(f"  • {issue}")
        
        print(f"\n{result['summary']}")
        
        if result["overall_success"]:
            print("🎉 DATA VERIFICATION PASSED!")
        else:
            print("💥 DATA VERIFICATION FAILED!")
        
        print("="*60 + "\n")


# Convenience function for quick verification
def verify_full_sync_test(table_name: str, expected_rows: int, test_description: str = "") -> bool:
    """
    Quick verification function for full_sync tests.
    
    Returns True if verification passes, False otherwise.
    """
    verifier = RedshiftDataVerifier()
    result = verifier.verify_full_sync_replace_mode(table_name, expected_rows, test_description)
    verifier.print_verification_report(result)
    return result["overall_success"]


if __name__ == "__main__":
    # Example usage
    success = verify_full_sync_test(
        table_name="dw_dim_driver",
        expected_rows=2000,
        test_description="Full Sync Replace Mode Test"
    )
    
    if success:
        print("✅ Test passed with actual data verification!")
    else:
        print("❌ Test failed - check Redshift data!")