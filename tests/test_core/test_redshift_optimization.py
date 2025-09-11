#!/usr/bin/env python3
"""
Test Redshift DISTKEY/SORTKEY optimization with real FlexibleSchemaManager.
Tests: unidw.dw_parcel_detail_tool, settlement.settlement_normal_delivery_detail, settlement.settle_orders
"""

import sys
import os
# Add project root to Python path (go up 3 levels from tests/test_core/)
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from src.core.flexible_schema_manager import FlexibleSchemaManager
from src.core.connections import ConnectionManager  
from src.config.settings import AppConfig

def test_redshift_optimization():
    """Test DDL generation for real tables using FlexibleSchemaManager"""
    
    test_tables = [
        {
            "name": "unidw.dw_parcel_detail_tool (no config - should be AUTO)",
            "table": "unidw.dw_parcel_detail_tool",
            "expected_dist": "DISTSTYLE AUTO",
            "expected_sort": "SORTKEY AUTO"
        },
        {
            "name": "settlement.settlement_normal_delivery_detail (compound)",
            "table": "settlement.settlement_normal_delivery_detail", 
            "expected_dist": "DISTKEY(ant_parcel_no)",
            "expected_sort": "COMPOUND SORTKEY(billing_num, create_at)"
        },
        {
            "name": "settlement.settle_orders (interleaved)",
            "table": "settlement.settle_orders",
            "expected_dist": "DISTKEY(tracking_number)", 
            "expected_sort": "INTERLEAVED SORTKEY(tracking_number, created_at)"
        }
    ]
    
    print("=" * 80)
    print("Testing Redshift Optimization with Real FlexibleSchemaManager")
    print("=" * 80)
    
    try:
        # Initialize real components
        config = AppConfig()
        connection_manager = ConnectionManager(config)
        schema_manager = FlexibleSchemaManager(connection_manager)
        
        # Test each table
        for i, test_case in enumerate(test_tables, 1):
            print(f"\n{i}. {test_case['name']}")
            print("-" * 60)
            
            table_name = test_case['table']
            
            try:
                # Generate actual DDL
                print(f"Generating DDL for: {table_name}")
                pyarrow_schema, ddl = schema_manager.get_table_schema(table_name)
                
                print("\n📄 Generated DDL:")
                print("-" * 40)
                print(ddl)
                print("-" * 40)
                
                # Verify expectations
                print(f"\n🔍 Verification:")
                
                dist_found = test_case['expected_dist'] in ddl
                sort_found = test_case['expected_sort'] in ddl
                
                if dist_found:
                    print(f"✅ Distribution: {test_case['expected_dist']}")
                else:
                    print(f"❌ Distribution: Expected '{test_case['expected_dist']}' not found")
                    # Show what was actually found
                    if 'DISTSTYLE' in ddl or 'DISTKEY' in ddl:
                        for line in ddl.split('\n'):
                            if 'DISTSTYLE' in line or 'DISTKEY' in line:
                                print(f"   Found instead: {line.strip()}")
                
                if sort_found:
                    print(f"✅ Sort: {test_case['expected_sort']}")
                else:
                    print(f"❌ Sort: Expected '{test_case['expected_sort']}' not found")
                    # Show what was actually found
                    if 'SORTKEY' in ddl:
                        for line in ddl.split('\n'):
                            if 'SORTKEY' in line:
                                print(f"   Found instead: {line.strip()}")
                
                if dist_found and sort_found:
                    print("✅ PASS - All expectations met")
                else:
                    print("❌ FAIL - Some expectations not met")
                    
            except Exception as e:
                print(f"❌ Error generating DDL for {table_name}: {e}")
                
    except Exception as e:
        print(f"❌ Failed to initialize components: {e}")
        import traceback
        traceback.print_exc()
    
    print(f"\n{'='*80}")
    print("Test Complete")
    print(f"{'='*80}")


if __name__ == "__main__":
    test_redshift_optimization()